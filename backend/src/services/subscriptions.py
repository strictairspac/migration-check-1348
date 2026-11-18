from datetime import datetime,timezone
from fastapi import HTTPException
from sqlalchemy import func
from src.config.database import Session
from src.models.downstream import (ChangeEvent,Subscription,DeliveryBatch,DownstreamRow,AppliedEvent)
from src.models.run import TargetRow
from src.services.migration import digest
from src.services import changes
from src.constants.enums import SUB_STATUSES,REBUILD_REASONS
SNAPSHOT_PAGE=100
def _get(session,sub_id,lock=False):
 q=session.query(Subscription).filter_by(id=sub_id)
 row=q.with_for_update().first() if lock else q.first()
 if not row:raise HTTPException(404,'未找到接库单')
 return row
def _open_batch(session,sub,lock=False):
 q=session.query(DeliveryBatch).filter_by(subscription_id=sub.id,status='open')
 return q.with_for_update().first() if lock else q.first()
def list_items():
 with Session() as s:
  rows=s.query(Subscription).order_by(Subscription.id.desc()).all()
  return [_view(s,r) for r in rows]
def detail(sub_id):
 with Session() as s:return _view(s,_get(s,sub_id))
def _view(session,sub):
 data={c.name:getattr(sub,c.name) for c in sub.__table__.columns}
 pending=_open_batch(session,sub)
 data['open_batch']={'batch_no':pending.batch_no,'phase':pending.phase,'attempts':pending.attempts,
  'checksum':pending.checksum,'items':len(pending.payload)} if pending else None
 data['downstream_count']=session.query(DownstreamRow).filter_by(subscription_id=sub.id).count()
 data['applied_count']=session.query(AppliedEvent).filter_by(subscription_id=sub.id).count()
 data['latest_seq']=changes.latest_seq(session)
 return data
def create(data):
 with Session() as s:
  watermark=changes.latest_seq(s)
  total=s.query(TargetRow).filter(TargetRow.born_seq<=watermark).count()
  sub=Subscription(name=data.name,downstream=data.downstream,status='snapshotting',
   snapshot_seq=watermark,snapshot_after_id=0,snapshot_total=total,snapshot_delivered=0,
   stream_acked_seq=watermark,retained_from=1,retention_limit=data.retention_limit,note=data.note or '')
  s.add(sub);s.commit();s.refresh(sub);return _view(s,sub)
def _new_batch_no(session,sub):
 last=session.query(func.max(DeliveryBatch.batch_no)).filter_by(subscription_id=sub.id).scalar()
 return (last or 0)+1
def pull_snapshot(sub_id):
 with Session() as s:
  sub=_get(s,sub_id,lock=True)
  if sub.status in ('streaming',):raise HTTPException(409,'全量已交接完成，当前为增量阶段')
  if sub.status in ('rebuild_required','closed'):raise HTTPException(409,'接库单不可用：'+(sub.rebuild_reason or '已关闭')+'，请重建')
  pending=_open_batch(s,sub,lock=True)
  if pending:
   pending.attempts+=1;s.commit();return _batch_payload(pending,sub,redeliver=True)
  rows=(s.query(TargetRow).filter(TargetRow.id>sub.snapshot_after_id,TargetRow.born_seq<=sub.snapshot_seq)
       .order_by(TargetRow.id.asc()).limit(SNAPSHOT_PAGE).all())
  if not rows:
   sub.status='streaming';s.commit()
   return {'phase':'snapshot','finished':True,'message':'全量交接完成，切换到增量拉取','subscription':_view(s,sub)}
  payload=[{'target_row_id':r.id,'run_id':r.run_id,'key_digest':r.key_digest,
   'content_hash':digest(r.payload),'payload':r.payload,'born_seq':r.born_seq} for r in rows]
  b=DeliveryBatch(subscription_id=sub.id,batch_no=_new_batch_no(s,sub),phase='snapshot',
   payload=payload,checksum=digest(payload),status='open')
  sub.snapshot_after_id=rows[-1].id;sub.snapshot_delivered+=len(rows)
  s.add(b);s.commit();return _batch_payload(b,sub)
def pull_stream(sub_id,limit=100):
 if not 1<=limit<=500:raise HTTPException(422,'单批增量上限为1-500')
 with Session() as s:
  sub=_get(s,sub_id,lock=True)
  if sub.status=='snapshotting' or sub.status=='rebuilding':raise HTTPException(409,'全量尚未交接完成，请继续拉取全量批次')
  if sub.status in ('rebuild_required','closed'):raise HTTPException(409,'接库单不可用：'+(sub.rebuild_reason or '已关闭')+'，请重建')
  pending=_open_batch(s,sub,lock=True)
  if pending:
   pending.attempts+=1;s.commit();return _batch_payload(pending,sub,redeliver=True)
  want=sub.stream_acked_seq+1
  # 缺口检测：需要的首条已被回收，禁止跳过，只能重建
  if want<sub.retained_from:
   sub.status='rebuild_required';sub.rebuild_reason='gap_detected'
   sub.rebuild_history=(sub.rebuild_history or [])+[{'at':datetime.now(timezone.utc).isoformat(),
    'reason':'gap_detected','wanted_from':want,'retained_from':sub.retained_from}]
   s.commit()
   raise HTTPException(409,f'增量缺口：需要seq={want}，最早保留seq={sub.retained_from}，不能跳过，请重建接库单')
  evs=(s.query(ChangeEvent).filter(ChangeEvent.seq>=want).order_by(ChangeEvent.seq.asc()).limit(limit).all())
  if not evs:return {'phase':'stream','empty':True,'next_seq':want,'subscription':_view(s,sub)}
  if evs[0].seq!=want:
   sub.status='rebuild_required';sub.rebuild_reason='gap_detected'
   sub.rebuild_history=(sub.rebuild_history or [])+[{'at':datetime.now(timezone.utc).isoformat(),
    'reason':'gap_detected','wanted_from':want,'found_from':evs[0].seq}]
   s.commit()
   raise HTTPException(409,f'增量缺口：期望seq={want}，实际下一条seq={evs[0].seq}，不能跳过，请重建')
  payload=[{'seq':e.seq,'kind':e.kind,'run_id':e.run_id,'target_row_id':e.target_row_id,
   'key_digest':e.key_digest,'content_hash':e.content_hash,'payload':e.payload} for e in evs]
  b=DeliveryBatch(subscription_id=sub.id,batch_no=_new_batch_no(s,sub),phase='stream',
   from_seq=evs[0].seq,to_seq=evs[-1].seq,payload=payload,checksum=digest(payload),status='open')
  s.add(b);s.commit();return _batch_payload(b,sub)
def _batch_payload(b,sub,redeliver=False):
 return {'batch_id':b.id,'batch_no':b.batch_no,'phase':b.phase,'from_seq':b.from_seq,'to_seq':b.to_seq,
  'checksum':b.checksum,'redelivered':redeliver,'attempts':b.attempts,'items':b.payload,
  'subscription':{'id':sub.id,'name':sub.name,'status':sub.status}}
def apply_downstream(sub_id,data):
 with Session() as s:
  sub=_get(s,sub_id,lock=True)
  b=s.query(DeliveryBatch).filter_by(id=data.batch_id,subscription_id=sub_id).with_for_update().first()
  if not b:raise HTTPException(404,'未找到交付批次')
  if data.checksum!=b.checksum:raise HTTPException(422,'批次校验和不一致，拒绝落库')
  applied=[];skipped=[];upserts=0
  if b.phase=='snapshot':
   for item in b.payload:
    existing=s.query(AppliedEvent).filter_by(subscription_id=sub_id,event_seq=item['born_seq']).first()
    if existing:skipped.append(item['born_seq']);continue
    row=s.query(DownstreamRow).filter_by(subscription_id=sub_id,key_digest=item['key_digest']).with_for_update().first()
    if not row:
     row=DownstreamRow(subscription_id=sub_id,run_id=item['run_id'],key_digest=item['key_digest'],
      payload=item['payload'],content_hash=item['content_hash'],applied_seq=item['born_seq'])
     s.add(row);upserts+=1
    elif item['born_seq']>row.applied_seq:
     row.payload=item['payload'];row.content_hash=item['content_hash'];row.applied_seq=item['born_seq'];row.run_id=item['run_id'];upserts+=1
    else:skipped.append(item['born_seq']);continue
    s.add(AppliedEvent(subscription_id=sub_id,event_seq=item['born_seq'],content_hash=item['content_hash'],result='applied'))
    applied.append(item['born_seq'])
  else:
   for item in b.payload:
    existing=s.query(AppliedEvent).filter_by(subscription_id=sub_id,event_seq=item['seq']).first()
    if existing:
     skipped.append(item['seq']);continue
    row=s.query(DownstreamRow).filter_by(subscription_id=sub_id,key_digest=item['key_digest']).with_for_update().first()
    if not row:
     row=DownstreamRow(subscription_id=sub_id,run_id=item['run_id'],key_digest=item['key_digest'],
      payload=item['payload'],content_hash=item['content_hash'],applied_seq=item['seq'])
     s.add(row);upserts+=1
    elif item['seq']>row.applied_seq:
     row.payload=item['payload'];row.content_hash=item['content_hash'];row.applied_seq=item['seq'];row.run_id=item['run_id'];upserts+=1
    else:skipped.append(item['seq']);continue
    s.add(AppliedEvent(subscription_id=sub_id,event_seq=item['seq'],content_hash=item['content_hash'],result='applied'))
    applied.append(item['seq'])
  s.commit()
  return {'batch_no':b.batch_no,'phase':b.phase,'applied':len(applied),'duplicate_skipped':len(skipped),
   'upserts':upserts,'downstream_count':s.query(DownstreamRow).filter_by(subscription_id=sub_id).count(),
   'note':'回执前重试为同一批次，去重账本保证不重复入库'}
def ack(sub_id,data):
 with Session() as s:
  sub=_get(s,sub_id,lock=True)
  b=s.query(DeliveryBatch).filter_by(id=data.batch_id,subscription_id=sub_id).with_for_update().first()
  if not b:raise HTTPException(404,'未找到交付批次')
  if data.checksum!=b.checksum:raise HTTPException(422,'批次校验和不一致，拒绝回执')
  if b.status=='acked':
   return {'idempotent':True,'batch_no':b.batch_no,'note':'回执已处理过，重复回执不推进游标','subscription':_view(s,sub)}
  if b.phase=='stream':sub.stream_acked_seq=b.to_seq
  b.status='acked';b.acked_at=datetime.now(timezone.utc)
  s.commit();return {'idempotent':False,'batch_no':b.batch_no,'phase':b.phase,
   'stream_acked_seq':sub.stream_acked_seq,'subscription':_view(s,sub)}
def rebuild(sub_id,reason='manual_rebuild'):
 if reason not in REBUILD_REASONS:raise HTTPException(422,'重建原因码无效')
 with Session() as s:
  sub=_get(s,sub_id,lock=True)
  if sub.status=='closed':raise HTTPException(409,'已关闭的接库单不能重建，请新建')
  s.query(DownstreamRow).filter_by(subscription_id=sub_id).delete()
  s.query(AppliedEvent).filter_by(subscription_id=sub_id).delete()
  s.query(DeliveryBatch).filter_by(subscription_id=sub_id).delete()
  watermark=changes.latest_seq(s)
  total=s.query(TargetRow).filter(TargetRow.born_seq<=watermark).count()
  min_seq=s.query(func.min(ChangeEvent.seq)).scalar()
  history=(sub.rebuild_history or [])+[{'at':datetime.now(timezone.utc).isoformat(),'reason':reason,
   'previous_snapshot_seq':sub.snapshot_seq,'new_snapshot_seq':watermark}]
  sub.status='rebuilding';sub.rebuild_reason=None;sub.rebuild_history=history
  sub.snapshot_seq=watermark;sub.snapshot_after_id=0;sub.snapshot_total=total;sub.snapshot_delivered=0
  sub.stream_acked_seq=watermark;sub.retained_from=min_seq if min_seq is not None else watermark+1
  s.commit();s.refresh(sub);return _view(s,sub)
def reclaim(sub_id,retention_limit):
 with Session() as s:
  sub=_get(s,sub_id,lock=True)
  if retention_limit is not None:
   if not 1<=retention_limit<=100000:raise HTTPException(422,'保留上限范围为1-100000')
   sub.retention_limit=retention_limit
  report=changes.reclaim_events(s,sub.retention_limit);s.commit()
  report['subscription_id']=sub_id;return report
def downstream_rows(sub_id):
 detail(sub_id)
 with Session() as s:
  rows=s.query(DownstreamRow).filter_by(subscription_id=sub_id).order_by(DownstreamRow.id).all()
  return {'count':len(rows),'rows':[{'key_digest':r.key_digest,'run_id':r.run_id,'applied_seq':r.applied_seq,
   'content_hash':r.content_hash,'payload':r.payload} for r in rows]}
def close(sub_id):
 with Session() as s:
  sub=_get(s,sub_id,lock=True)
  if _open_batch(s,sub,lock=True):raise HTTPException(409,'尚有未回执批次，不能关闭')
  sub.status='closed';s.commit();return _view(s,sub)
