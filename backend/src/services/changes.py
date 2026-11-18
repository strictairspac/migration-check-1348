from sqlalchemy import func
from src.models.downstream import ChangeMeta,ChangeEvent,Subscription
from src.models.run import TargetRow
from src.services.migration import digest
# 活跃接库单仍占保留额度；关闭/重建切换中的单子按其下限参与，避免误删缺口
ACTIVE=('snapshotting','streaming','rebuild_required','rebuilding')
def _meta(session):
 row=session.query(ChangeMeta).filter_by(id=1).with_for_update().first()
 if not row:
  row=ChangeMeta(id=1,next_seq=1);session.add(row);session.flush()
  row=session.query(ChangeMeta).filter_by(id=1).with_for_update().first()
 return row
def latest_seq(session):
 meta=session.query(ChangeMeta).filter_by(id=1).first()
 return (meta.next_seq-1) if meta else 0
def record_target_changes(session,run_id,target_rows,kind):
 # 与目标行同一事务：锁计数器顺序取号，提交顺序即交付顺序，序列连续无缺口
 if not target_rows:return None
 meta=_meta(session);start=meta.next_seq;seq=start
 events=[]
 for row in target_rows:
  row.born_seq=seq
  events.append(ChangeEvent(seq=seq,kind=kind,run_id=run_id,target_row_id=row.id,
   key_digest=row.key_digest,payload=row.payload,content_hash=digest(row.payload)))
  seq+=1
 meta.next_seq=seq;session.add_all(events)
 return {'from_seq':start,'to_seq':seq-1,'count':len(events)}
def reclaim_events(session,retention_limit):
 # 全局可保留条数取所有活跃接库单保留上限的最小值；任何单子都不会被跳过缺口
 if retention_limit<1:raise ValueError('保留上限至少为1')
 caps=[retention_limit]+[r.retention_limit for r in session.query(Subscription).filter(Subscription.status.in_(ACTIVE)).all()]
 cap=min(caps)
 top=latest_seq(session)
 horizon=top-cap+1
 if horizon<=1:return {'deleted':0,'horizon':1,'retained':session.query(ChangeEvent).count(),'cap':cap}
 deleted=session.query(ChangeEvent).filter(ChangeEvent.seq<horizon).delete(synchronize_session=False)
 session.flush()
 min_seq=session.query(func.min(ChangeEvent.seq)).scalar()
 new_from=min_seq if min_seq is not None else top+1
 for sub in session.query(Subscription).filter(Subscription.retained_from<new_from).all():sub.retained_from=new_from
 return {'deleted':deleted,'horizon':new_from,'retained':session.query(ChangeEvent).count(),'cap':cap}
