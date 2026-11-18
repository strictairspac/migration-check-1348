"""下游接库全量/增量交接验证（本地 SQLite 等价验证；生产为 PostgreSQL + Redis）。

覆盖需求：
A 全量按接库水位分批、并发迁移照常落库，全量不纳入建单后新行
B 增量按提交顺序、连续序、同值恢复也算一次变更
C 回执丢失重试不重复入库（开放批次重投 + 下游幂等去重）
D 可设上限回收；追不上提示重建，不能跳过缺口
E 重建后按新水位重新全量；重建原因留痕
F 角色权限与入参校验
运行：PYTHONPATH=/workspace/backend python -I verify_handover.py
"""
import os,sys
DB='/tmp/downstream_test.db'
# 必须连同 -wal/-shm 一起清掉，避免上次异常退出残留的热 WAL 造成锁竞争
for ext in ('','-wal','-shm'):
 try:os.remove(DB+ext)
 except OSError:pass
os.environ['DATABASE_URL']='sqlite:///'+DB
os.environ['JWT_SECRET']='verify-secret'
os.environ['SEED_DEMO']='false'
sys.path.insert(0,'/workspace/backend')

# ---- 假 redis：本地无 redis 服务，仅需 rpush/ping/blpop 接口 ----
class FakeRedis:
 def __init__(self):self.q=[]
 def rpush(self,k,v):self.q.append(v)
 def blpop(self,k,timeout=2):return None
 def ping(self):return True
import redis as _redis
_redis.Redis.from_url=lambda url:FakeRedis()

from fastapi.testclient import TestClient
from sqlalchemy import event
from src.config.database import Base,Session
from src.config.database import engine
# SQLite 默认库级写锁；开启 WAL 允许长事务读与短事务写并发（生产 PostgreSQL 为行级锁，无需此设置）
@event.listens_for(engine.sync_engine if hasattr(engine,'sync_engine') else engine,'connect')
def _wal(dbapi_conn,_):
 cur=dbapi_conn.cursor();cur.execute('PRAGMA journal_mode=WAL');cur.execute('PRAGMA busy_timeout=30000');cur.close()
from src.models.identity import User
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.models.downstream import ChangeEvent,Subscription,DeliveryBatch,DownstreamRow,AppliedEvent
from src.utils.security import hash_password
from src.services import changes
from src.services.migration import migrate,make_backup,key_digest,digest
import src.main as main_mod

Base.metadata.create_all(engine)
RULES=[{'source':a,'target':b,'type':c,'required':True} for a,b,c in
       [('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),
        ('单位','unit','text'),('时间','measured_at','datetime')]]
def mk_rows(n,prefix='JS',base=420):
 return [{'委托号':f'{prefix}-{i:04d}','检测项':'拉伸强度','结果':str(base+i/10),'单位':'MPa',
          '时间':'2026-11-17T09:00:00+08:00'} for i in range(1,n+1)]
def do_migration(name,rows,chunk_size=50):
 """复刻 worker 的成功提交事务（含变更事件登记）。"""
 with Session() as s:
  b=Batch(name=name+'-批次',source_file=name+'.json',description='verify',rows=rows);s.add(b);s.flush()
  m=Mapping(name=name+'-映射',batch_id=b.id,rules=RULES,chunk_size=chunk_size);s.add(m);s.flush()
  r=Run(name=name,batch_id=b.id,mapping_id=m.id,status='queued',
        snapshot={'batch':{'rows':rows},'mapping':{'rules':RULES,'chunk_size':chunk_size}})
  s.add(r);s.flush();run_id=r.id;s.commit()
 result=migrate(rows,RULES,chunk_size)
 with Session() as s:
  rr=s.get(Run,run_id);new=[]
  for i,row in enumerate(result['accepted_rows']):
   tr=TargetRow(run_id=run_id,ordinal=i,key_digest=key_digest(row),payload=row);s.add(tr);new.append(tr)
  s.flush();rng=changes.record_target_changes(s,run_id,new,'upsert')
  result['change_range']=rng;rr.result=result;rr.status='completed';s.commit()
 return run_id,result
def do_restore(run_id):
 """复刻 runs.restore：同值恢复也登记 restore 事件。"""
 with Session() as s:
  r=s.query(Run).filter_by(id=run_id).with_for_update().first()
  rows=[x.payload for x in s.query(TargetRow).filter_by(run_id=run_id).order_by(TargetRow.ordinal)]
  backup=make_backup({'accepted_rows':rows,'row_hashes':[digest(x) for x in rows],
                      'checksum':digest(rows)},run_id,'verify')
  s.query(TargetRow).filter_by(run_id=run_id).delete();s.flush()
  restored=[]
  for i,row in enumerate(rows):
   tr=TargetRow(run_id=run_id,ordinal=i,key_digest=key_digest(row),payload=row);s.add(tr);restored.append(tr)
  s.flush();rng=changes.record_target_changes(s,run_id,restored,'restore');s.commit()
 return rng,backup

with Session() as s:
 for role in ('admin','operator','viewer'):
  s.add(User(email=role+'@migration.local',role=role,password=hash_password('Migrate2026!')))
 s.commit()

client=TestClient(main_mod.app)
def login(role):
 r=client.post('/auth/login',json={'email':f'{role}@migration.local','password':'Migrate2026!'})
 assert r.status_code==200,r.text
 return {'Authorization':'Bearer '+r.json()['token']}
ADMIN,OPERATOR,VIEWER=login('admin'),login('operator'),login('viewer')
PASS=[];FAIL=[]
def check(name,cond,detail=''):
 (PASS if cond else FAIL).append(name)
 print(('  PASS' if cond else '  FAIL'),name,('' if cond else '-> '+str(detail)))
def pull_snapshot_until_finish(h,sub_id):
 # 每批必须先落库+回执关闭，下一次拉取才会产生新批次，否则开放批次会按回执丢失语义重投
 batches=[]
 while True:
  r=client.post(f'/subscriptions/{sub_id}/snapshot/pull',headers=h)
  assert r.status_code==200,r.text;d=r.json()
  if d.get('finished'):return batches,d
  apply_ack(h,sub_id,d);batches.append(d)
def apply_ack(h,sub_id,b):
 ar=client.post(f'/subscriptions/{sub_id}/apply',headers=h,json={'batch_id':b['batch_id'],'checksum':b['checksum']})
 assert ar.status_code==200,ar.text
 kr=client.post(f'/subscriptions/{sub_id}/ack',headers=h,json={'batch_id':b['batch_id'],'checksum':b['checksum']})
 assert kr.status_code==200,kr.text
 return ar.json(),kr.json()
def drain_snapshot_full(h,sub_id):
 n=0
 while True:
  d=client.post(f'/subscriptions/{sub_id}/snapshot/pull',headers=h).json()
  if d.get('finished'):return n
  ar,_=apply_ack(h,sub_id,d);n+=ar['applied']

print('\n== 准备：第一次迁移 250 行（跨多批，SNAPSHOT_PAGE=100） ==')
run1,res1=do_migration('verify-run-1',mk_rows(250))
with Session() as s:
 ev_count=s.query(ChangeEvent).count();seqs=[e.seq for e in s.query(ChangeEvent).order_by(ChangeEvent.seq)]
check('迁移登记250条变更事件',ev_count==250,ev_count)
check('变更序连续无缺口 1..250',seqs==list(range(1,251)),seqs[:5])
check('target_rows 全部回填 born_seq',s.query(TargetRow).filter(TargetRow.born_seq==0).count()==0)

print('\n== A. 建单水位 + 全量分批，期间并发新迁移不进全量 ==')
r=client.post('/subscriptions',headers=OPERATOR,json={'name':'下游复核库接库单','downstream':'qms-review-db','retention_limit':200})
assert r.status_code==200,r.text;main_sub=r.json();sid=main_sub['id']
r=client.post('/subscriptions',headers=OPERATOR,json={'name':'慢消费者接库单','downstream':'slow-db','retention_limit':3})
slow=r.json();slid=slow['id']
check('建单状态 snapshotting、水位=250',main_sub['status']=='snapshotting' and main_sub['snapshot_seq']==250 and main_sub['snapshot_total']==250,main_sub)
# 抄库期间并发：第二次迁移落库 30 行（只进增量，不进全量）
run2,res2=do_migration('verify-run-2-concurrent',mk_rows(30,prefix='CN'))
with Session() as s:check('并发迁移照常落库（280目标行/280事件）',s.query(TargetRow).count()==280 and s.query(ChangeEvent).count()==280)
pulled,finished=pull_snapshot_until_finish(OPERATOR,sid)
check('全量分3批（100/100/50）',[len(b['items']) for b in pulled]==[100,100,50],[len(b['items']) for b in pulled])
allkeys=[it['key_digest'] for b in pulled for it in b['items']]
check('全量250行且无重复',len(allkeys)==250 and len(set(allkeys))==250)
check('全量只含水位前 run1 的行',all(it['run_id']==run1 for b in pulled for it in b['items']))
check('全量完成自动切 streaming',finished['subscription']['status']=='streaming',finished)
# 助手已对每批落库+回执，直接核对下游库行数
n=client.get(f'/subscriptions/{sid}/downstream',headers=OPERATOR).json()['count']
check('主单全量下游落库250行',n==250,n)

print('\n== A2. 主单增量先追平并发迁移 seq251..280 ==')
d=client.post(f'/subscriptions/{sid}/stream/pull',headers=OPERATOR).json()
check('增量首批交付并发迁移30条 seq251..280',d.get('from_seq')==251 and d.get('to_seq')==280 and len(d['items'])==30,d)
check('增量事件按 seq 升序',[it['seq'] for it in d['items']]==list(range(251,281)))
ar,kr=apply_ack(OPERATOR,sid,d)
check('增量30条落库，下游累计280行',ar['applied']==30 and ar['downstream_count']==280,ar)
check('回执后游标推进到280',kr['stream_acked_seq']==280,kr)

print('\n== D. 回收上限 + 慢消费者追不上提示重建，禁止跳过缺口 ==')
# 慢单建单后从未拉取；主单已追到280，不会被全局回收误伤
rep=client.post(f'/subscriptions/{slid}/reclaim',headers=OPERATOR,json={'retention_limit':3}).json()
check('回收按最小上限cap=3执行',rep['cap']==3 and rep['horizon']==278 and rep['deleted']==277 and rep['retained']==3,rep)
# 慢单全量走 target_rows，不依赖事件留存，仍可完成
applied_slow=drain_snapshot_full(OPERATOR,slid)
check('慢单全量仍完成250行（全量不依赖事件留存）',applied_slow==250,applied_slow)
sub_now=client.get(f'/subscriptions/{slid}',headers=OPERATOR).json()
check('慢单 retained_from 被回收推进到278',sub_now['retained_from']==278,sub_now)
r=client.post(f'/subscriptions/{slid}/stream/pull',headers=OPERATOR)
check('缺口返回409且明确不能跳过',r.status_code==409 and '缺口' in r.json()['detail'],(r.status_code,r.text))
sub_now=client.get(f'/subscriptions/{slid}',headers=OPERATOR).json()
check('接库单标记 rebuild_required + 原因 gap_detected',sub_now['status']=='rebuild_required' and sub_now['rebuild_reason']=='gap_detected',sub_now)
check('重建原因写入历史留痕',[h['reason'] for h in sub_now['rebuild_history']]==['gap_detected'],sub_now['rebuild_history'])
r=client.post(f'/subscriptions/{slid}/stream/pull',headers=OPERATOR)
check('需重建状态拒绝继续收数（不能跳过缺口）',r.status_code==409,r.status_code)

print('\n== E. 重建：清空下游、按新水位重新全量 ==')
rb=client.post(f'/subscriptions/{slid}/rebuild',headers=OPERATOR,json={'reason':'manual_rebuild'}).json()
with Session() as s:
 check('重建清空下游库/去重账本/交付批次',s.query(DownstreamRow).filter_by(subscription_id=slid).count()==0
       and s.query(AppliedEvent).filter_by(subscription_id=slid).count()==0
       and s.query(DeliveryBatch).filter_by(subscription_id=slid).count()==0)
check('重建状态 rebuilding 且清空旧原因',rb['status']=='rebuilding' and rb['rebuild_reason'] is None,rb)
check('重建水位刷新到最新280',rb['snapshot_seq']==280 and rb['stream_acked_seq']==280 and rb['retained_from']==278,rb)
applied_slow=drain_snapshot_full(OPERATOR,slid)
check('重建后重新抄全量280行',applied_slow==280,applied_slow)
sub_now=client.get(f'/subscriptions/{slid}',headers=OPERATOR).json()
check('重建全量完成回到 streaming',sub_now['status']=='streaming',sub_now['status'])
check('重建历史含 gap + manual 两次',[h['reason'] for h in sub_now['rebuild_history']]==['gap_detected','manual_rebuild'],sub_now['rebuild_history'])

print('\n== B. 同值恢复也算一次变更，增量连续交付 ==')
rng,backup=do_restore(run1)
check('同值恢复仍产生250条 restore 事件 seq281..530',rng['count']==250 and rng['from_seq']==281 and rng['to_seq']==530,rng)
with Session() as s:
 kinds={k for (k,) in s.query(ChangeEvent.kind).filter(ChangeEvent.seq>=281).distinct()}
check('恢复事件 kind=restore',kinds=={'restore'},kinds)
d=client.post(f'/subscriptions/{sid}/stream/pull?limit=500',headers=OPERATOR).json()
check('增量连续交付 restore 事件 seq281..530',d.get('from_seq')==281 and d.get('to_seq')==530 and len(d['items'])==250,d)
check('同值恢复内容哈希与迁移时该行一致',d['items'][0]['content_hash']==digest(res1['accepted_rows'][0]))
ar,kr=apply_ack(OPERATOR,sid,d)
check('恢复事件落库为同键覆盖，下游仍280行',ar['applied']==250 and ar['duplicate_skipped']==0 and ar['downstream_count']==280,ar)
d=client.post(f'/subscriptions/{sid}/stream/pull',headers=OPERATOR).json()
check('无新变更时增量返回空并给出 next_seq=531',d.get('empty') and d.get('next_seq')==531,d)

print('\n== C. 回执丢失重试：重投同一批次，不重复入库 ==')
run3,_=do_migration('verify-run-3',mk_rows(5,prefix='NX'))
b1=client.post(f'/subscriptions/{sid}/stream/pull',headers=OPERATOR).json()
check('新批次 seq531..535',b1['from_seq']==531 and b1['to_seq']==535,b1)
# 错误校验和必须拒绝
bad=client.post(f'/subscriptions/{sid}/apply',headers=OPERATOR,json={'batch_id':b1['batch_id'],'checksum':'f'*64})
check('校验和不一致拒绝落库(422)',bad.status_code==422,bad.status_code)
# 下游落库成功，但回执丢失（不 ack）
ar=client.post(f'/subscriptions/{sid}/apply',headers=OPERATOR,json={'batch_id':b1['batch_id'],'checksum':b1['checksum']}).json()
check('首次落库应用5条',ar['applied']==5 and ar['duplicate_skipped']==0 and ar['downstream_count']==285,ar)
# 重试拉取：重投同一开放批次
b2=client.post(f'/subscriptions/{sid}/stream/pull',headers=OPERATOR).json()
check('重试重投同一批次（batch_id/checksum 不变）',b2.get('redelivered') and b2['batch_id']==b1['batch_id'] and b2['checksum']==b1['checksum'],b2)
check('重投 attempts 累加',b2['attempts']>=2,b2.get('attempts'))
ar2=client.post(f'/subscriptions/{sid}/apply',headers=OPERATOR,json={'batch_id':b2['batch_id'],'checksum':b2['checksum']}).json()
check('重试落库5条全部去重跳过，不重复入库',ar2['applied']==0 and ar2['duplicate_skipped']==5 and ar2['downstream_count']==285,ar2)
kr=client.post(f'/subscriptions/{sid}/ack',headers=OPERATOR,json={'batch_id':b1['batch_id'],'checksum':b1['checksum']}).json()
check('回执成功推进游标到535',kr['stream_acked_seq']==535,kr)
kr2=client.post(f'/subscriptions/{sid}/ack',headers=OPERATOR,json={'batch_id':b1['batch_id'],'checksum':b1['checksum']}).json()
check('重复回执幂等不重复推进',kr2.get('idempotent') is True,kr2)
check('下游库最终285行（280+5，无重复）',client.get(f'/subscriptions/{sid}/downstream',headers=OPERATOR).json()['count']==285)

print('\n== F. 权限、校验与边界 ==')
r=client.post('/subscriptions',headers=VIEWER,json={'name':'无权建单','downstream':'x'})
check('viewer 禁止建单(403)',r.status_code==403,r.status_code)
r=client.post(f'/subscriptions/{sid}/apply',headers=VIEWER,json={'batch_id':1,'checksum':'0'*64})
check('viewer 禁止落库(403)',r.status_code==403,r.status_code)
r=client.get('/subscriptions',headers=VIEWER)
check('viewer 可只读查看接库单',r.status_code==200 and len(r.json())==2)
r=client.post('/subscriptions',headers=OPERATOR,json={'name':'x','downstream':'y','retention_limit':0})
check('保留上限越界被拒(422)',r.status_code==422,r.status_code)
r=client.get('/subscriptions')
check('无令牌拒绝(401)',r.status_code in (401,403),r.status_code)
# 关闭的接库单不能重建
client.post(f'/subscriptions/{sid}/close',headers=OPERATOR)
r=client.post(f'/subscriptions/{sid}/rebuild',headers=OPERATOR,json={'reason':'manual_rebuild'})
check('已关闭接库单拒绝重建(409)',r.status_code==409,r.status_code)
r=client.post(f'/subscriptions/{sid}/stream/pull',headers=OPERATOR)
check('关闭后拒绝拉取(409)',r.status_code==409,r.status_code)

print(f'\n结果：通过 {len(PASS)} 项，失败 {len(FAIL)} 项')
if FAIL:
 print('失败项：',FAIL);sys.exit(1)
print('全部断言通过')
