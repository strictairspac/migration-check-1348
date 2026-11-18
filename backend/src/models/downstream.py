from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey,UniqueConstraint,Text
from src.config.database import Base
class ChangeMeta(Base):
 # 单行计数器，事务内锁行取号，保证变更序连续无缺口且按提交顺序可见
 __tablename__='change_meta'
 id=Column(Integer,primary_key=True)
 next_seq=Column(Integer,nullable=False,default=1)
class ChangeEvent(Base):
 __tablename__='change_events'
 id=Column(Integer,primary_key=True)
 seq=Column(Integer,unique=True,nullable=False,index=True)
 kind=Column(String(20),nullable=False)
 run_id=Column(Integer,ForeignKey('runs.id'),nullable=False)
 target_row_id=Column(Integer,nullable=False)
 key_digest=Column(String(64),nullable=False,index=True)
 payload=Column(JSON,nullable=False)
 content_hash=Column(String(64),nullable=False)
 created_at=Column(DateTime,server_default=func.now())
class Subscription(Base):
 __tablename__='subscriptions'
 id=Column(Integer,primary_key=True)
 name=Column(String(120),unique=True,nullable=False)
 downstream=Column(String(120),nullable=False)
 status=Column(String(20),nullable=False,default='snapshotting')
 # 接库水位：全量只交 born_seq<=snapshot_seq 的目标行，之后走增量
 snapshot_seq=Column(Integer,nullable=False)
 # 全量 keyset 分页游标（target_rows.id），为0表示从头
 snapshot_after_id=Column(Integer,nullable=False,default=0)
 snapshot_total=Column(Integer,nullable=False,default=0)
 snapshot_delivered=Column(Integer,nullable=False,default=0)
 # 增量已确认序：下一条待交付为 stream_acked_seq+1
 stream_acked_seq=Column(Integer,nullable=False,default=0)
 # 已被回收的最低保留序；拉取发现缺口时据此判定重建
 retained_from=Column(Integer,nullable=False,default=1)
 retention_limit=Column(Integer,nullable=False,default=200)
 rebuild_reason=Column(String(60),nullable=True)
 rebuild_history=Column(JSON,nullable=False,default=list)
 note=Column(String(500),nullable=False,default='')
 created_at=Column(DateTime,server_default=func.now())
class DeliveryBatch(Base):
 __tablename__='delivery_batches'
 __table_args__=(UniqueConstraint('subscription_id','batch_no'),)
 id=Column(Integer,primary_key=True)
 subscription_id=Column(Integer,ForeignKey('subscriptions.id'),nullable=False)
 batch_no=Column(Integer,nullable=False)
 phase=Column(String(20),nullable=False)
 # stream 批次的起止序；snapshot 批次起止序为空
 from_seq=Column(Integer,nullable=True)
 to_seq=Column(Integer,nullable=True)
 payload=Column(JSON,nullable=False)
 checksum=Column(String(64),nullable=False)
 status=Column(String(20),nullable=False,default='open')
 attempts=Column(Integer,nullable=False,default=1)
 delivered_at=Column(DateTime,server_default=func.now())
 acked_at=Column(DateTime,nullable=True)
class DownstreamRow(Base):
 # 模拟下游检验库：自然键幂等 upsert，重复回执不会二次入库
 __tablename__='downstream_rows'
 __table_args__=(UniqueConstraint('subscription_id','key_digest'),)
 id=Column(Integer,primary_key=True)
 subscription_id=Column(Integer,ForeignKey('subscriptions.id'),nullable=False)
 run_id=Column(Integer,nullable=False)
 key_digest=Column(String(64),nullable=False)
 payload=Column(JSON,nullable=False)
 content_hash=Column(String(64),nullable=False)
 applied_seq=Column(Integer,nullable=False)
 updated_at=Column(DateTime,server_default=func.now())
class AppliedEvent(Base):
 # 下游去重账本：同一条事件（含同值恢复）只生效一次
 __tablename__='applied_events'
 __table_args__=(UniqueConstraint('subscription_id','event_seq'),)
 id=Column(Integer,primary_key=True)
 subscription_id=Column(Integer,ForeignKey('subscriptions.id'),nullable=False)
 event_seq=Column(Integer,nullable=False)
 content_hash=Column(String(64),nullable=False)
 result=Column(String(20),nullable=False)
 created_at=Column(DateTime,server_default=func.now())
