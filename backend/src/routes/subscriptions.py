from fastapi import APIRouter,Depends,Query
from src.middlewares.auth import current,writer
from src.schemas.business import SubscriptionInput,BatchAck,ReclaimInput,RebuildInput
from src.services import subscriptions
from src.services.audit import log
router=APIRouter()
@router.get('/subscriptions')
def listing(u=Depends(current)):return subscriptions.list_items()
@router.post('/subscriptions')
def create(data:SubscriptionInput,u=Depends(writer)):
 r=subscriptions.create(data);log(u.email,'subscription.create',r['name']);return r
@router.get('/subscriptions/{sub_id}')
def detail(sub_id:int,u=Depends(current)):return subscriptions.detail(sub_id)
@router.post('/subscriptions/{sub_id}/snapshot/pull')
def pull_snapshot(sub_id:int,u=Depends(current)):return subscriptions.pull_snapshot(sub_id)
@router.post('/subscriptions/{sub_id}/stream/pull')
def pull_stream(sub_id:int,limit:int=Query(default=100,ge=1,le=500),u=Depends(current)):
 return subscriptions.pull_stream(sub_id,limit)
@router.post('/subscriptions/{sub_id}/apply')
def apply(sub_id:int,data:BatchAck,u=Depends(writer)):
 r=subscriptions.apply_downstream(sub_id,data);log(u.email,'subscription.apply',f'{sub_id}#batch{data.batch_id}');return r
@router.post('/subscriptions/{sub_id}/ack')
def ack(sub_id:int,data:BatchAck,u=Depends(writer)):
 r=subscriptions.ack(sub_id,data);log(u.email,'subscription.ack',f'{sub_id}#batch{data.batch_id}');return r
@router.post('/subscriptions/{sub_id}/rebuild')
def rebuild(sub_id:int,data:RebuildInput,u=Depends(writer)):
 r=subscriptions.rebuild(sub_id,data.reason);log(u.email,'subscription.rebuild',f'{sub_id}:{data.reason}');return r
@router.post('/subscriptions/{sub_id}/reclaim')
def reclaim(sub_id:int,data:ReclaimInput,u=Depends(writer)):
 r=subscriptions.reclaim(sub_id,data.retention_limit);log(u.email,'subscription.reclaim',f'{sub_id}:{r}');return r
@router.get('/subscriptions/{sub_id}/downstream')
def downstream(sub_id:int,u=Depends(current)):return subscriptions.downstream_rows(sub_id)
@router.post('/subscriptions/{sub_id}/close')
def close(sub_id:int,u=Depends(writer)):
 r=subscriptions.close(sub_id);log(u.email,'subscription.close',str(sub_id));return r
