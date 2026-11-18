import {api,post} from './client.js';
export const subscriptions={
 list:()=>api('/subscriptions'),
 create:d=>post('/subscriptions',d),
 detail:id=>api('/subscriptions/'+id),
 pullSnapshot:id=>post('/subscriptions/'+id+'/snapshot/pull',{}),
 pullStream:(id,limit=100)=>post('/subscriptions/'+id+'/stream/pull?limit='+limit,{}),
 apply:(id,batch_id,checksum)=>post('/subscriptions/'+id+'/apply',{batch_id,checksum}),
 ack:(id,batch_id,checksum)=>post('/subscriptions/'+id+'/ack',{batch_id,checksum}),
 rebuild:(id,reason='manual_rebuild')=>post('/subscriptions/'+id+'/rebuild',{reason}),
 reclaim:(id,retention_limit=null)=>post('/subscriptions/'+id+'/reclaim',{retention_limit}),
 downstream:id=>api('/subscriptions/'+id+'/downstream'),
 close:id=>post('/subscriptions/'+id+'/close',{})
};
