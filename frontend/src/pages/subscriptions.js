import {store,refresh} from '../stores/business.js';
import {esc,json} from '../utils/html.js';
import {table} from '../components/common/table.js';
import {subscriptions} from '../api/subscriptions.js';
import {SUB_STATUSES,REBUILD_REASONS} from '../constants/enums.js';
import {feedback} from '../components/common/feedback.js';
const subStatus=s=>`<span class="status ${s==='streaming'?'completed':s==='rebuild_required'?'failed':s==='closed'?'cancelled':'running'}">${SUB_STATUSES[s]||s}</span>`;
function progressBar(sub){
 const pct=sub.snapshot_total?Math.min(100,Math.round(sub.snapshot_delivered/sub.snapshot_total*100)):(sub.status==='streaming'?100:0);
 return `<div class="snap-progress"><div style="width:${pct}%"></div><small>${sub.snapshot_delivered}/${sub.snapshot_total} 行 · ${pct}% · 水位seq=${sub.snapshot_seq}</small></div>`;
}
function rebuildBlock(sub){
 if(!sub.rebuild_reason && !(sub.rebuild_history||[]).length)return '';
 const reason=sub.rebuild_reason?`<p class="feedback error">当前重建原因：${esc(REBUILD_REASONS[sub.rebuild_reason]||sub.rebuild_reason)}</p>`:'';
 return `${reason}<details><summary>重建历史（${(sub.rebuild_history||[]).length}次）</summary><pre>${json(sub.rebuild_history)}</pre></details>`;
}
function row(sub){
 const canWrite=store.user.role!=='viewer';
 const open=sub.open_batch;
 const batchButtons=open?`<p class="muted">待回执批次 #${open.batch_no}（${open.phase==='snapshot'?'全量':'增量'}，已投递${open.attempts}次，${open.items}条）。回执丢失会重投同一批次。</p>
  <button data-apply="${sub.id}" data-batch="${open.batch_no}" data-checksum="${open.checksum}">① 下游幂等落库</button>
  <button class="quiet" data-ack="${sub.id}" data-batch="${open.batch_no}" data-checksum="${open.checksum}">② 回执确认</button>
  <button class="quiet" data-lostack="${sub.id}" data-batch="${open.batch_no}" data-checksum="${open.checksum}">模拟回执丢失后重试</button>`
 :`<button data-pull-snap="${sub.id}" ${sub.status!=='snapshotting'&&sub.status!=='rebuilding'?'disabled':''}>拉全量下一批（≤100行/短事务）</button>
  <button data-pull-stream="${sub.id}" ${sub.status!=='streaming'?'disabled':''}>拉增量连续序</button>`;
 return [`<b>${esc(sub.name)}</b><small>${esc(sub.downstream)}<br>建单 ${esc(sub.created_at)}</small><br>${subStatus(sub.status)}`,
  `${progressBar(sub)}<small>增量已确认seq=${sub.stream_acked_seq} ｜ 最早保留seq=${sub.retained_from} ｜ 保留上限=${sub.retention_limit} ｜ 最新事件seq=${sub.latest_seq}</small><small>下游库 ${sub.downstream_count} 行 ｜ 去重账本 ${sub.applied_count} 条</small>`,
  `<div class="sub-actions">${canWrite?batchButtons:''}
   <button class="quiet" data-downstream="${sub.id}">查看下游库</button>
   ${canWrite?`<button class="quiet" data-reclaim="${sub.id}">按上限回收增量</button>
   <button class="quiet" data-rebuild="${sub.id}">重建接库单</button>
   <button class="quiet" data-close="${sub.id}">关闭</button>`:''}</div>
   <pre id="downstream-${sub.id}" hidden></pre>${rebuildBlock(sub)}`];
}
export function subscriptionsPage(){
 return `<div class="split"><section><h2>登记一个下游检验库接库单</h2>
 <p>先按接库时刻水位分批抄全量（每批独立短事务，不长占锁）；全量交接后切换增量，变更按提交顺序连续交付，同值恢复也算一次变更。</p>
 <form id="subscription-form">
  <label>接库单名称<input name="name" required placeholder="qms-下游复核库-20261118"></label>
  <label>下游库标识<input name="downstream" required placeholder="qms-review-db"></label>
  <label>增量保留上限（条）<input name="retention_limit" type="number" min="1" max="100000" value="200" required></label>
  <label>备注<input name="note" placeholder="接库负责人与用途"></label>
  <button type="submit">建立接库单（记录全量水位）</button>
 </form>
 <ol class="steps"><li>建单冻结水位 seq，全量只抄水位前目标行</li><li>分批拉全量→下游幂等落库→逐批回执</li><li>全量完成自动转增量，按 seq 连续交付</li><li>回收后若出现缺口，禁止跳过，提示重建</li></ol>
 </section><section><h2>接库进度 <em>${store.subscriptions.length}</em></h2>
 ${table(['接库单 / 状态','进度 / 游标','操作'],store.subscriptions.map(row))}
 <aside class="note">回执丢失时重试拿到的是同一开放批次（batch_no/checksum 不变）；下游按自然键 upsert 并以事件 seq 去重，重复投递不会二次入库。保留上限按所有接库单的最小上限全局回收，追不上的单子会被标记“需重建”并记录原因。</aside>
 </section></div>`;
}
function formatResult(r){
 if(r?.redelivered)return `重投批次 #${r.batch_no}（${r.phase}），checksum不变，下游去重不会重复入库`;
 if(r?.finished)return r.message;
 if(r?.empty)return `暂无增量，下一条待收seq=${r.next_seq}`;
 if(r?.batch_no)return `批次 #${r.batch_no}（${r.phase==='snapshot'?'全量':'增量'}）${r.items?.length||0}条已生成，checksum=${r.checksum.slice(0,16)}…`;
 if(r?.idempotent!==undefined)return r.idempotent?'重复回执已忽略，游标不重复推进':`批次#${r.batch_no}已回执，增量游标→${r.stream_acked_seq??'不变'}`;
 if(r?.applied!==undefined)return `下游落库：应用${r.applied}条，重复跳过${r.duplicate_skipped}条，库内共${r.downstream_count}行`;
 if(r?.deleted!==undefined)return `回收完成：删除${r.deleted}条，保留${r.retained}条（全局cap=${r.cap}，新保留起点seq=${r.horizon}）`;
 if(r?.status)return `接库单状态：${SUB_STATUSES[r.status]||r.status}`;
 return '操作完成';
}
export function bindSubscriptionActions(rerender){
 const form=document.getElementById('subscription-form');
 form?.addEventListener('submit',async e=>{e.preventDefault();try{
  const d=Object.fromEntries(new FormData(form));d.retention_limit=Number(d.retention_limit);
  await subscriptions.create(d);await refresh();rerender();
 }catch(e){feedback(e.message,true);}});
 const run=async(btn,fn)=>{btn.disabled=true;try{const r=await fn();await refresh();rerender();feedback(formatResult(r));}
  catch(e){feedback(e.message,true);}finally{btn.disabled=false;}};
 document.querySelectorAll('[data-pull-snap]').forEach(b=>b.onclick=()=>run(b,()=>subscriptions.pullSnapshot(b.dataset.pullSnap)));
 document.querySelectorAll('[data-pull-stream]').forEach(b=>b.onclick=()=>run(b,()=>subscriptions.pullStream(b.dataset.pullStream)));
 document.querySelectorAll('[data-apply]').forEach(b=>b.onclick=()=>run(b,()=>subscriptions.apply(b.dataset.apply,Number(b.dataset.batch),b.dataset.checksum)));
 document.querySelectorAll('[data-ack]').forEach(b=>b.onclick=()=>run(b,()=>subscriptions.ack(b.dataset.ack,Number(b.dataset.batch),b.dataset.checksum)));
 document.querySelectorAll('[data-lostack]').forEach(b=>b.onclick=async()=>{
  const id=b.dataset.lostack,batch=Number(b.dataset.batch),checksum=b.dataset.checksum;
  b.disabled=true;
  try{
   // 第一步：下游已落库，但回执在网络中丢失（不调用 ack）
   const applied=await subscriptions.apply(id,batch,checksum);
   // 第二步：下游重试，服务端重投同一开放批次（batch_no/checksum不变），再幂等落库+补回执
   const redelivered=await subscriptions.pullSnapshot(id).catch(()=>subscriptions.pullStream(id));
   if(redelivered?.batch_id){await subscriptions.apply(id,redelivered.batch_id,redelivered.checksum);await subscriptions.ack(id,redelivered.batch_id,redelivered.checksum);}
   await refresh();rerender();
   feedback(`回执丢失演练：首次落库应用${applied.applied}条；重投批次#${redelivered?.batch_no}后重复跳过，仅入库一次`);
  }catch(e){feedback(e.message,true);}finally{b.disabled=false;}
 });
 document.querySelectorAll('[data-reclaim]').forEach(b=>b.onclick=()=>{const v=prompt('设置本接库单增量保留上限（条），确认后全局回收','5');if(v===null)return;run(b,()=>subscriptions.reclaim(b.dataset.reclaim,Number(v)));});
 document.querySelectorAll('[data-rebuild]').forEach(b=>b.onclick=()=>{if(!confirm('重建会清空该接库单的模拟下游库与去重账本，按当前水位重新抄全量，确认？'))return;run(b,()=>subscriptions.rebuild(b.dataset.rebuild));});
 document.querySelectorAll('[data-close]').forEach(b=>b.onclick=()=>run(b,()=>subscriptions.close(b.dataset.close)));
 document.querySelectorAll('[data-downstream]').forEach(b=>b.onclick=async()=>{
  const el=document.getElementById('downstream-'+b.dataset.downstream);if(!el)return;
  if(!el.hidden){el.hidden=true;return;}
  try{const r=await subscriptions.downstream(b.dataset.downstream);el.hidden=false;el.textContent=JSON.stringify(r,null,2);}
  catch(e){feedback(e.message,true);}
 });
}
