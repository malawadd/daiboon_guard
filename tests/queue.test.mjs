import {test} from 'node:test';
import assert from 'node:assert/strict';
import {runTicket,drain,queueHealth,discardDeleted} from '../src/server/engine.ts';
import {enqueue} from '../src/server/queue.ts';
import {Q, claim, dueTickets, getSlot, itemFromTicket, metrics, releaseIfOwned, retryOrDead, MAX_ATTEMPTS} from '../src/server/queue.ts';
import {H,HISTORY_TTL,HISTORY_LIMIT,newRecord,recordKey,saveRecord,loadRecord,recentRecords,purgeItemHistory,maintainHistory,retainRemovalHistory} from '../src/server/history.ts';
import {deliverNotice,drainNotices,explanation,noticeMarker} from '../src/server/notifications.ts';
import {moderatorRoutes} from '../src/server/moderator.ts';

class RedisFake {
  constructor() {this.k=new Map();this.z=new Map();this.h=new Map();this.rangeCalls=[];}
  async set(k,v,o={}) {
    if (o.expiration !== undefined) {
      assert.ok(o.expiration instanceof Date, 'Devvit expiration must be a Date');
      assert.ok(o.expiration.getTime() > Date.now(), 'lock expiration must be in the future');
    }
    if(o.nx&&this.k.has(k))return null;this.k.set(k,v);return 'OK';
  }
  async get(k){return this.k.get(k)??null;}
  async del(k){this.k.delete(k);}
  async incrBy(k,n){const r=Number(this.k.get(k)||0)+n;this.k.set(k,String(r));return r;}
  async expire(){return 1;}
  async zAdd(k,...entries){let x=this.z.get(k);if(!x){x=new Map();this.z.set(k,x);}for(const v of entries)x.set(v.member,v.score);return entries.length;}
  async zRange(k,s,e,opts={by:'score'}){
    this.rangeCalls.push({key:k,start:s,end:e,options:opts});
    const rows=[...(this.z.get(k)||[]).entries()].map(([member,score])=>({member,score})).filter(x=>x.score>=s&&x.score<=e).sort((a,b)=>a.score-b.score||a.member.localeCompare(b.member));
    return opts.limit ? rows.slice(opts.limit.offset,opts.limit.offset+opts.limit.count) : rows;
  }
  async zRem(k,values){for(const v of values) this.z.get(k)?.delete(v);}
  async zScore(k,v){return this.z.get(k)?.get(v);}
  async zCard(k){return this.z.get(k)?.size||0;}
  async hIncrBy(k,f,n){let m=this.h.get(k);if(!m){m=new Map();this.h.set(k,m);}const v=(m.get(f)||0)+n;m.set(f,v);return v;}
  async watch(key){const db=this;let prev=await db.get(key);let committed=false;return {
    async get(){return this;},async multi(){},async del(k){this._del=k;},
    async exec(){if(await db.get(key)!==prev)return null;if(this._del)await db.del(this._del);committed=true;return ['OK'];},
    async unwatch(){},
  };}
}
const delay = ms=>new Promise(resolve=>setTimeout(resolve,ms));
function fixture(options={}){
 const db=new RedisFake();const items=new Map(); let clock=100000;
 const calls=[];let active=0,peak=0;
 const dep={db,now:()=>clock,config:async()=>({mode:'live',model:'gpt-6-luna',sampleRate:0,dailyLimit:500}),
  readItem:async id=>{const item=items.get(id);if(!item)throw new Error('not found');return item;},
  readPost:async id=>items.get(id)??{title:'يوم 1، مشروع تعلم البرمجة'},
  judge:async(input,model)=>{calls.push(input); active++;peak=Math.max(peak,active);await delay(options.latency??0);active--; if(options.judge)return options.judge(input,calls.length);
   return {decision:'allow',confidence:.99,reason_code:'ok',reason_ar:'مناسب'};}};
 return {db,items,dep,calls,advance(ms){clock+=ms;},get peak(){return peak},
   addComment(id,body='أحسنت واصل، بالتوفيق لك!'){const item={id,postId:'t3_parent',parentId:'t3_parent',body,approved:false,removed:false,spam:false,
     remove:async()=>{item.removed=true;item.removes=(item.removes||0)+1;},filter:async()=>{item.filters=(item.filters||0)+1;}};items.set(id,item);return item;},
   addPost(id,image='https://i.redd.it/img.jpg'){const item={id,title:'يوم 1، العمل على مشروع البرمجة',body:'اليوم 1: سأصمم واجهة المستخدم للمشروع وأراجع الصفحات وأجهز قائمة مهام مفصلة.',url:image,
     approved:false,removed:false,spam:false,remove:async()=>{item.removed=true;item.removes=(item.removes||0)+1;},filter:async()=>{item.filters=(item.filters||0)+1;}};items.set(id,item);return item;}
 };
}
test('queue tickets are unique even for duplicated events',async()=>{const d=new RedisFake();const a=await enqueue(d,'t1_abc',1);const b=await enqueue(d,'t1_abc',1);assert.notEqual(a,b);assert.equal((await dueTickets(d,1)).length,2);assert.equal(itemFromTicket(b),'t1_abc');});
test('atomic claim plus conditional release preserves current owner',async()=>{const d=new RedisFake();assert.equal(await claim(d,'lock','A'),true);assert.equal(await claim(d,'lock','B'),false);await releaseIfOwned(d,'lock','B');assert.equal(await d.get('lock'),'A');await releaseIfOwned(d,'lock','A');assert.equal(await d.get('lock'),null);});

test('lock release preserves an owner that changes after WATCH', async()=>{
  const db=new RedisFake();
  await db.set('lock','A');
  const watch=db.watch.bind(db);
  db.watch=async key=>{
    const txn=await watch(key);
    const multi=txn.multi.bind(txn);
    txn.multi=async()=>{await db.set(key,'B');await multi();};
    return txn;
  };
  await releaseIfOwned(db,'lock','A');
  assert.equal(await db.get('lock'),'B');
});
test('20 simultaneous events never exceed ten workers and all finish',async()=>{const f=fixture({latency:6}); const jobs=[];for(let i=0;i<20;i++){const id=`t1_c${i}`;f.addComment(id);jobs.push(await enqueue(f.db,id,100000));}
 await Promise.all(jobs.map(j=>runTicket(f.dep,j)));assert.ok(f.peak<=10);for(let i=0;i<8&&(await metrics(f.db)).pending;i++)await drain(f.dep,10);
 assert.equal((await metrics(f.db)).pending,0);assert.equal(f.calls.length,20);});
test('two events for same content use only one AI call',async()=>{const f=fixture({latency:3});f.addComment('t1_same');const a=await enqueue(f.db,'t1_same',100000),b=await enqueue(f.db,'t1_same',100000);
 await Promise.all([runTicket(f.dep,a),runTicket(f.dep,b)]);await drain(f.dep,10);assert.equal(f.calls.length,1);assert.equal((await metrics(f.db)).pending,0);});
test('missing image is auto removed without AI',async()=>{const f=fixture();const item=f.addPost('t3_bad','');const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);assert.equal(item.removes,1);assert.equal(f.calls.length,0);});
test('image sampling off never sends image bytes or URL',async()=>{const f=fixture();const item=f.addPost('t3_valid');const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);assert.equal(f.calls.length,1);assert.equal(f.calls[0].imageUrl,undefined);});
test('clear AI removal is automatic',async()=>{const f=fixture({judge:()=>({decision:'remove',confidence:.99,reason_code:'hostility',reason_ar:'إهانة واضحة'})});const item=f.addComment('t1_mean','أنت غبي');const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);assert.equal(item.removes,1);});
test('extremely uncertain low-confidence removal is filtered for human review',async()=>{const f=fixture({judge:()=>({decision:'remove',confidence:.42,reason_code:'other',reason_ar:'غير واضح'})});const item=f.addComment('t1_uncertain');const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);assert.equal(item.removes,undefined);assert.equal(item.filters,1);});
test('edited text invalidates stale removal and enqueues a fresh ticket',async()=>{const f=fixture({judge:(_input,n)=>{if(n===1){f.items.get('t1_edit').body='نقاش تقني محترم، ما رأيكم بهذا التغيير؟';return {decision:'remove',confidence:.99,reason_code:'hostility',reason_ar:'إهانة'};}
 return {decision:'allow',confidence:.99,reason_code:'ok',reason_ar:'نقاش'};}});const item=f.addComment('t1_edit','أنت غبي');const a=await enqueue(f.db,item.id,100000);await runTicket(f.dep,a);assert.equal(item.removes,undefined);assert.equal((await metrics(f.db)).pending,1);await drain(f.dep,5);assert.equal(item.removes,undefined);assert.equal(f.calls.length,2);});
test('manual moderator approval before AI returns prevents removal',async()=>{const f=fixture({judge:()=>{f.items.get('t1_approved').approved=true;return {decision:'remove',confidence:.99,reason_code:'hostility',reason_ar:'إهانة'};}});const item=f.addComment('t1_approved');const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);assert.equal(item.removes,undefined);});
test('temporary API failure keeps job for retry without deleting item',async()=>{const f=fixture({judge:(_i,n)=>{if(n===1)throw new Error('HTTP 429');return {decision:'allow',confidence:.9,reason_code:'ok',reason_ar:'مناسب'};}});const item=f.addComment('t1_retry');const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);assert.equal((await metrics(f.db)).pending,1);assert.equal((await dueTickets(f.db,100000)).length,0);f.advance(5000);await runTicket(f.dep,t);assert.equal((await metrics(f.db)).pending,0);assert.equal(f.calls.length,2);});
test('exhausted failures reach dead-letter and ModQueue fallback',async()=>{const f=fixture({judge:()=>{throw new Error('service outage')}});const item=f.addComment('t1_down');const t=await enqueue(f.db,item.id,100000);
 for(let n=0;n<MAX_ATTEMPTS;n++){await runTicket(f.dep,t);f.advance(200000);}
 assert.equal((await metrics(f.db)).pending,0);assert.equal((await metrics(f.db)).dead,1);assert.equal(item.filters,1);assert.equal(item.removes,undefined);});
test('budget limit zero sends human review without paid calls',async()=>{const f=fixture();f.dep.config=async()=>({mode:'live',model:'gpt-6-luna',sampleRate:0,dailyLimit:0});const item=f.addComment('t1_limit');const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);assert.equal(f.calls.length,0);assert.equal(item.filters,1);});

test('deletion removes pending jobs, verdict caches and dead-letter references',async()=>{
 const f=fixture();const item=f.addComment('t1_del');const t=await enqueue(f.db,item.id,100000);
 await runTicket(f.dep,t);assert.equal(f.calls.length,1);
 await enqueue(f.db,item.id,100000);await discardDeleted(f.dep,item.id);
 assert.equal((await metrics(f.db)).pending,0);
 const remaining=[...f.db.k.keys()].filter(k=>k.includes('t1_del')&&(k.includes('verdict')||k.includes('done')));
 assert.deepEqual(remaining,[]);
});
test('a transient Reddit moderation action error reuses stored verdict without re-paying AI',async()=>{
 const f=fixture({judge:()=>({decision:'remove',confidence:.99,reason_code:'hostility',reason_ar:'سيء'})});
 const item=f.addComment('t1_action');let tried=0;item.remove=async()=>{tried++;if(tried===1)throw new Error('Reddit temporarily unavailable');item.removed=true;};
 const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);f.advance(5000);await runTicket(f.dep,t);
 assert.equal(tried,2);assert.equal(f.calls.length,1);assert.equal((await metrics(f.db)).pending,0);
});

test('crossposts auto-remove without AI even when title and image look valid',async()=>{
 const f=fixture();const item=f.addPost('t3_crosspost');item.crosspostParentId='t3_original';
 const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);
 assert.equal(item.removes,1);assert.equal(f.calls.length,0);
 assert.equal((await metrics(f.db)).pending,0);
});
test('crossposts respect audit mode and are not removed',async()=>{
 const f=fixture();const item=f.addPost('t3_crossaudit');item.crosspostParentId='t3_original';
 const config=f.dep.config;f.dep.config=async()=>({...await config(),mode:'audit'});
 const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);
 assert.equal(item.removes,undefined);assert.equal(item.filters,undefined);assert.equal(f.calls.length,0);
});
test('crossposts respect moderator approval and are skipped',async()=>{
 const f=fixture();const item=f.addPost('t3_crossapproved');item.crosspostParentId='t3_original';item.approved=true;
 const t=await enqueue(f.db,item.id,100000);await runTicket(f.dep,t);
 assert.equal(item.removes,undefined);assert.equal(f.calls.length,0);
});

function captureLogs(t) {
  const events=[];
  for (const method of ['log','error']) t.mock.method(console,method,message=>events.push(JSON.parse(message)));
  return events;
}

test('recovery selects ten distinct oldest items and leaves duplicate and excess tickets queued',async(t)=>{
  captureLogs(t);
  const f=fixture();
  f.addComment('t1_duplicate');
  const oldest=await enqueue(f.db,'t1_duplicate',1);
  for(let i=0;i<30;i++) await enqueue(f.db,'t1_duplicate',2+i);
  for(let i=0;i<15;i++) {const id=`t1_distinct${i}`;f.addComment(id);await enqueue(f.db,id,40+i);}
  const summary=await drain(f.dep);
  assert.deepEqual(summary,{examined:46,attempted:10,processed:10,busy:0,notDue:0});
  assert.equal(f.calls.length,10);
  assert.equal(await f.db.zScore(Q.ready,oldest),undefined);
  assert.equal((await metrics(f.db)).pending,36);
  assert.deepEqual(f.db.rangeCalls[0].options,{by:'score',limit:{offset:0,count:100}});
});

test('recovery examines at most 100 candidates even if all belong to one item',async(t)=>{
  captureLogs(t);
  const f=fixture();f.addComment('t1_duplicate');f.addComment('t1_later');
  for(let i=0;i<105;i++) await enqueue(f.db,'t1_duplicate',1+i);
  const later=await enqueue(f.db,'t1_later',200);
  const summary=await drain(f.dep);
  assert.equal(summary.examined,100);assert.equal(summary.attempted,1);
  assert.equal(f.calls.length,1);assert.equal(await f.db.zScore(Q.ready,later),200);
  assert.equal((await metrics(f.db)).pending,105);
});

test('future retries are excluded and retain retry metadata',async(t)=>{
  captureLogs(t);
  const f=fixture();f.addComment('t1_future');
  const ticket=await enqueue(f.db,'t1_future',100000);
  assert.deepEqual(await retryOrDead(f.db,ticket,100000),{status:'retry',attempts:1,nextRetryAt:105000});
  assert.deepEqual(await drain(f.dep),{examined:0,attempted:0,processed:0,busy:0,notDue:0});
  assert.equal(await f.db.zScore(Q.ready,ticket),105000);
  f.advance(5000);assert.equal((await drain(f.dep)).processed,1);
});

test('recovery reports occupied slots without losing tickets',async(t)=>{
  captureLogs(t);
  const f=fixture();f.addComment('t1_waiting');
  const ticket=await enqueue(f.db,'t1_waiting',100000);
  for(let i=0;i<10;i++) await claim(f.db,`dg3:slot:${i}`,`occupied${i}`);
  assert.deepEqual(await drain(f.dep),{examined:1,attempted:1,processed:0,busy:1,notDue:0});
  assert.equal(await f.db.zScore(Q.ready,ticket),100000);assert.equal(f.calls.length,0);
});

test('recovery reports a ticket removed after candidate selection as not due',async(t)=>{
  captureLogs(t);
  const f=fixture();f.addComment('t1_raced');
  await enqueue(f.db,'t1_raced',100000);
  const range=f.db.zRange.bind(f.db);
  f.db.zRange=async(...args)=>{const rows=await range(...args);await f.db.zRem(Q.ready,rows.map(x=>x.member));return rows;};
  assert.deepEqual(await drain(f.dep),{examined:1,attempted:1,processed:0,busy:0,notDue:1});
});

test('recovery and incoming triggers share the same ten worker slots',async(t)=>{
  captureLogs(t);
  let release;
  const gate=new Promise(resolve=>{release=resolve;});
  const f=fixture();let active=0,peak=0,started=0;
  f.dep.judge=async()=>{started++;active++;peak=Math.max(peak,active);await gate;active--;return {decision:'allow',confidence:1,reason_code:'ok',reason_ar:'مناسب'};};
  const tickets=[];
  for(let i=0;i<20;i++){const id=`t1_shared${i}`;f.addComment(id);tickets.push(await enqueue(f.db,id,100000));}
  const recovery=drain(f.dep);
  const incoming=tickets.slice(10).map(ticket=>runTicket(f.dep,ticket));
  try {
    for(let i=0;i<100&&started<10;i++) await delay(1);
    assert.equal(started,10);assert.ok(peak<=10);
  } finally {release();}
  await Promise.all([recovery,...incoming]);
  for(let i=0;i<3&&(await metrics(f.db)).pending;i++) await drain(f.dep);
  assert.equal((await metrics(f.db)).pending,0);assert.equal(started,20);assert.ok(peak<=10);
});

for (const [decision,mode,expected] of [
  ['allow','live','none'],['remove','live','remove'],['review','live','filter'],
  ['remove','audit','none'],['remove','review','filter'],
]) test(`logs proposed and completed ${decision} decisions in ${mode}`,async(t)=>{
  const events=captureLogs(t);
  const f=fixture({judge:()=>({decision,confidence:.99,reason_code:decision==='allow'?'ok':'hostility',reason_ar:'سبب قصير'})});
  f.dep.config=async()=>({mode,model:'unchanged',sampleRate:0,dailyLimit:10});
  f.addComment('t1_logged');const ticket=await enqueue(f.db,'t1_logged',100000);
  await runTicket(f.dep,ticket);
  const proposed=events.find(x=>x.event==='dg3_decision');
  const completed=events.find(x=>x.event==='dg3_moderated');
  assert.equal(proposed.action,'none');assert.equal(proposed.proposedAction,expected);
  assert.equal(completed.action,expected);
  for(const event of [proposed,completed]) {
    assert.equal(event.ticket,ticket);assert.equal(event.id,'t1_logged');assert.equal(event.mode,mode);
    assert.equal(event.verdict,decision);assert.equal(event.confidence,.99);
    assert.equal(event.reason_ar,'سبب قصير');assert.equal(event.attempt,1);
  }
});

test('cached and moderator skips are explicit and do not claim a new action',async(t)=>{
  const events=captureLogs(t);const f=fixture();
  const item=f.addComment('t1_skips');
  await runTicket(f.dep,await enqueue(f.db,item.id,100000));
  await runTicket(f.dep,await enqueue(f.db,item.id,100000));
  item.approved=true;
  await runTicket(f.dep,await enqueue(f.db,item.id,100000));
  assert.deepEqual(events.filter(x=>x.event==='dg3_skipped').map(x=>[x.reason,x.action]),
    [['cached_completion','none'],['already_moderated','none']]);
  assert.equal(f.calls.length,1);
});

test('stale edits log discarded verdicts and moderator actions log skips',async(t)=>{
  const events=captureLogs(t);const f=fixture();
  const item=f.addComment('t1_changed');
  f.dep.judge=async()=>{item.body='نص جديد مختلف';return {decision:'remove',confidence:1,reason_code:'hostility',reason_ar:'سبب'};};
  await runTicket(f.dep,await enqueue(f.db,item.id,100000));
  assert.equal(events.find(x=>x.event==='dg3_superseded').action,'none');
  assert.equal(events.find(x=>x.event==='dg3_superseded').verdict,'remove');
  f.dep.judge=async()=>{item.approved=true;return {decision:'remove',confidence:1,reason_code:'hostility',reason_ar:'سبب'};};
  await drain(f.dep);
  assert.ok(events.some(x=>x.event==='dg3_skipped'&&x.reason==='moderated_before_action'));
  assert.equal(item.removes,undefined);
});

for(const outcome of ['filtered','failed','skipped_already_moderated'])
test(`audit failure safety fallback logs ${outcome} and exact retry metadata`,async(t)=>{
  const events=captureLogs(t);const f=fixture({judge:()=>{throw new Error('OpenAI HTTP 429');}});
  f.dep.config=async()=>({mode:'audit',model:'unchanged',sampleRate:0,dailyLimit:100});
  const item=f.addComment('t1_failure');
  if(outcome==='failed') item.filter=async()=>{throw new Error('provider response containing private content');};
  if(outcome==='skipped_already_moderated') {
    const read=f.dep.readItem;let reads=0;
    f.dep.readItem=async id=>{const result=await read(id);if(++reads===5) result.approved=true;return result;};
  }
  const ticket=await enqueue(f.db,item.id,100000);
  const times=[100000,105000,125000,185000];
  for(let i=0;i<4;i++){if(i) f.advance(times[i]-times[i-1]);await runTicket(f.dep,ticket);}
  const failures=events.filter(x=>x.event==='dg3_process_error');
  assert.deepEqual(failures.map(x=>x.attempts),[1,2,3,4]);
  assert.deepEqual(failures.map(x=>x.nextRetryAt),[105000,125000,185000,null]);
  assert.deepEqual(failures.map(x=>x.status),['retry','retry','retry','dead']);
  assert.ok(failures.every(x=>x.mode==='audit'&&x.error==='OpenAI HTTP 429'));
  assert.ok(events.some(x=>x.event==='dg3_dead_letter'&&x.ticket===ticket&&x.attempts===4));
  const fallback=events.find(x=>x.fallback===outcome);
  assert.ok(fallback);assert.equal(fallback.action,outcome==='filtered'?'filter':'none');
  assert.equal(item.removes,undefined);
});

test('failed moderation is not logged as completed; a successful action is logged before persistence',async(t)=>{
  const events=captureLogs(t);const f=fixture({judge:()=>({decision:'remove',confidence:1,reason_code:'hostility',reason_ar:'سبب'})});
  const item=f.addComment('t1_actionlog');item.remove=async()=>{throw new Error('outage');};
  const ticket=await enqueue(f.db,item.id,100000);await runTicket(f.dep,ticket);
  assert.ok(!events.some(x=>x.event==='dg3_moderated'));
  assert.equal(events.find(x=>x.event==='dg3_process_error').action,'none');
  item.remove=async()=>{item.removed=true;};
  const set=f.db.set.bind(f.db);
  f.db.set=async(key,...args)=>{if(key.startsWith('dg3:done:')) throw new Error('storage outage');return set(key,...args);};
  f.advance(5000);await runTicket(f.dep,ticket);
  assert.equal(events.find(x=>x.event==='dg3_moderated').action,'remove');
  assert.equal(events.filter(x=>x.event==='dg3_process_error').at(-1).action,'remove');
});

test('logs omit full input text, credentials and arbitrary provider error messages',async(t)=>{
  const events=captureLogs(t);const f=fixture();
  const text='PRIVATE_FULL_SUBMISSION_12345';const secret='sk-proj-PRIVATESECRET';
  const item=f.addComment('t1_private',text);
  f.dep.judge=async()=>({decision:'allow',confidence:1,reason_code:'ok',reason_ar:`${text} Bearer ${secret}`});
  await runTicket(f.dep,await enqueue(f.db,item.id,100000));
  item.body='changed private submission';
  f.dep.judge=async()=>{throw new Error(`Authorization: Bearer ${secret} ${item.body}`);};
  await runTicket(f.dep,await enqueue(f.db,item.id,100000));
  const serialized=JSON.stringify(events);
  for(const value of [text,secret,item.body,'Authorization:','Bearer ']) assert.ok(!serialized.includes(value));
  assert.equal(events.find(x=>x.event==='dg3_process_error').error,'Processing failed');
});

const mockDecision=(id='t3_history',now=100000)=>newRecord({
  key:recordKey(id,`${id}:${now}`),itemId:id,ticket:`${id}|1`,fingerprint:'fingerprint',mode:'live',
  source:'ai',model:'configured-model',verdict:{decision:'remove',confidence:.99,reason_code:'off_topic',reason_ar:'محتوى غير مرتبط بالهدف اليومي'},
  intendedAction:'remove',reason:'محتوى غير مرتبط بالهدف اليومي',
},now);
function noticeFixture(db=new RedisFake()) {
  const record=mockDecision();record.outcome='removed';record.notification='pending';record.note='pending';
  const replies=[];const notes=[];const distinctions=[];const state={approved:false,deleted:false,subreddit:'test_community'};
  const port={
    state:async()=>state,
    findReply:async(_id,marker)=>({replyId:replies.find(x=>x.text.includes(marker))?.id??null,complete:true}),
    reply:async(id,text)=>{const reply={id:`t1_notice${replies.length}`,parent:id,text};replies.push(reply);return reply.id;},
    distinguish:async(...args)=>{distinctions.push(args);},
    removalNote:async(...args)=>{notes.push(args);},
  };
  return {db,record,replies,notes,distinctions,state,port};
}

test('confirmed removals persist their verdict and enqueue feedback independently',async(t)=>{
  captureLogs(t);const f=fixture({judge:()=>({decision:'remove',confidence:.99,reason_code:'hostility',reason_ar:'إهانة شخصية'})});
  const item=f.addComment('t1_history');const ticket=await enqueue(f.db,item.id,100000);
  await runTicket(f.dep,ticket);
  const history=await loadRecord(f.db,recordKey(item.id,ticket),100000);
  assert.equal(history.outcome,'removed');assert.equal(history.source,'ai');assert.equal(history.reason,'إهانة شخصية');
  assert.equal(history.notification,'pending');assert.equal(history.note,'pending');
  assert.equal(await f.db.zCard(H.notices),1);assert.equal(item.removes,1);
  await discardDeleted(f.dep,item.id);
  assert.equal((await loadRecord(f.db,history.key,100000)).outcome,'removed');
  assert.equal(await f.db.zCard(H.notices),1,'moderator removal cache cleanup must retain notices');
});

test('failed or interrupted moderation is unresolved and cannot send a removal notice',async(t)=>{
  captureLogs(t);const f=fixture({judge:()=>({decision:'remove',confidence:.99,reason_code:'hostility',reason_ar:'سبب'})});
  const item=f.addComment('t1_unknown');item.remove=async()=>{throw new Error('transport failure');};
  const ticket=await enqueue(f.db,item.id,100000);await runTicket(f.dep,ticket);
  const record=await loadRecord(f.db,recordKey(item.id,ticket),100000);
  assert.equal(record.outcome,'unresolved');assert.equal(record.notification,'not_needed');
  const notices=noticeFixture(f.db);await deliverNotice(f.db,notices.port,record.key,100000);
  assert.equal(notices.replies.length,0);assert.equal(await f.db.zCard(H.notices),0);
  item.removed=true;f.advance(5000);await runTicket(f.dep,ticket);
  assert.equal((await loadRecord(f.db,record.key,105000)).outcome,'unresolved');
});

test('app comments are exempt before AI and never create notification loops',async(t)=>{
  captureLogs(t);const f=fixture();const item=f.addComment('t1_own');item.appAuthored=true;
  await runTicket(f.dep,await enqueue(f.db,item.id,100000));
  assert.equal(f.calls.length,0);assert.equal(item.removes,undefined);assert.equal(item.filters,undefined);
  assert.equal(await f.db.zCard(H.notices),0);
});

test('allowed, audit, skipped, and stale decisions do not enqueue public feedback',async(t)=>{
  captureLogs(t);
  for(const mode of ['live','audit']) {
    const f=fixture();f.dep.config=async()=>({mode,model:'same',sampleRate:0,dailyLimit:1});
    const item=f.addComment('t1_nonotice');const ticket=await enqueue(f.db,item.id,100000);
    await runTicket(f.dep,ticket);await runTicket(f.dep,await enqueue(f.db,item.id,100000));
    assert.equal(await f.db.zCard(H.notices),0);
    assert.equal((await loadRecord(f.db,recordKey(item.id,ticket),100000)).outcome,mode==='audit'?'audit':'allowed');
  }
});

test('audit safety fallback records pending review without a violation accusation',async(t)=>{
  captureLogs(t);const f=fixture({judge:()=>{throw new Error('outage');}});
  f.dep.config=async()=>({mode:'audit',model:'same',sampleRate:0,dailyLimit:100});
  const item=f.addComment('t1_fallbackhistory');const ticket=await enqueue(f.db,item.id,100000);
  for(let i=0;i<4;i++){await runTicket(f.dep,ticket);if(i<3) f.advance(200000);}
  const record=await loadRecord(f.db,recordKey(item.id,`${ticket}:fallback`),700000);
  assert.equal(record.source,'system_fallback');assert.equal(record.outcome,'reviewed');assert.equal(record.verdict,null);
  assert.match(explanation(record,'test'),/ليس حكمًا بمخالفة/);assert.equal(record.notification,'pending');
});

test('duplicate notice deliveries reuse the same distinguished reply and completed note',async(t)=>{
  captureLogs(t);const f=noticeFixture();await saveRecord(f.db,f.record);
  await Promise.all([deliverNotice(f.db,f.port,f.record.key,100000),deliverNotice(f.db,f.port,f.record.key,100000)]);
  await deliverNotice(f.db,f.port,f.record.key,160000);
  assert.equal(f.replies.length,1);assert.equal(f.notes.length,1);assert.deepEqual(f.distinctions,[[f.replies[0].id,true]]);
  const stored=await loadRecord(f.db,f.record.key,160000);
  assert.equal(stored.notification,'sent');assert.equal(stored.note,'sent');assert.equal(stored.replyId,f.replies[0].id);
  assert.match(f.replies[0].text,/Modmail/);assert.ok(f.replies[0].text.includes(noticeMarker(f.record)));
});

test('ambiguous sends find the existing app reply before retrying',async(t)=>{
  captureLogs(t);const f=noticeFixture();await saveRecord(f.db,f.record);
  const reply=f.port.reply;
  f.port.reply=async(...args)=>{await reply(...args);throw new Error('lost response after send');};
  await deliverNotice(f.db,f.port,f.record.key,100000);
  assert.equal((await loadRecord(f.db,f.record.key,100000)).sendStarted,true);
  await deliverNotice(f.db,f.port,f.record.key,160000);
  assert.equal(f.replies.length,1);assert.equal((await loadRecord(f.db,f.record.key,160000)).notification,'sent');
});

test('incomplete reply reconciliation never risks a second public reply',async(t)=>{
  captureLogs(t);const f=noticeFixture();await saveRecord(f.db,f.record);
  f.port.findReply=async()=>({replyId:null,complete:false});
  for(let i=0;i<4;i++) await deliverNotice(f.db,f.port,f.record.key,100000+i*60000);
  assert.equal(f.replies.length,0);assert.equal((await loadRecord(f.db,f.record.key,280000)).notification,'failed');
  assert.equal(await f.db.zCard(H.failed),1);assert.equal(await f.db.zCard(H.notices),0);
});

test('note failures retry independently without resending a successfully posted explanation',async(t)=>{
  captureLogs(t);const f=noticeFixture();await saveRecord(f.db,f.record);
  f.port.removalNote=async()=>{throw new Error('note unavailable');};
  for(let i=0;i<4;i++) await deliverNotice(f.db,f.port,f.record.key,100000+i*60000);
  const stored=await loadRecord(f.db,f.record.key,280000);
  assert.equal(f.replies.length,1);assert.equal(stored.notification,'sent');assert.equal(stored.note,'failed');
  assert.equal(stored.deliveryAttempts,4);assert.equal(await f.db.zCard(H.failed),1);
});

for(const cancellation of ['approved','deleted']) test(`${cancellation} items cancel pending feedback`,async(t)=>{
  captureLogs(t);const f=noticeFixture();await saveRecord(f.db,f.record);f.state[cancellation]=true;
  await deliverNotice(f.db,f.port,f.record.key,100000);
  assert.equal(f.replies.length,0);assert.equal(f.notes.length,0);
  assert.equal((await loadRecord(f.db,f.record.key,100000)).notification,'cancelled');
});

test('author deletion purges history and pending jobs and blocks a stale worker resurrecting them',async(t)=>{
  captureLogs(t);const f=noticeFixture();await saveRecord(f.db,f.record);
  await purgeItemHistory(f.db,f.record.itemId);
  assert.equal(await loadRecord(f.db,f.record.key,100000),null);assert.equal(await f.db.zCard(H.notices),0);
  assert.equal(await saveRecord(f.db,f.record),false);assert.equal(await f.db.zCard(H.index),0);
  await deliverNotice(f.db,f.port,f.record.key,100000);assert.equal(f.replies.length,0);
});

test('notification drain attempts no more than five due deliveries',async(t)=>{
  captureLogs(t);const f=noticeFixture();
  for(let i=0;i<7;i++) {const record=mockDecision(`t3_batch${i}`);record.outcome='reviewed';record.notification='pending';await saveRecord(f.db,record);}
  const future=mockDecision('t3_future_notice');future.outcome='reviewed';future.notification='pending';future.updatedAt=200000;
  await saveRecord(f.db,future);
  assert.equal(await drainNotices(f.db,f.port,100000),5);assert.equal(f.replies.length,5);
  assert.equal(await f.db.zCard(H.notices),3);
});

test('history is capped at 1000, expires after 14 days, and pagination never exposes full content',async(t)=>{
  captureLogs(t);const db=new RedisFake();
  for(let i=0;i<HISTORY_LIMIT+2;i++) {const record=mockDecision(`t3_cap${i}`,100000+i);record.outcome='allowed';await saveRecord(db,record);}
  assert.equal(await db.zCard(H.index),HISTORY_LIMIT);
  assert.equal(await loadRecord(db,mockDecision('t3_cap0',100000).key,102000),null);
  const first=await recentRecords(db,0,undefined,102000);const second=await recentRecords(db,1,undefined,102000);
  assert.equal(first.records.length,20);assert.equal(first.total,1000);
  assert.notEqual(first.records[0].key,second.records[0].key);
  assert.equal(first.records[0].itemId,'t3_cap1001');
  await maintainHistory(db,102001+HISTORY_TTL*1000);
  assert.equal(await db.zCard(H.index),0);assert.equal(await db.zCard(H.notices),0);
});

test('moderator authorization applies to every menu and form endpoint before accessing history',async()=>{
  const db=new RedisFake();let calls=0;
  const routes=moderatorRoutes({db,authorize:async()=>{calls++;return false;},now:()=>100000});
  for(const path of ['/decision','/activity','/activity-submit','/close']) {
    const response=await routes.request(path,{method:'POST',headers:{'content-type':'application/json'},body:'{}'});
    assert.equal(response.status,403);
  }
  assert.equal(calls,4);assert.equal(db.rangeCalls.length,0);
});

test('moderator views show stored decisions, safe failures, page navigation and missing history',async(t)=>{
  captureLogs(t);const db=new RedisFake();
  for(let i=0;i<23;i++) {const record=mockDecision(`t3_view${i}`,100000+i);record.outcome='removed';record.notification=i===22?'failed':'sent';await saveRecord(db,record);}
  const routes=moderatorRoutes({db,authorize:async()=>true,now:()=>101000});
  const request=async(path,body)=>{
    const response=await routes.request(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
    return {status:response.status,value:await response.json()};
  };
  const detail=await request('/decision',{targetId:'t3_view22'});
  assert.equal(detail.status,200);assert.match(JSON.stringify(detail.value),/configured-model/);
  assert.match(JSON.stringify(detail.value),/فشل/);
  const missing=await request('/decision',{targetId:'t3_unknown'});
  assert.match(JSON.stringify(missing.value),/No stored decision/);
  const activity=await request('/activity',{});
  const picker=activity.value.showForm.form.fields.find(x=>x.name==='decisionKey');
  assert.equal(picker.options.length,20);
  assert.ok(activity.value.showForm.form.fields.every(x=>x.type!=='paragraph'));
  assert.ok(activity.value.showForm.form.description.includes('اختر قرارًا'));
  const second=await request('/activity-submit',{page:0,navigation:['next']});
  assert.equal(second.value.showForm.form.fields.find(x=>x.name==='decisionKey').options.length,3);
  const lookup=await request('/activity-submit',{page:0,itemId:'t3_view2',navigation:['refresh']});
  assert.equal(lookup.value.showForm.form.fields.find(x=>x.name==='decisionKey').options.length,1);
  const opened=await request('/activity-submit',{navigation:['open:0'],decisionKey:[picker.options[0].value]});
  assert.equal(opened.value.showForm.name,'dgDecision');
  assert.match(opened.value.showForm.form.description,/configured-model/);
  assert.ok(opened.value.showForm.form.fields.every(x=>x.type!=='paragraph'));
  const back=await request('/close',{navigation:['back:1']});
  assert.equal(back.value.showForm.name,'dgActivity');
  assert.equal(back.value.showForm.form.fields.find(x=>x.name==='decisionKey').options.length,3);
  assert.equal((await request('/activity-submit',{navigation:['open:0'],decisionKey:['invalid']})).status,400);
  const expired=await request('/activity-submit',{navigation:['open:0'],decisionKey:['t3_gone:0123456789abcdef01234567']});
  assert.match(expired.value.showForm.form.description,/No stored decision/);
  assert.equal((await request('/activity-submit',{page:999,itemId:'bad'})).status,400);
  assert.equal((await request('/decision',{targetId:'malformed'})).status,400);
});

test('format explanations include a complete matching title and body with accurate repost guidance',()=>{
  const record=mockDecision();record.outcome='removed';record.verdict.reason_code='format';
  const text=explanation(record,'test');
  assert.match(text,/العنوان.*يوم 40/);assert.match(text,/المتن/);assert.match(text,/اليوم 40:/);
  assert.match(text,/هدفي اليوم إنهاء تصميم الصفحة الأولى/);assert.match(text,/استبدل رقم 40/);
  assert.match(text,/يمكنك نشر نسخة جديدة بعد التصحيح/);assert.match(text,/وحده لن يعيده تلقائيًا/);
  assert.ok(!text.includes('لا تنشر المحتوى نفسه مرارًا'));
});

test('missing-image explanations explicitly explain attaching an image to a corrected new post',()=>{
  const record=mockDecision();record.outcome='removed';record.verdict.reason_code='missing_image';
  const text=explanation(record,'test');
  assert.match(text,/أرفق صورة/);assert.match(text,/مع النسخة الجديدة من منشورك/);
  assert.match(text,/يمكنك نشر نسخة جديدة بعد التصحيح/);assert.match(text,/وحده لن يعيده تلقائيًا/);
});

test('pending review asks users to wait while other removals direct appeals without promising automatic restoration',()=>{
  const record=mockDecision();record.outcome='reviewed';record.verdict.reason_code='format';
  const pending=explanation(record,'test');
  assert.match(pending,/انتظر قرارهم ولا تعِد نشره/);assert.ok(!pending.includes('يمكنك نشر نسخة جديدة'));
  assert.ok(!pending.includes('مثال كامل'));
  record.outcome='removed';record.verdict.reason_code='hostility';
  const removed=explanation(record,'test');
  assert.match(removed,/لطلب مراجعة القرار أو استعادة المحتوى/);
  assert.ok(!removed.includes('يمكنك نشر نسخة جديدة'));
});

test('known moderator/admin removals retain history and unknown/author deletions purge it',()=>{
  for(const value of [2,3,'ADMIN','MODERATOR']) assert.equal(retainRemovalHistory(value),true);
  for(const value of [1,'USER',0,'UNKNOWN_EVENT_SOURCE',undefined,-1,'unexpected']) assert.equal(retainRemovalHistory(value),false);
});

test('a delivery interrupted on its fourth attempt cannot start a fifth send',async(t)=>{
  captureLogs(t);const f=noticeFixture();f.record.deliveryAttempts=4;f.record.sendStarted=true;
  await saveRecord(f.db,f.record);await deliverNotice(f.db,f.port,f.record.key,160000);
  const stored=await loadRecord(f.db,f.record.key,160000);
  assert.equal(f.replies.length,0);assert.equal(stored.notification,'failed');assert.equal(stored.deliveryAttempts,4);
});

test('a transient note failure recovers without repeating moderation or the public reply',async(t)=>{
  captureLogs(t);const f=fixture({judge:()=>({decision:'remove',confidence:1,reason_code:'hostility',reason_ar:'إهانة'})});
  const item=f.addComment('t1_noteonly');const ticket=await enqueue(f.db,item.id,100000);await runTicket(f.dep,ticket);
  const notice=noticeFixture(f.db);let attempts=0;
  notice.port.removalNote=async()=>{if(++attempts===1) throw new Error('temporary error');};
  const key=recordKey(item.id,ticket);
  await deliverNotice(f.db,notice.port,key,100000);await deliverNotice(f.db,notice.port,key,160000);
  const record=await loadRecord(f.db,key,160000);
  assert.equal(item.removes,1);assert.equal(f.calls.length,1);assert.equal(notice.replies.length,1);
  assert.equal(record.notification,'sent');assert.equal(record.note,'sent');
});

test('private text echoed in AI reasons is omitted from history, author replies and moderator forms',async(t)=>{
  captureLogs(t);const f=fixture();const privateText='PRIVATE_SUBMISSION_NOT_FOR_HISTORY';
  const item=f.addComment('t1_secretreason',privateText);
  f.dep.judge=async()=>({decision:'remove',confidence:1,reason_code:'hostility',reason_ar:`${privateText} Bearer sk-private-token`});
  const ticket=await enqueue(f.db,item.id,100000);await runTicket(f.dep,ticket);
  const record=await loadRecord(f.db,recordKey(item.id,ticket),100000);
  assert.ok(!JSON.stringify(record).includes(privateText));assert.ok(!JSON.stringify(record).includes('sk-private-token'));
  assert.ok(!explanation(record,'test').includes(privateText));
  const routes=moderatorRoutes({db:f.db,authorize:async()=>true,now:()=>100000});
  const response=await routes.request('/decision',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({targetId:item.id})});
  const text=await response.text();assert.ok(!text.includes(privateText));assert.ok(!text.includes('sk-private-token'));
});

test('history repairs a lost notice queue entry without queuing unresolved actions',async()=>{
  const db=new RedisFake();const record=mockDecision();record.outcome='removed';record.notification='pending';
  await saveRecord(db,record);await db.zRem(H.notices,[record.key]);
  const unresolved=mockDecision('t3_unresolved');await saveRecord(db,unresolved);
  await maintainHistory(db,100000);
  assert.equal(await db.zCard(H.notices),1);assert.equal(await db.zScore(H.notices,unresolved.key),undefined);
});

test('duplicate tickets for the same content decision share a public marker and reuse the reply',async(t)=>{
  captureLogs(t);const f=noticeFixture();
  const duplicate={...f.record,key:recordKey(f.record.itemId,'second-ticket'),ticket:`${f.record.itemId}|2`};
  assert.equal(noticeMarker(f.record),noticeMarker(duplicate));
  await saveRecord(f.db,f.record);await saveRecord(f.db,duplicate);
  await Promise.all([deliverNotice(f.db,f.port,f.record.key,100000),deliverNotice(f.db,f.port,duplicate.key,100000)]);
  await deliverNotice(f.db,f.port,duplicate.key,160000);
  assert.equal(f.replies.length,1);
  assert.equal((await loadRecord(f.db,duplicate.key,160000)).notification,'sent');
});
