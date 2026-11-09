import { createHash, randomUUID } from 'node:crypto';
import { claim, releaseIfOwned, type ItemId, type RedisPort } from './queue.ts';
import type { Mode, Verdict } from './policy.ts';

export const HISTORY_TTL = 14 * 86400;
export const HISTORY_LIMIT = 1000;
export const H = { index:'dg3:history', notices:'dg3:notices', failed:'dg3:notice-failed' } as const;
export type DeliveryStatus = 'not_needed' | 'pending' | 'sent' | 'failed' | 'cancelled';
export type DecisionRecord = {
  key: string; itemId: ItemId; ticket: string; fingerprint: string;
  createdAt: number; updatedAt: number; mode: Mode | null;
  source: 'format' | 'ai' | 'cache' | 'system_fallback' | 'system';
  model: string | null; verdict: Verdict | null;
  intendedAction: 'none' | 'remove' | 'filter';
  outcome: 'unresolved' | 'removed' | 'reviewed' | 'allowed' | 'audit' | 'skipped' | 'superseded' | 'failed';
  reason: string; error: string | null;
  notification: DeliveryStatus; note: DeliveryStatus;
  deliveryAttempts: number; replyId: string | null; sendStarted: boolean;
};
export const recordKey = (id: ItemId, identity: string) => `${id}:${createHash('sha256').update(identity).digest('hex').slice(0,24)}`;
const dataKey = (key: string) => `dg3:decision:${key}`;
const itemKey = (id: ItemId) => `dg3:history-item:${id}`;
export const deletedKey = (id: ItemId) => `dg3:author-deleted:${id}`;
/** Explicit moderator/admin removals retain history; author/unknown deletion sources purge it. */
export const retainRemovalHistory = (source:unknown):boolean => source===2||source===3||source==='ADMIN'||source==='MODERATOR';

async function locked<T>(db: RedisPort, fn: () => Promise<T>): Promise<T> {
  const owner=randomUUID();
  for(let i=0;i<30;i++) {
    if(await claim(db,'dg3:history-lock',owner)) {
      try {return await fn();} finally {await releaseIfOwned(db,'dg3:history-lock',owner);}
    }
    await new Promise(resolve=>setTimeout(resolve,50));
  }
  throw new Error('Decision history busy');
}

async function drop(db: RedisPort, key: string): Promise<void> {
  const id=key.split(':')[0] as ItemId;
  await db.del(dataKey(key));
  await db.zRem(H.index,[key]);
  await db.zRem(itemKey(id),[key]);
  await db.zRem(H.notices,[key]);
  await db.zRem(H.failed,[key]);
}

export async function loadRecord(db: RedisPort, key: string, now=Date.now()): Promise<DecisionRecord | null> {
  const raw=await db.get(dataKey(key));
  if(!raw) return null;
  const record=JSON.parse(raw) as DecisionRecord;
  return record.createdAt > now-HISTORY_TTL*1000 ? record : null;
}

async function trim(db: RedisPort, now: number): Promise<void> {
  const expired=await db.zRange(H.index,0,now-HISTORY_TTL*1000,{by:'score',limit:{offset:0,count:HISTORY_LIMIT+1}});
  for(const {member} of expired) await drop(db,member);
  const excess=(await db.zCard(H.index))-HISTORY_LIMIT;
  if(excess>0) {
    const oldest=await db.zRange(H.index,0,Number.MAX_SAFE_INTEGER,{by:'score',limit:{offset:0,count:excess}});
    for(const {member} of oldest) await drop(db,member);
  }
}

/** All writes and retention changes share a lock, including deletion tombstones. */
export async function saveRecord(db: RedisPort, record: DecisionRecord): Promise<boolean> {
  return locked(db,async()=>{
    if(await db.get(deletedKey(record.itemId))) return false;
    if(record.updatedAt-record.createdAt>=HISTORY_TTL*1000) {await drop(db,record.key);return false;}
    await db.set(dataKey(record.key),JSON.stringify(record),{expiration:new Date(Date.now()+record.createdAt+HISTORY_TTL*1000-record.updatedAt)});
    await db.zAdd(H.index,{member:record.key,score:record.createdAt});
    await db.zAdd(itemKey(record.itemId),{member:record.key,score:record.createdAt});
    await db.expire(itemKey(record.itemId),HISTORY_TTL);
    if(record.notification==='pending'||record.note==='pending') {
      // Persist before queueing: a failed queue write is repaired by history maintenance.
      if(await db.zScore(H.notices,record.key)==null) await db.zAdd(H.notices,{member:record.key,score:record.updatedAt});
    } else await db.zRem(H.notices,[record.key]);
    if(record.notification==='failed'||record.note==='failed') await db.zAdd(H.failed,{member:record.key,score:record.updatedAt});
    else await db.zRem(H.failed,[record.key]);
    await trim(db,record.updatedAt);
    return true;
  });
}

export async function maintainHistory(db: RedisPort, now=Date.now()): Promise<void> {
  await locked(db,()=>trim(db,now));
  // Repair unfinished indexing after a partial Redis write without enabling unresolved actions.
  const rows=await db.zRange(H.index,now-HISTORY_TTL*1000,now,{by:'score',limit:{offset:0,count:HISTORY_LIMIT}});
  const page=await db.incrBy('dg3:history-repair-page',1);
  await db.expire('dg3:history-repair-page',HISTORY_TTL);
  const offset=((page-1)%Math.max(1,Math.ceil(rows.length/20)))*20;
  for(const {member} of rows.slice(offset,offset+20)) {
    const record=await loadRecord(db,member,now);
    if(record&&(record.notification==='pending'||record.note==='pending')&&await db.zScore(H.notices,member)==null) {
      await db.zAdd(H.notices,{member,score:now});
    }
  }
}

export async function purgeItemHistory(db: RedisPort, id: ItemId): Promise<void> {
  await locked(db,async()=>{
    await db.set(deletedKey(id),'1',{expiration:new Date(Date.now()+HISTORY_TTL*1000)});
    const rows=await db.zRange(itemKey(id),0,Number.MAX_SAFE_INTEGER,{by:'score',limit:{offset:0,count:HISTORY_LIMIT}});
    for(const {member} of rows) await drop(db,member);
    await db.del(itemKey(id));
  });
}

export async function recentRecords(db: RedisPort, page=0, id?: ItemId, now=Date.now()): Promise<{records:DecisionRecord[];total:number}> {
  await maintainHistory(db,now);
  const index=id?itemKey(id):H.index;
  const rows=await db.zRange(index,now-HISTORY_TTL*1000,now,{by:'score',limit:{offset:0,count:HISTORY_LIMIT}});
  const records: DecisionRecord[]=[];
  for(const {member} of rows.reverse().slice(page*20,page*20+20)) {
    const record=await loadRecord(db,member,now);
    if(record) records.push(record);
  }
  return {records,total:rows.length};
}

export function newRecord(input: Pick<DecisionRecord,'key'|'itemId'|'ticket'|'fingerprint'|'mode'|'source'|'model'|'verdict'|'intendedAction'|'reason'>, now: number): DecisionRecord {
  return {...input,createdAt:now,updatedAt:now,outcome:'unresolved',error:null,
    notification:'not_needed',note:'not_needed',deliveryAttempts:0,replyId:null,sendStarted:false};
}
