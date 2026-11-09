/** Testable moderation engine. Devvit adapters live in index.ts. */
import { createHash, randomUUID } from 'node:crypto';
import { claim, complete, dueTickets, enqueue, getSlot, itemFromTicket, metrics, Q,
  releaseIfOwned, retryOrDead, ticketIsDue, WORKER_SLOTS, RECOVERY_CANDIDATES, type ItemId, type RedisPort } from './queue.ts';
import { checkPostFormat, commentTarget, enforcement, extractImageUrls, shouldSampleImage,
  type Mode, type Verdict } from './policy.ts';
import type { ReviewInput } from './ai.ts';
import { safeError, safeReason } from './logging.ts';
import { loadRecord, newRecord, recordKey, saveRecord, type DecisionRecord } from './history.ts';

export const POLICY_VERSION = 'daiboon-v3.1-2026-10-28';
export type ModItem = {
  id: ItemId;
  appAuthored?: boolean;
  approved?: boolean;
  removed?: boolean;
  spam?: boolean;
  title?: string;
  body?: string;
  bodyHtml?: string;
  url?: string;
  crosspostParentId?: string;
  gallery?: Array<{ url: string; status?: string | number }>;
  parentId?: string;
  postId?: string;
  remove(isSpam?: boolean): Promise<void>;
  filter(options?: {reason?: string}): Promise<void>;
};
export type EngineConfig = { mode: Mode; model: string; sampleRate: number; dailyLimit: number };
export type Deps = {
  db: RedisPort;
  config(): Promise<EngineConfig>;
  readItem(id: ItemId): Promise<ModItem>;
  readPost(id: string): Promise<ModItem>;
  judge(input: ReviewInput, model: string): Promise<Verdict>;
  now?(): number;
};

const hash = (s: string) => createHash('sha256').update(s).digest('hex');
async function cacheKeyForDeletion(db: RedisPort, id: ItemId, key: string, now: number) {
  const idx = `dg3:cache-index:${id}`;
  await db.zAdd(idx,{member:key,score:now});
  await db.expire(idx,86400*16);
}
const nowFor = (deps: Deps) => deps.now?.() ?? Date.now();
const alreadyModerated = (item: ModItem) => Boolean(item.removed || item.spam || item.approved);

type Snapshot = { item: ModItem; fingerprint: string; input?: ReviewInput; immediate?: Verdict };

async function snapshot(deps: Deps, id: ItemId, config: EngineConfig): Promise<Snapshot> {
  const item = await deps.readItem(id);
  if (item.appAuthored || alreadyModerated(item)) return { item, fingerprint: 'manual-action' };
  if (id.startsWith('t3_')) {
    const title = item.title ?? '';
    const body = item.body ?? '';
    // Crossposts have an explicit Reddit parent ID. Never spend an AI call on them.
    // Include this attribute in the fingerprint so a later edit/event cannot apply
    // a decision from a snapshot without (or with) a crosspost parent ID.
    const crosspost = Boolean(item.crosspostParentId?.trim());
    if (crosspost) {
      return {item, fingerprint:hash(JSON.stringify([POLICY_VERSION, config.mode, 'crosspost', item.crosspostParentId])),
        immediate:{decision:'remove',confidence:1,reason_code:'crosspost',
          reason_ar:'لا يُسمح بإعادة نشر منشورات المجتمعات الأخرى؛ اكتب تحديثك اليومي الأصلي.'}};
    }
    const images = extractImageUrls({body, ...(item.url === undefined ? {} : {url:item.url}),
      ...(item.bodyHtml === undefined ? {} : {bodyHtml:item.bodyHtml}),
      ...(item.gallery === undefined ? {} : {gallery:item.gallery})});
    const format = checkPostFormat(title, body, images);
    const sampled = format.ok && shouldSampleImage(id, config.sampleRate);
    const fingerprint = hash(JSON.stringify([POLICY_VERSION, config.mode, config.model, config.sampleRate, title, body, images, sampled, item.crosspostParentId ?? null]));
    if (!format.ok) return {item, fingerprint, immediate: {decision:'remove',confidence:1,
      reason_code:format.error ?? 'format', reason_ar:format.detail ?? 'المنشور لا يتبع القالب.'}};
    return {item, fingerprint, input: {target:'post',postTitle:title,postBody:body,content:title,
      ...(sampled && images[0] ? {imageUrl:images[0]} : {})}};
  }
  if (!item.parentId || !item.postId) throw new Error('Missing comment parent/post ID');
  const target = commentTarget(item.parentId);
  const post = await deps.readPost(item.postId);
  let parentText: string | undefined;
  if (target === 'reply') {
    // A missing parent should not turn a constructive reply into a violation.
    try { parentText = (await deps.readItem(item.parentId as ItemId)).body; }
    catch { parentText = undefined; }
  }
  const body = item.body ?? '';
  const fingerprint = hash(JSON.stringify([POLICY_VERSION,config.mode,config.model,target,body,item.parentId,post.title,parentText]));
  return {item,fingerprint,input:{target,postTitle:post.title??'',content:body,
    ...(parentText === undefined ? {} : {parentComment:parentText})}};
}

async function aiCallAllowed(deps: Deps, max: number): Promise<boolean> {
  if (max <= 0) return false;
  // One atomic increment per attempted paid request. Requests above the cap never run.
  // This caps request attempts, NOT dollars/tokens, and overshoots the counter only.
  const key = `dg3:budget:${new Date(nowFor(deps)).toISOString().slice(0, 10)}`;
  const total = await deps.db.incrBy(key, 1);
  await deps.db.expire(key, 86400*3);
  return total <= max;
}

async function computeVerdict(deps: Deps, snap: Snapshot, config: EngineConfig): Promise<{verdict:Verdict;source:DecisionRecord['source']}> {
  if (snap.immediate) return {verdict:snap.immediate,source:'format'};
  if (!snap.input) throw new Error('Missing snapshot input');
  const cacheKey = `dg3:verdict:${snap.item.id}:${snap.fingerprint}`;
  const cached = await deps.db.get(cacheKey);
  if (cached) return {verdict:JSON.parse(cached) as Verdict,source:'cache'};
  if (!await aiCallAllowed(deps, config.dailyLimit)) {
    return {verdict:{decision:'review',confidence:0,reason_code:'other',reason_ar:'تم تجاوز الحد اليومي لفحوصات الذكاء الاصطناعي.'},source:'system_fallback'};
  }
  const v = await deps.judge(snap.input, config.model);
  // Cache before moderation: if Reddit remove() temporarily fails, retries needn't rebill.
  await deps.db.set(cacheKey, JSON.stringify(v));
  await cacheKeyForDeletion(deps.db,snap.item.id,cacheKey,nowFor(deps));
  await deps.db.expire(cacheKey, 86400*14);
  return {verdict:v,source:'ai'};
}

export type RunResult = 'processed'|'busy'|'not-due';
export async function runTicket(deps: Deps, ticket: string): Promise<RunResult> {
  const db = deps.db;
  const id = itemFromTicket(ticket);
  if (!await ticketIsDue(db,ticket,nowFor(deps))) return 'not-due';
  const owner = randomUUID();
  const slot = await getSlot(db,owner);
  if (!slot) return 'busy';
  const lockKey = `dg3:lock:${id}`;
  let entityLock = false;
  let mode: Mode | null = null;
  let verdict: Verdict | undefined;
  let proposedAction: 'none' | 'remove' | 'filter' | null = null;
  let actualAction: 'none' | 'remove' | 'filter' = 'none';
  let attempt: number | null = null;
  let inputs: Array<string | undefined> = [];
  let record: DecisionRecord | null = null;
  let model: string | null = null;
  let fingerprint = '';
  const historyKey=recordKey(id,ticket);
  const persistEvent=async(outcome:DecisionRecord['outcome'],reason:string,source:DecisionRecord['source']='system')=>{
    record=record??newRecord({key:historyKey,itemId:id,ticket,fingerprint,mode,source,model,
      verdict:verdict?{...verdict,reason_ar:safeReason(verdict.reason_ar,inputs)}:null,
      intendedAction:proposedAction??'none',reason:safeReason(reason,inputs)},nowFor(deps));
    Object.assign(record,{updatedAt:nowFor(deps),outcome,fingerprint,mode,source,
      model:source==='format'?null:model,verdict:verdict?{...verdict,reason_ar:safeReason(verdict.reason_ar,inputs)}:null,
      intendedAction:proposedAction??'none',reason:safeReason(verdict?.reason_ar??reason,inputs)});
    return saveRecord(db,record);
  };
  const decisionFields = () => ({ticket, id, mode, attempt,
    verdict: verdict?.decision ?? null, confidence: verdict?.confidence ?? null,
    code: verdict?.reason_code ?? null,
    reason_ar: verdict ? safeReason(verdict.reason_ar, inputs) : null,
    proposedAction, action: actualAction});
  const skipped = async (reason: string) => {
    const previous=await loadRecord(db,historyKey,nowFor(deps));
    if(record&&verdict) await persistEvent('skipped',reason,record.source);
    else if(previous?.outcome==='unresolved') {
      previous.error='Action result unconfirmed; item now moderated';
      previous.updatedAt=nowFor(deps);await saveRecord(db,previous);
    } else if(!previous||previous.outcome==='failed') await persistEvent('skipped',reason);
    await complete(db,ticket,'skipped');
    console.log(JSON.stringify({event:'dg3_skipped',...decisionFields(),reason}));
  };
  try {
    entityLock = await claim(db, lockKey, owner);
    if (!entityLock) return 'busy';
    if (!await ticketIsDue(db,ticket,nowFor(deps))) return 'not-due';
    try {
      record=await loadRecord(db,historyKey,nowFor(deps));
      attempt = Number(await db.get(`dg3:attempt:${ticket}`) ?? 0) + 1;
      const config = await deps.config();
      mode = config.mode;
      model=config.model;
      const first = await snapshot(deps, id, config);
      fingerprint=first.fingerprint;
      inputs = [first.item.title, first.item.body, first.input?.postTitle, first.input?.postBody,
        first.input?.content, first.input?.parentComment];
      if (first.item.appAuthored) {await complete(db,ticket,'skipped');return 'processed';}
      if (alreadyModerated(first.item)) {await skipped('already_moderated');return 'processed';}
      const processedKey = `dg3:done:${id}:${first.fingerprint}`;
      if (await db.get(processedKey)) {await skipped('cached_completion');return 'processed';}
      const computed=await computeVerdict(deps,first,config);
      verdict = computed.verdict;
      proposedAction = enforcement(verdict,config.mode);
      if(!await persistEvent('unresolved',verdict.reason_ar,computed.source)) {await complete(db,ticket,'skipped');return 'processed';}
      console.log(JSON.stringify({event:'dg3_decision',...decisionFields()}));
      // Re-fetch latest content before irreversible action. Drop stale decisions.
      const latest = await snapshot(deps,id,config);
      if (alreadyModerated(latest.item)) {await skipped('moderated_before_action');return 'processed';}
      if (first.fingerprint !== latest.fingerprint) {
        // Unique followup ticket avoids zRem from erasing the newer edit.
        await enqueue(db,id,nowFor(deps));
        await complete(db,ticket,'superseded');
        await persistEvent('superseded','content_changed');
        console.log(JSON.stringify({event:'dg3_superseded',...decisionFields(),reason:'content_changed'}));
        return 'processed';
      }
      // Guard against a lock that was replaced while waiting for OpenAI.
      if (await db.get(lockKey) !== owner || await db.get(slot) !== owner) {
        throw new Error('Lease expired before moderation action');
      }
      const action = proposedAction;
      if (action === 'remove') await latest.item.remove(false);
      if (action === 'filter') await latest.item.filter({reason:`دائبون: ${verdict.reason_ar.slice(0,130)}`});
      actualAction = action;
      if(record) {
        record.outcome=action==='remove'?'removed':action==='filter'?'reviewed':config.mode==='audit'?'audit':'allowed';
        record.updatedAt=nowFor(deps);
        record.notification=action==='none'?'not_needed':'pending';
        record.note=action==='remove'?'pending':'not_needed';
        await saveRecord(db,record);
      }
      // Record the completed Reddit action even if subsequent queue persistence fails.
      console.log(JSON.stringify({event:'dg3_moderated',...decisionFields()}));
      // Final action is idempotent; failed Redis persistence leaves the ticket for recovery.
      await db.set(processedKey,action);
      await cacheKeyForDeletion(db,id,processedKey,nowFor(deps));
      await db.expire(processedKey,86400*14);
      await complete(db,ticket,action==='remove'?'removed':action==='filter'?'reviewed':'allowed');
    } catch (error) {
      const message = safeError(error);
      try {
        if(!record) await persistEvent('failed',message);
        if(record) {record.error=message;record.updatedAt=nowFor(deps);await saveRecord(db,record);}
      } catch(historyError) {
        console.error(JSON.stringify({event:'dg3_history_error',id,ticket,error:safeError(historyError)}));
      }
      let retry;
      try {
        retry = await retryOrDead(db,ticket,nowFor(deps));
      } catch (persistenceError) {
        console.error(JSON.stringify({event:'dg3_process_error',...decisionFields(),error:message,
          status:'retry_persistence_failed', nextRetryAt:null, fallback:'not_attempted',
          persistenceError:safeError(persistenceError)}));
        throw persistenceError;
      }
      const retryFields = {...decisionFields(),...retry,error:message};
      console.error(JSON.stringify({event:'dg3_process_error',...retryFields,
        fallback:retry.status === 'dead' ? 'pending' : 'not_attempted'}));
      if (retry.status === 'dead') {
        console.error(JSON.stringify({event:'dg3_dead_letter',...retryFields,fallback:'pending'}));
        // We cannot safely auto-remove when Reddit/OpenAI is inaccessible.
        // Attempt to put the current item into human ModQueue; otherwise dead-letter + logs persist.
        try {
          const current = await deps.readItem(id);
          if (alreadyModerated(current)) {
            console.log(JSON.stringify({event:'dg3_review_fallback',...retryFields,fallback:'skipped_already_moderated'}));
          } else {
            // A separate fallback record cannot overwrite a possibly completed removal.
            record=newRecord({key:recordKey(id,`${ticket}:fallback`),itemId:id,ticket,fingerprint,mode,
              source:'system_fallback',model:null,verdict:null,intendedAction:'filter',
              reason:'تعذر إكمال الفحص الآلي بعد محاولات متكررة.'},nowFor(deps));
            if(!await saveRecord(db,record)) return 'processed';
            await current.filter({reason:'دائبون: تعذر إكمال الفحص الآلي بعد محاولات متكررة.'});
            record.outcome='reviewed';record.notification='pending';record.updatedAt=nowFor(deps);
            await saveRecord(db,record);
            console.log(JSON.stringify({event:'dg3_review_fallback',...retryFields,action:'filter',fallback:'filtered'}));
          }
        } catch (reviewError) {
          console.error(JSON.stringify({event:'dg3_review_unavailable',...retryFields,
            error:safeError(reviewError),fallback:'failed'}));
        }
      }
    }
    return 'processed';
  } finally {
    // conditional release; both locks also expire after 65s if the process dies.
    if (entityLock) {try {await releaseIfOwned(db,lockKey,owner);} catch(e){console.error(JSON.stringify({event:'dg3_lock_release_error',ticket,id,lock:'entity',error:safeError(e)}));}}
    try {await releaseIfOwned(db,slot,owner);} catch(e){console.error(JSON.stringify({event:'dg3_lock_release_error',ticket,id,lock:'slot',error:safeError(e)}));}
  }
}

export type RecoveryResult = { examined: number; attempted: number; processed: number; busy: number; notDue: number };
export async function drain(deps: Deps, max = WORKER_SLOTS): Promise<RecoveryResult> {
  const count = Number.isFinite(max) ? Math.min(WORKER_SLOTS,Math.max(0,Math.floor(max))) : 0;
  if (!count) return {examined:0,attempted:0,processed:0,busy:0,notDue:0};
  const tickets = await dueTickets(deps.db,nowFor(deps),RECOVERY_CANDIDATES);
  const seen = new Set<ItemId>();
  const selected: string[] = [];
  for (const ticket of tickets) {
    const id = itemFromTicket(ticket);
    if (seen.has(id)) continue;
    seen.add(id);
    selected.push(ticket);
    if (selected.length === count) break;
  }
  const results = await Promise.all(selected.map(ticket=>runTicket(deps,ticket)));
  return {examined:tickets.length,attempted:selected.length,
    processed:results.filter(x=>x==='processed').length,
    busy:results.filter(x=>x==='busy').length,notDue:results.filter(x=>x==='not-due').length};
}

/** Exposed for scheduler logs; no untrusted HTTP status endpoint. */
export async function queueHealth(deps: Deps) { return metrics(deps.db); }

/** When someone deletes their content, drop pending IDs and forget cached judgments.
 * Cached verdict keys expire automatically; they contain no original post text.
 */
export async function discardDeleted(deps: Deps, id: ItemId): Promise<void> {
  const db=deps.db;
  const ticketsKey=`dg3:tickets:${id}`;
  const cacheIndexKey=`dg3:cache-index:${id}`;
  const tickets=await db.zRange(ticketsKey,0,Number.MAX_SAFE_INTEGER,{by:'score'});
  const cacheKeys=await db.zRange(cacheIndexKey,0,Number.MAX_SAFE_INTEGER,{by:'score'});
  for (const {member} of tickets) {
    await db.zRem(Q.ready,[member]);
    await db.zRem(Q.dead,[member]);
    await db.del(`dg3:attempt:${member}`);
  }
  for (const {member} of cacheKeys) await db.del(member);
  await db.del(ticketsKey);
  await db.del(cacheIndexKey);
  await db.del(`dg3:reconciled:${id}`);
  console.log(JSON.stringify({event:'dg3_deleted',id,tickets:tickets.length,cacheKeys:cacheKeys.length}));
}
