import { Hono, type Context } from 'hono';
import { serve } from '@hono/node-server';
import { createServer, getServerPort, reddit, settings } from '@devvit/web/server';
import { redis } from '@devvit/redis';
import { reviewWithOpenAI, DEFAULT_MODEL } from './ai';
import { drain, discardDeleted, queueHealth, runTicket, type Deps } from './engine';
import { enqueue } from './queue';
import type { ItemId, RedisPort } from './queue';
import type { Mode } from './policy';
import { safeError } from './logging';
import { maintainHistory, purgeItemHistory, retainRemovalHistory, H } from './history';
import { drainNotices } from './notifications';
import { moderatorRoutes } from './moderator';
import { readModerationItem, notificationPort, authorizeModerator } from './reddit-adapters';

const app = new Hono();
const db: RedisPort = redis;
const engine: Deps = {
  db,
  async config() {
    const [modeRaw,modelRaw,sampleRaw,limitRaw] = await Promise.all([
      settings.get('moderationMode'),settings.get('openaiModel'),
      settings.get('imageSamplingRate'),settings.get('dailyAiChecksLimit'),
    ]);
    // Saved select settings are arrays; manifest defaults can be plain strings.
    const selectedMode = Array.isArray(modeRaw) ? modeRaw[0] : modeRaw;
    const mode: Mode = selectedMode === 'audit' || selectedMode === 'review' ? selectedMode : 'live';
    const sampleRate = Number(sampleRaw ?? 0);
    const dailyLimit = Number(limitRaw ?? 2000);
    return {
      mode, model: String(modelRaw || DEFAULT_MODEL),
      sampleRate: Number.isFinite(sampleRate)?Math.min(100,Math.max(0,sampleRate)):0,
      dailyLimit: Number.isFinite(dailyLimit)?Math.max(0,Math.floor(dailyLimit)):2000,
    };
  },
  readItem:readModerationItem,
  async readPost(id: string) {return readModerationItem(id as `t3_${string}`);},
  async judge(input,model) {
    const secret = String(await settings.get('openaiKey') ?? '');
    return reviewWithOpenAI(input,secret,model);
  },
};
app.route('/internal/mod',moderatorRoutes({db,authorize:authorizeModerator}));

/** Fail closed on persistence: return HTTP error so Reddit can retry trigger delivery. */
async function onSubmission(c: Context, kind: 'post'|'comment') {
  try {
    const payload = await c.req.json<{ post?: { id?: string }; comment?: { id?: string } }>();
    const id = kind==='post' ? payload.post?.id : payload.comment?.id;
    if (typeof id !== 'string' || !id.startsWith(kind==='post'?'t3_':'t1_')) {
      // Missing event payload is not a moderation decision; record it for monitoring.
      console.error(JSON.stringify({event:'dg3_missing_trigger_id',kind}));
      return c.json({status:'missing_id'},422);
    }
    const ticket = await enqueue(db,id as ItemId);
    // Fast path moderates right away. A full 10-worker pool leaves the ticket for the 1m recovery cron.
    await runTicket(engine,ticket);
    return c.json({status:'ok'},200);
  } catch(e) {
    console.error(JSON.stringify({event:'dg3_trigger_error',kind,error:safeError(e)}));
    return c.json({status:'retry'},503);
  }
}
app.post('/internal/post-create',c=>onSubmission(c,'post'));
app.post('/internal/post-update',c=>onSubmission(c,'post'));
app.post('/internal/comment-create',c=>onSubmission(c,'comment'));
app.post('/internal/comment-update',c=>onSubmission(c,'comment'));

async function onDelete(c: Context,kind:'post'|'comment') {
  try {
    const payload = await c.req.json<{ postId?: string; commentId?: string; source?:unknown }>();
    const id = kind==='post'?payload.postId:payload.commentId;
    if (typeof id==='string'&&/^t[13]_[a-zA-Z0-9]+$/.test(id)&&id.startsWith(kind==='post'?'t3_':'t1_')) {
      if(!retainRemovalHistory(payload.source)) await purgeItemHistory(db,id as ItemId);
      await discardDeleted(engine,id as ItemId);
    }
    return c.json({status:'ok'});
  } catch(e){console.error(JSON.stringify({event:'dg3_delete_error',kind,error:safeError(e)}));return c.json({status:'retry'},503);}
}
app.post('/internal/post-delete',c=>onDelete(c,'post'));
app.post('/internal/comment-delete',c=>onDelete(c,'comment'));

/** Best-effort reconciliation for missed Reddit triggers. Bounded to 40 recent unmoderated items. */
app.post('/internal/reconcile',async(c)=>{
  try {
    const subreddit = await reddit.getCurrentSubreddit();
    const recent = await subreddit.getUnmoderated().get(40);
    let added = 0;
    for (const item of recent) {
      if (typeof item.id !== 'string' || !/^t[13]_[a-zA-Z0-9]+$/.test(item.id)) continue;
      const mark = `dg3:reconciled:${item.id}`;
      if (await db.get(mark)) continue;
      await enqueue(db,item.id as ItemId);
      // Mark only after persisting the ticket. Re-evaluate after TTL for missed edits.
      await db.set(mark,'1');
      await db.expire(mark,600);
      added++;
    }
    console.log(JSON.stringify({event:'dg3_reconciled',examined:recent.length,added}));
    return c.json({status:'ok'});
  } catch(e){console.error(JSON.stringify({event:'dg3_reconcile_error',error:safeError(e)}));return c.json({status:'retry'},503);}
});

app.post('/internal/recovery',async(c)=>{
  try {
    const [recovery,notices] = await Promise.all([
      drain(engine),
      (async()=>{await maintainHistory(db);return drainNotices(db,notificationPort);})(),
    ]);
    const health=await queueHealth(engine);
    const notificationFailures=await db.zCard(H.failed);
    console.log(JSON.stringify({event:'dg3_health',...recovery,...health,notices,notificationFailures}));
    return c.json({status:'ok'});
  } catch(e){console.error(JSON.stringify({event:'dg3_recovery_error',error:safeError(e)}));return c.json({status:'retry'},503);}
});
void serve({fetch:app.fetch,createServer,port:getServerPort()});
