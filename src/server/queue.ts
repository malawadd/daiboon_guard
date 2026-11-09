/**
 * Daiboon v3 durable queue. No process-local memory or external hosting.
 * Reddit Devvit Redis supports sorted sets, set NX+expiry and WATCH/MULTI/EXEC.
 * Store IDs and short status only. Never store the text of submitted content.
 */
export const Q = {
  ready: 'dg3:ready',
  dead: 'dg3:dead',
  stats: 'dg3:stats',
} as const;

export const MAX_ATTEMPTS = 4;
export const WORKER_SLOTS = 10;
export const RECOVERY_CANDIDATES = 100;
export const LEASE_SECONDS = 65; // >= maximum Devvit request duration; expires after crash
export const RETRY_MS = [5000, 20000, 60000, 180000];

export type ItemId = `t1_${string}` | `t3_${string}`;
export type QueueEntry = { ticket: string; id: ItemId; due: number };
export type RedisPort = {
  set(key: string, value: string, opts?: { nx?: boolean; expiration?: Date }): Promise<unknown>;
  get(key: string): Promise<string | undefined | null>;
  del(key: string): Promise<unknown>;
  incrBy(key: string, n: number): Promise<number>;
  expire(key: string, sec: number): Promise<unknown>;
  zAdd(key: string, ...values: Array<{ member: string; score: number }>): Promise<unknown>;
  zRange(key: string, start: number, end: number, opts?: { by: 'score'; limit?: { offset: number; count: number } }): Promise<Array<{ member: string; score: number }>>;
  zRem(key: string, values: string[]): Promise<unknown>;
  zScore(key: string, member: string): Promise<number | undefined>;
  zCard(key: string): Promise<number>;
  hIncrBy(key: string, field: string, amount: number): Promise<number>;
  watch(key: string): Promise<{
    multi(): Promise<unknown>;
    del(key: string): Promise<unknown>;
    exec(): Promise<unknown>;
    unwatch(): Promise<unknown>;
  }>;
};

/** Events get unique tickets. A new edit never gets lost by a prior worker's zRem. */
export async function enqueue(db: RedisPort, id: ItemId, now = Date.now()): Promise<string> {
  if (!/^t[13]_[a-zA-Z0-9]+$/.test(id)) throw new Error('Bad Reddit fullname');
  const counterKey = `dg3:seq:${id}`;
  const n = await db.incrBy(counterKey, 1);
  const ticket = `${id}|${n}`;
  // queue member itself is the durable job; a failed zAdd makes the event handler fail, not silently succeed.
  await db.zAdd(Q.ready, { member: ticket, score: now });
  await db.zAdd(`dg3:tickets:${id}`, {member:ticket,score:now});
  await db.expire(`dg3:tickets:${id}`,86400*16);
  await db.expire(counterKey, 86400 * 30);
  await db.hIncrBy(Q.stats, 'enqueued', 1);
  return ticket;
}

export function itemFromTicket(ticket: string): ItemId {
  const m = /^(t[13]_[a-zA-Z0-9]+)\|[1-9][0-9]*$/.exec(ticket);
  if (!m) throw new Error(`Invalid queue ticket ${ticket.slice(0, 80)}`);
  return m[1] as ItemId;
}

export async function dueTickets(db: RedisPort, now = Date.now(), max = 25): Promise<string[]> {
  const count = Number.isFinite(max) ? Math.min(RECOVERY_CANDIDATES, Math.max(0, Math.floor(max))) : 0;
  if (!count) return [];
  const rows = await db.zRange(Q.ready, 0, now, { by: 'score', limit: { offset: 0, count } });
  return rows.map(x => x.member);
}

export async function ticketIsDue(db: RedisPort, ticket: string, now: number): Promise<boolean> {
  const score = await db.zScore(Q.ready, ticket);
  return score != null && score <= now;
}

export async function complete(db: RedisPort, ticket: string, state: 'allowed' | 'removed' | 'reviewed' | 'skipped' | 'superseded'): Promise<void> {
  await db.zRem(Q.ready, [ticket]);
  await db.del(`dg3:attempt:${ticket}`);
  await db.hIncrBy(Q.stats, state, 1);
}

export type RetryResult = { status: 'retry' | 'dead'; attempts: number; nextRetryAt: number | null };

export async function retryOrDead(db: RedisPort, ticket: string, now: number): Promise<RetryResult> {
  const attemptsKey = `dg3:attempt:${ticket}`;
  const attempts = await db.incrBy(attemptsKey, 1);
  await db.expire(attemptsKey, 86400 * 7);
  if (attempts >= MAX_ATTEMPTS) {
    // Record dead-letter before removing from ready queue, so a crash can't erase the failure.
    await db.zAdd(Q.dead, { member: ticket, score: now });
    await db.zRem(Q.ready, [ticket]);
    await db.hIncrBy(Q.stats, 'dead', 1);
    return { status: 'dead', attempts, nextRetryAt: null };
  }
  const delay = RETRY_MS[Math.min(attempts - 1, RETRY_MS.length - 1)] ?? 60000;
  await db.zAdd(Q.ready, { member: ticket, score: now + delay });
  await db.hIncrBy(Q.stats, 'retried', 1);
  return { status: 'retry', attempts, nextRetryAt: now + delay };
}

/** Conditional removal prevents lock owner A deleting a lock acquired later by B. */
export async function releaseIfOwned(db: RedisPort, key: string, owner: string): Promise<void> {
  const txn = await db.watch(key);
  try {
    // Read after WATCH through the normal client; transaction commands return the
    // transaction client, not their values. WATCH protects changes before EXEC.
    if (await db.get(key) !== owner) { await txn.unwatch(); return; }
    await txn.multi();
    await txn.del(key);
    await txn.exec();
  } catch (error) {
    try { await txn.unwatch(); } catch { /* lock expires */ }
    throw error;
  }
}

/** Redis set(NX, EX) is atomic across independently executing Devvit instances. */
export async function claim(db: RedisPort, key: string, owner: string): Promise<boolean> {
  const result = await db.set(key, owner, { nx: true, expiration: new Date(Date.now() + LEASE_SECONDS * 1000) });
  // A successful NX returns 'OK' in the Redis implementation. Check the token as an extra guard.
  return Boolean(result) && (await db.get(key)) === owner;
}

/** Slot lease reserves 1 of 10 global workers. No per-process counting. */
export async function getSlot(db: RedisPort, owner: string, maxSlots = WORKER_SLOTS): Promise<string | undefined> {
  for (let i = 0; i < maxSlots; i++) {
    const key = `dg3:slot:${i}`;
    if (await claim(db, key, owner)) return key;
  }
  return undefined;
}

/** Recover stalled work automatically: incomplete tickets remain in the sorted set. */
export async function metrics(db: RedisPort): Promise<{ pending: number; dead: number }> {
  return { pending: await db.zCard(Q.ready), dead: await db.zCard(Q.dead) };
}
