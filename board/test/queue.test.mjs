import test from 'node:test';
import assert from 'node:assert/strict';
import { handle } from '../src/worker.js';
import { flush, buildWakeText } from '../src/queue.js';
import { fakeD1 } from './faked1.mjs';

const mkenv = (extra = {}) => ({ DB: fakeD1(), TOKEN: 't', HOOK_TOKEN: 'h', REGISTER_TOKEN: 'r', QUEUE_TOKEN: 'q', ...extra });
const call = (e, method, path, body, token = 't') =>
  handle(new Request('http://x' + path, { method, headers: { authorization: `Bearer ${token}` }, body: body ? JSON.stringify(body) : undefined }), e, {});
const hook = (e, events, token = 'h') => call(e, 'POST', '/hooks/queue', { events }, token);
async function card(e, id, branch, thread) {
  await call(e, 'POST', '/api/cards', { id, title: id });
  await call(e, 'POST', '/api/claim', { id, owner: 'w', branch, owner_thread: thread });
}
const withFetch = async (fn, impl) => { const real = globalThis.fetch; globalThis.fetch = impl; try { return await fn(); } finally { globalThis.fetch = real; } };

test('hook 要權杖', async () => {
  assert.equal((await hook(mkenv(), [], 'bad')).status, 401);
});

test('事件只更新 phase 快取；對不到卡照收；重複事件略過', async () => {
  const e = mkenv(); await card(e, 'c1', 'dev/c1', 'thread-a');
  const r = await (await hook(e, [{ id: 1, kind: 'tested_green', branch: 'dev/c1' }, { id: 2, kind: 'merged', branch: 'dev/nope' }])).json();
  assert.deepEqual([r.applied, r.unmatched, r.duplicate], [1, 1, 0]);
  const get = async () => (await (await call(e, 'GET', '/api/cards')).json())[0];   // 沒 live：讀快取
  assert.deepEqual([(await get()).status, (await get()).phase], ['claimed', 'selftest_green']);
  await hook(e, [{ id: 3, kind: 'queued', branch: 'dev/c1' }, { id: 4, kind: 'merged', branch: 'dev/c1' }]);
  assert.equal((await get()).phase, 'merged');
  assert.equal((await get()).status, 'claimed');   // 卡片自己的狀態不被事件改
  const again = await (await hook(e, [{ id: 1, kind: 'tested_red', branch: 'dev/c1' }])).json();
  assert.equal(again.duplicate, 1);
});

test('排隊器狀態事件帶 to 時，phase 用它', async () => {
  const e = mkenv(); await card(e, 'c1', 'dev/c1', 'a');
  await hook(e, [{ id: 1, kind: 'queued', branch: 'dev/c1', from: 'selftest_green', to: 'batching' }, { id: 2, kind: 'approved', branch: 'dev/c1' }]);
  assert.equal((await (await call(e, 'GET', '/api/cards')).json())[0].phase, 'batching');
});

test('排隊器 notify 的原格式（rejected/conflict/halted）能收', async () => {
  const e = mkenv(); await card(e, 'c1', 'dev/c1', 'thread-a');
  const body = { schema: 1, sent_at: 1, count: 3, text: 'x', events: [
    { id: 1, kind: 'rejected', branch: 'dev/c1', detail: '紅項 a', at: 1 },
    { id: 2, kind: 'conflict', branch: 'dev/c1', detail: null, at: 2 },
    { id: 3, kind: 'halted', branch: null, detail: '磁碟滿', at: 3 }] };
  assert.equal((await call(e, 'POST', '/hooks/queue', body, 'h')).status, 200);
  assert.equal((await (await call(e, 'GET', '/api/cards')).json())[0].phase, 'rejected');
});

test('讀取時向上游查真實狀態（live），查不到用快取', async () => {
  const e = mkenv(); await card(e, 'c1', 'dev/c1', 'a'); await card(e, 'c2', 'dev/c2', 'a');
  await hook(e, [{ id: 1, kind: 'tested_green', branch: 'dev/c1' }]);
  await handle(new Request('http://x/relay/queue', { method: 'PUT', headers: { authorization: 'Bearer r' }, body: 'https://q.trycloudflare.com' }), e, {});
  const list = await withFetch(async () => (await (await call(e, 'GET', '/api/cards?live=1')).json()),
    async (u) => u.endsWith('dev%2Fc1') ? new Response(JSON.stringify({ state: 'queued', approved: true })) : new Response('{}', { status: 404 }));
  const m = Object.fromEntries(list.map(c => [c.id, c]));
  assert.deepEqual([m.c1.phase, m.c1.phase_live, m.c1.approved], ['queued', true, true]);
  assert.equal(m.c2.phase_live, false);
  // 快取也被更新了
  assert.equal((await (await call(e, 'GET', '/api/cards')).json()).find(c => c.id === 'c1').phase, 'queued');
});

test('叫醒：沒設定靜默略過；沒滿批次不送；滿了依負責串各一則；失敗保留', async () => {
  const e = mkenv(); await card(e, 'c1', 'dev/c1', 'thread-a'); await card(e, 'c2', 'dev/c2', 'thread-b');
  await hook(e, [{ id: 1, kind: 'tested_red', branch: 'dev/c1', detail: '紅項 x' }, { id: 2, kind: 'rejected', branch: 'dev/c1' }, { id: 3, kind: 'conflict', branch: 'dev/c2' }]);
  assert.deepEqual(await flush(e), { skipped: 'not configured' });
  Object.assign(e, { ROUTINE_ID: 'routine-x', ROUTINE_KEY: 'k', WAKE_BATCH_S: 300 });
  assert.deepEqual(await flush(e), { waiting: true });
  e.WAKE_BATCH_S = 0;
  assert.equal((await withFetch(() => flush(e), async () => new Response('', { status: 500 }))).sent, 0);   // 失敗保留
  const calls = [];
  const r = await withFetch(() => flush(e), async (u, init) => { calls.push({ u, init, b: JSON.parse(init.body) }); return new Response('{}'); });
  assert.equal(r.sent, 2);
  assert.equal(calls[0].u, 'https://api.anthropic.com/v1/claude_code/routines/routine-x/fire');
  assert.equal(calls[0].init.headers['anthropic-beta'], 'experimental-cc-routine-2026-04-01');
  assert.match(calls.map(c => c.b.text).join('\n'), /thread-a[\s\S]*c1[\s\S]*dev\/c1[\s\S]*自測紅[\s\S]*紅項 x/);
  assert.equal((await withFetch(() => flush(e), async () => new Response('{}'))).sent ?? 0, 0);   // 已送不重送
});

test('叫醒每小時上限', async () => {
  const e = mkenv({ ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0, WAKE_MAX_PER_HOUR: 1 });
  await card(e, 'c1', 'dev/c1', 'a'); await card(e, 'c2', 'dev/c2', 'b');
  await hook(e, [{ id: 1, kind: 'rejected', branch: 'dev/c1' }, { id: 2, kind: 'rejected', branch: 'dev/c2' }]);
  let n = 0;
  await withFetch(() => flush(e), async () => { n++; return new Response('{}'); });
  assert.equal(n, 1);
});

test('合入不叫醒', async () => {
  const e = mkenv({ ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 }); await card(e, 'c1', 'dev/c1', 'a');
  await hook(e, [{ id: 1, kind: 'merged', branch: 'dev/c1' }]);
  assert.equal((await e.DB.prepare('SELECT count(*) AS n FROM outbox').first()).n, 0);
});

const regQueue = e => handle(new Request('http://x/relay/queue', { method: 'PUT', headers: { authorization: 'Bearer r' }, body: 'https://q.trycloudflare.com' }), e, {});

test('放行／撤回／查測試：經門牌打排隊器，帶看板自己的權杖', async () => {
  const e = mkenv(); await card(e, 'c1', 'dev/c1', 'a'); await regQueue(e);
  const seen = [];
  await withFetch(async () => {
    assert.equal((await call(e, 'POST', '/api/cards/c1/approve', { actor: 'w' })).status, 400);   // 沒帶 commit
    const r = await call(e, 'POST', '/api/cards/c1/approve', { commit: 'abc1234', actor: 'w' });
    assert.equal(r.status, 200); assert.equal((await r.json()).delivered, true);
    assert.equal((await call(e, 'POST', '/api/cards/c1/revoke', {})).status, 200);
    assert.equal((await call(e, 'GET', '/api/cards/c1/tests')).status, 200);
  }, async (u, init) => { seen.push({ u, init }); return new Response('{"ok":true}'); });
  const posts = seen.filter(s => s.init.method === 'POST');
  assert.equal(posts[0].u, 'https://q.trycloudflare.com/track');   // 認領時登記
  const ap = posts.find(s => s.u.endsWith('/approve'));
  assert.deepEqual(JSON.parse(ap.init.body), { branch: 'dev/c1', sha: 'abc1234', by: 'w' });
  assert.equal(ap.init.headers.authorization, 'Bearer q');
  assert.ok(seen.some(s => s.u === 'https://q.trycloudflare.com/branches/dev%2Fc1'));
});

const getCard = async (e, id) => (await (await call(e, 'GET', `/api/cards/${id}`)).json());

test('登記制：認領即登記（帶父卡的分支）；連不上先記著，補送到成功；順序不亂', async () => {
  const e = mkenv(); await regQueue(e);
  await call(e, 'POST', '/api/cards', { id: 'P', title: 'p' });
  await call(e, 'POST', '/api/cards', { id: 'K', title: 'k', parent: 'P' });
  const sent = []; let up = false;
  const impl = async (u, init) => { if (!up) throw new Error('down'); if (init.method === 'POST') sent.push([u.replace('https://q.trycloudflare.com', ''), init.body && JSON.parse(init.body)]); return new Response('{}'); };
  await withFetch(async () => {
    await call(e, 'POST', '/api/claim', { id: 'P', owner: 'a' });          // 連不上
    assert.equal((await getCard(e, 'P')).sync_pending, 1);
    await call(e, 'POST', '/api/claim', { id: 'K', owner: 'a' });
    up = true;
    await flushSync(e);                                                       // cron 補送
    assert.equal((await getCard(e, 'P')).sync_pending, 0);
  }, impl);
  assert.deepEqual(sent.map(x => x[0]), ['/track', '/track']);
  assert.deepEqual(sent[0][1], { branch: 'dev/p' });
  // K 的父卡 P 在 K 認領時已有分支，所以帶 parent
  assert.deepEqual(sent[1][1], { branch: 'dev/k', parent: 'dev/p' });
});

test('換分支：先取消舊的再登記新的；釋放／放棄：取消登記', async () => {
  const e = mkenv(); await regQueue(e); await card(e, 'c1', 'dev/c1', 'a');
  const sent = [];
  await withFetch(async () => {
    await call(e, 'POST', '/api/cards/c1/rebranch', {});
    await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped' });
    await call(e, 'PATCH', '/api/cards/c1', { status: 'open' });
  }, async (u, init) => { sent.push(u.replace('https://q.trycloudflare.com', '') + ' ' + init.body); return new Response('{}'); });
  const tail = sent.slice(-4);
  assert.deepEqual(tail, ['/untrack {"branch":"dev/c1"}', '/track {"branch":"dev/c1-r2"}', '/untrack {"branch":"dev/c1-r2"}', '/untrack {"branch":"dev/c1-r2"}']);
});

test('上游明確拒絕（4xx）：標失敗不重送；5xx 保留重送；放行連不上回 202', async () => {
  const e = mkenv(); await regQueue(e);
  await call(e, 'POST', '/api/cards', { id: 'c1', title: 'c1' });
  await withFetch(() => call(e, 'POST', '/api/claim', { id: 'c1', owner: 'a' }), async () => new Response('{}'));
  let n = 0;
  await withFetch(async () => {
    const r = await call(e, 'POST', '/api/cards/c1/approve', { commit: 'abc1234' });
    assert.equal(r.status, 202); assert.equal((await r.json()).sync_pending, 1);
  }, async () => { n++; return new Response('boom', { status: 502 }); });
  assert.equal((await getCard(e, 'c1')).sync_pending, 1);   // 5xx：還在，等補送
  await withFetch(() => flushSync(e), async () => new Response('{"error":"分支的版本已經變了"}', { status: 409 }));
  const c = await getCard(e, 'c1');
  assert.deepEqual([c.sync_pending, c.sync_failed], [0, 1]);
  assert.match(c.sync_error.last_error, /409/);
  const before = n;
  await withFetch(() => flushSync(e), async () => { n++; return new Response('{}'); });
  assert.equal(n, before);   // 失敗的不再重送
});

import { runSync } from '../src/queue.js';
const flushSync = e => runSync(e);
