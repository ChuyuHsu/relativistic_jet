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

test('放行／撤回／查測試：經門牌打排隊器，帶看板自己的權杖', async () => {
  const e = mkenv(); await card(e, 'c1', 'dev/c1', 'a');
  assert.equal((await call(e, 'POST', '/api/cards/c1/approve', { commit: 'abc1234' })).status, 503);   // 還沒登記門牌
  await handle(new Request('http://x/relay/queue', { method: 'PUT', headers: { authorization: 'Bearer r' }, body: 'https://q.trycloudflare.com' }), e, {});
  const seen = [];
  await withFetch(async () => {
    assert.equal((await call(e, 'POST', '/api/cards/c1/approve', { actor: 'w' })).status, 400);   // 沒帶 commit
    const r = await call(e, 'POST', '/api/cards/c1/approve', { commit: 'abc1234', actor: 'w' });
    assert.equal(r.status, 200);
    assert.equal((await call(e, 'POST', '/api/cards/c1/revoke', {})).status, 200);
    assert.equal((await call(e, 'GET', '/api/cards/c1/tests')).status, 200);
  }, async (u, init) => { seen.push({ u, init }); return new Response('{"ok":true}'); });
  assert.equal(seen[0].u, 'https://q.trycloudflare.com/approve');
  assert.deepEqual(JSON.parse(seen[0].init.body), { branch: 'dev/c1', sha: 'abc1234', by: 'w' });
  assert.equal(seen[0].init.headers.authorization, 'Bearer q');
  assert.equal(seen[1].u, 'https://q.trycloudflare.com/revoke');
  assert.equal(seen[2].u, 'https://q.trycloudflare.com/branches/dev%2Fc1');
});
