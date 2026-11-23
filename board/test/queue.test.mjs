import test from 'node:test';
import assert from 'node:assert/strict';
import { handle } from '../src/worker.js';
import { flush, buildWakeText } from '../src/queue.js';
import { fakeD1 } from './faked1.mjs';

const mkenv = (extra = {}) => ({ DB: fakeD1(), TOKEN: 't', HOOK_TOKEN: 'h', ...extra });
const call = (e, method, path, body, token = 't') =>
  handle(new Request('http://x' + path, { method, headers: { authorization: `Bearer ${token}` }, body: body ? JSON.stringify(body) : undefined }), e, {});
const hook = (e, events, token = 'h') => call(e, 'POST', '/hooks/queue', { events }, token);
async function card(e, id, thread, extra = {}) {
  await call(e, 'POST', '/api/cards', { id, title: id, ...extra });
  return (await call(e, 'POST', '/api/claim', { id, owner: 'w', owner_thread: thread })).json();
}
const getCard = async (e, id) => (await call(e, 'GET', `/api/cards/${id}`)).json();
const withFetch = async (fn, impl) => { const real = globalThis.fetch; globalThis.fetch = impl; try { return await fn(); } finally { globalThis.fetch = real; } };

test('hook 要權杖', async () => {
  assert.equal((await hook(mkenv(), [], 'bad')).status, 401);
});

test('事件只更新 phase 與最新結果；對不到卡照收；重複事件略過', async () => {
  const e = mkenv(); await card(e, 'c1', 'a');
  const r = await (await hook(e, [{ id: 1, kind: 'tested_green', branch: 'dev/c1', commit: 'abc' }, { id: 2, kind: 'merged', branch: 'dev/nope' }])).json();
  assert.deepEqual([r.applied, r.unmatched, r.duplicate], [1, 1, 0]);
  let c = await getCard(e, 'c1');
  assert.deepEqual([c.status, c.phase, c.result.commit], ['claimed', 'selftest_green', 'abc']);
  await hook(e, [{ id: 3, kind: 'queued', branch: 'dev/c1' }, { id: 4, kind: 'merged', branch: 'dev/c1' }]);
  assert.equal((await getCard(e, 'c1')).phase, 'merged');
  assert.equal((await getCard(e, 'c1')).status, 'claimed');
  assert.equal((await (await hook(e, [{ id: 1, kind: 'tested_red', branch: 'dev/c1' }])).json()).duplicate, 1);
});

test('結果帶紅項、提交、to：存在卡片上，/tests 回存下來的最新結果', async () => {
  const e = mkenv(); await card(e, 'c1', 'a');
  await hook(e, [{ id: 1, kind: 'rejected', branch: 'dev/c1', commit: 'deadbeef', detail: '紅', failed_items: [{ item_id: 'unit', status: 'fail', summary: 'x' }], from: 'selftesting', to: 'rejected' }]);
  const t = await (await call(e, 'GET', '/api/cards/c1/tests')).json();
  assert.deepEqual([t.phase, t.result.commit, t.result.failed_items[0].item_id, t.result.to], ['rejected', 'deadbeef', 'unit', 'rejected']);
  assert.equal((await call(e, 'GET', '/api/cards/zzz/tests')).status, 404);
});

test('排隊器狀態事件帶 to 時，phase 用它；approved／revoked 事件不蓋掉最新結果', async () => {
  const e = mkenv(); await card(e, 'c1', 'a');
  await hook(e, [{ id: 1, kind: 'queued', branch: 'dev/c1', from: 'selftest_green', to: 'batching' }, { id: 2, kind: 'approved', branch: 'dev/c1' }]);
  const c = await getCard(e, 'c1');
  assert.deepEqual([c.phase, c.result.kind], ['batching', 'queued']);
});

test('排隊器 notify 的原格式（rejected/conflict/halted）能收', async () => {
  const e = mkenv(); await card(e, 'c1', 'a');
  const body = { schema: 1, sent_at: 1, count: 3, text: 'x', events: [
    { id: 1, kind: 'rejected', branch: 'dev/c1', detail: '紅項 a', at: 1 },
    { id: 2, kind: 'conflict', branch: 'dev/c1', detail: null, at: 2 },
    { id: 3, kind: 'halted', branch: null, detail: '磁碟滿', at: 3 }] };
  assert.equal((await call(e, 'POST', '/hooks/queue', body, 'h')).status, 200);
  assert.equal((await getCard(e, 'c1')).phase, 'rejected');
});

test('放行是卡片欄位：放行、撤回、換分支清空；不需要連排隊器', async () => {
  const e = mkenv(); await card(e, 'c1', 'a');
  assert.equal((await call(e, 'POST', '/api/cards/c1/approve', {})).status, 400);
  const a = await (await call(e, 'POST', '/api/cards/c1/approve', { commit: 'abc1234', actor: 'rev' })).json();
  assert.deepEqual([a.approved_commit, a.approved_by], ['abc1234', 'rev']);
  const r = await (await call(e, 'POST', '/api/cards/c1/revoke', {})).json();
  assert.equal(r.approved_commit, null);
  await call(e, 'POST', '/api/cards/c1/approve', { commit: 'abc1234' });
  const nb = await (await call(e, 'POST', '/api/cards/c1/rebranch', {})).json();
  assert.deepEqual([nb.branch, nb.approved_commit], ['dev/c1-r2', null]);
});

test('排隊器拉清單：已認領、有分支、未合入的卡；帶父卡分支與放行；要權杖', async () => {
  const e = mkenv({ READ_TOKEN: 'rd' });
  await card(e, 'P', 'a'); await card(e, 'K', 'a', { parent: 'P' });
  await call(e, 'POST', '/api/cards', { id: 'O', title: 'o' });            // 未認領：不在清單
  await card(e, 'D', 'a'); await call(e, 'PATCH', '/api/cards/D', { status: 'dropped' });   // 放棄：不在
  await card(e, 'M', 'a'); await hook(e, [{ id: 1, kind: 'merged', branch: 'dev/m' }]);     // 已合入：不在
  await call(e, 'POST', '/api/cards/K/approve', { commit: 'abc1234' });
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 'bad')).status, 401);
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 't')).status, 401);   // 寫入權杖不能拉，只認 HOOK_TOKEN／READ_TOKEN
  const a = await (await call(e, 'GET', '/api/queue/desired', undefined, 'h')).json();
  assert.deepEqual(a, [{ branch: 'dev/p' }, { branch: 'dev/k', parent: 'dev/p', approved_sha: 'abc1234' }]);
  assert.deepEqual(await (await call(e, 'GET', '/api/queue/desired', undefined, 'rd')).json(), a);
});

test('拉清單出錯回非 200，不回空清單', async () => {
  const e = mkenv(); e.DB = { prepare() { throw new Error('db down'); } };
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 'h')).status, 500);
});

test('叫醒：沒設定靜默略過；沒滿批次不送；滿了依負責串各一則；失敗保留', async () => {
  const e = mkenv(); await card(e, 'c1', 'cmsg_a'); await card(e, 'c2', 'cmsg_b');
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
  assert.match(calls.map(c => c.b.text).join('\n'), /cmsg_a[\s\S]*c1[\s\S]*dev\/c1[\s\S]*自測紅[\s\S]*紅項 x/);
  assert.equal((await withFetch(() => flush(e), async () => new Response('{}'))).sent ?? 0, 0);   // 已送不重送
});

test('叫醒每小時上限', async () => {
  const e = mkenv({ ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0, WAKE_MAX_PER_HOUR: 1 });
  await card(e, 'c1', 'a'); await card(e, 'c2', 'b');
  await hook(e, [{ id: 1, kind: 'rejected', branch: 'dev/c1' }, { id: 2, kind: 'rejected', branch: 'dev/c2' }]);
  let n = 0;
  await withFetch(() => flush(e), async () => { n++; return new Response('{}'); });
  assert.equal(n, 1);
});

test('合入不叫醒', async () => {
  const e = mkenv({ ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 }); await card(e, 'c1', 'a');
  await hook(e, [{ id: 1, kind: 'merged', branch: 'dev/c1' }]);
  assert.equal((await e.DB.prepare('SELECT count(*) AS n FROM outbox').first()).n, 0);
});
