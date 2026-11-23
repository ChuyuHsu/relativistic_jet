import test from 'node:test';
import assert from 'node:assert/strict';
import { handle } from '../src/worker.js';
import { flush, stuckCheck } from '../src/queue.js';
import { fakeD1 } from './faked1.mjs';

const mkenv = (extra = {}) => ({ DB: fakeD1(), TOKEN: 't', HOOK_TOKEN: 'h', ...extra });
const call = (e, method, path, body, token = 't') =>
  handle(new Request('http://x' + path, { method, headers: { authorization: `Bearer ${token}` }, body: body ? JSON.stringify(body) : undefined }), e, {});
const hook = (e, events, token = 'h') => call(e, 'POST', '/hooks/queue', { events }, token);
const act = (e, id, a, body = {}) => call(e, 'POST', `/api/cards/${id}/${a}`, body);
const getCard = async (e, id) => (await call(e, 'GET', `/api/cards/${id}`)).json();
const withFetch = async (fn, impl) => { const real = globalThis.fetch; globalThis.fetch = impl; try { return await fn(); } finally { globalThis.fetch = real; } };
const SHA = 'abc1234', SHA2 = 'bcd2345';
// 領卡（領卡人 w）
async function claim(e, id, thread = 'dev-' + id, extra = {}) {
  await call(e, 'POST', '/api/cards', { id, title: id, ...extra });
  return (await call(e, 'POST', '/api/claim', { id, owner: 'w', owner_thread: thread })).json();
}
// 領卡→送審→放行（審查者 rev）
async function approved(e, id, thread, extra) {
  await claim(e, id, thread, extra); await act(e, id, 'submit', { commit: SHA, actor: 'w' });
  return act(e, id, 'approve', { commit: SHA, actor: 'rev' });
}
const br = id => `dev/${id.toLowerCase()}`;
const ev = (e, id, o) => hook(e, [{ id: ++ev.n, branch: br(id), ...o }]);
ev.n = 0;
const stageOf = async (e, id) => (await getCard(e, id)).stage;

test('hook 與拉清單要權杖', async () => {
  assert.equal((await hook(mkenv(), [], 'bad')).status, 401);
});

test('主線：開卡→領→送審→放行→自測綠→合併；審查者不能是領卡人', async () => {
  const e = mkenv(); await call(e, 'POST', '/api/cards', { id: 'c1', title: 'c1' });
  assert.equal(await stageOf(e, 'c1'), 'new');
  await call(e, 'POST', '/api/claim', { id: 'c1', owner: 'w', owner_thread: 'a' });
  assert.equal(await stageOf(e, 'c1'), 'developing');
  assert.equal((await act(e, 'c1', 'submit', { commit: 'zzz' })).status, 400);
  await act(e, 'c1', 'submit', { commit: SHA });
  assert.equal(await stageOf(e, 'c1'), 'reviewing');
  assert.equal((await act(e, 'c1', 'approve', { commit: SHA, actor: 'w' })).status, 409);       // 放行的人不能是領卡的人
  assert.equal((await act(e, 'c1', 'approve', { commit: 'fffffff', actor: 'rev' })).status, 409);   // 版本必須等於送審的
  const a = await (await act(e, 'c1', 'approve', { commit: SHA + 'deadbeef', actor: 'rev' })).json();   // 前綴比對
  assert.deepEqual([a.stage, a.approved_by], ['selftesting', 'rev']);
  await ev(e, 'c1', { kind: 'tested_green', approved_sha: SHA });
  assert.equal(await stageOf(e, 'c1'), 'batching');
  await ev(e, 'c1', { kind: 'merged', approved_sha: SHA });
  assert.equal(await stageOf(e, 'c1'), 'merged');
});

test('表外動作一律 409 並回現在的 stage；同一動作重送結果一樣', async () => {
  const e = mkenv(); await claim(e, 'c1');
  for (const a of ['approve', 'revoke', 'retest']) {
    const r = await act(e, 'c1', a, { commit: SHA, actor: 'rev' });
    assert.deepEqual([r.status, (await r.json()).stage], [409, 'developing'], a);
  }
  assert.equal((await act(e, 'c1', 'reject', { reason: 'x' })).status, 409);   // 還沒送審
  await act(e, 'c1', 'submit', { commit: SHA });
  assert.equal((await act(e, 'c1', 'submit', { commit: SHA })).status, 200);   // 同版本重送
  assert.equal((await act(e, 'c1', 'revoke')).status, 200);                    // 審查中撤回＝沒事
  await act(e, 'c1', 'approve', { commit: SHA, actor: 'rev' });
  assert.equal((await act(e, 'c1', 'approve', { commit: SHA, actor: 'rev' })).status, 200);   // 重複放行
  await act(e, 'c1', 'reject', { reason: '少測試', actor: 'rev' });
  assert.equal((await act(e, 'c1', 'reject', { reason: '少測試', actor: 'rev' })).status, 200);
  assert.equal((await act(e, 'c1', 'reject', { reason: '別的原因' })).status, 409);
  // 不存在的卡
  assert.equal((await act(e, 'zzz', 'submit', { commit: SHA })).status, 404);
});

test('審查退回：原因存卡片、通知開發者、放行清掉；改好再送審；被審查退回不能重測', async () => {
  const e = mkenv({ ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 }); await approved(e, 'c1', 'dev1');
  await ev(e, 'c1', { kind: 'tested_green', approved_sha: SHA });
  const r = await (await act(e, 'c1', 'reject', { reason: '少測試', actor: 'rev' })).json();   // 批測中也能退回
  assert.deepEqual([r.stage, r.rejected.source, r.rejected.reason, r.approved_commit], ['rejected', 'review', '少測試', null]);
  assert.equal((await act(e, 'c1', 'retest', { actor: 'rev' })).status, 409);
  assert.equal((await act(e, 'c1', 'submit', { commit: SHA })).status, 409);                    // 版本沒變不能再送審
  const texts = []; await withFetch(() => flush(e), async (u, init) => { texts.push(JSON.parse(init.body).text); return new Response('{}'); });
  assert.match(texts.join(), /dev1[\s\S]*審查退回[\s\S]*少測試/);
  await act(e, 'c1', 'submit', { commit: SHA2 });
  assert.deepEqual([await stageOf(e, 'c1'), (await getCard(e, 'c1')).rejected_reason], ['reviewing', null]);
});

test('撤回放行回審查中（保留送審版本）；放行後又送審新版本回審查中並清放行與結果', async () => {
  const e = mkenv(); await approved(e, 'c1');
  await ev(e, 'c1', { kind: 'tested_green', approved_sha: SHA });
  await act(e, 'c1', 'revoke');
  let c = await getCard(e, 'c1');
  assert.deepEqual([c.stage, c.submitted_commit, c.approved_commit, c.result], ['reviewing', SHA, null, null]);
  await act(e, 'c1', 'approve', { commit: SHA, actor: 'rev' });
  await act(e, 'c1', 'submit', { commit: SHA2 });
  c = await getCard(e, 'c1');
  assert.deepEqual([c.stage, c.submitted_commit, c.approved_commit], ['reviewing', SHA2, null]);
});

test('排隊器退回→重測：放行保留、retest 加一、清結果，回自測中；只有排隊器退回的能重測', async () => {
  const e = mkenv(); await approved(e, 'c1');
  await ev(e, 'c1', { kind: 'tested_red', approved_sha: SHA, retest: 0, detail: '紅', failed_items: [{ item_id: 'unit', status: 'fail' }] });
  const c = await getCard(e, 'c1');
  assert.deepEqual([c.stage, c.rejected.source, c.rejected.failed_items[0].item_id], ['rejected', 'queue', 'unit']);
  const r = await (await act(e, 'c1', 'retest', { actor: 'rev' })).json();
  assert.deepEqual([r.stage, r.retest, r.approved_commit, r.result], ['selftesting', 1, SHA, null]);
  assert.equal((await act(e, 'c1', 'retest')).status, 409);                       // 已不是退回
  // 舊的 retest=0 回報晚到：丟掉
  const stale = await (await ev(e, 'c1', { kind: 'tested_red', approved_sha: SHA, retest: 0 })).json();
  assert.equal(stale.stale, 1);
  assert.equal(await stageOf(e, 'c1'), 'selftesting');
  await ev(e, 'c1', { kind: 'tested_green', approved_sha: SHA, retest: 1 });
  assert.equal(await stageOf(e, 'c1'), 'batching');
});

test('過期回報不算數：放行版本不符、沒放行、已放棄的卡的事件都丟掉（回 200，不改卡）', async () => {
  const e = mkenv(); await claim(e, 'c1'); await claim(e, 'c2'); await approved(e, 'c3');
  assert.equal((await (await ev(e, 'c1', { kind: 'tested_red' })).json()).stale, 1);   // 沒放行
  assert.equal((await ev(e, 'c3', { kind: 'tested_red', approved_sha: 'fffffff' })).status, 200);
  assert.equal(await stageOf(e, 'c3'), 'selftesting');                                    // 舊放行版本晚到的退回不算
  await call(e, 'PATCH', '/api/cards/c2', { status: 'dropped' });
  assert.equal((await (await ev(e, 'c2', { kind: 'merged' })).json()).stale, 1);
  assert.equal(await stageOf(e, 'c2'), 'dropped');
  const dup = await (await hook(e, [{ id: 777, kind: 'tested_green', branch: br('c3'), approved_sha: SHA }, { id: 777, kind: 'tested_red', branch: br('c3'), approved_sha: SHA }])).json();
  assert.deepEqual([dup.applied, dup.duplicate], [1, 1]);                                 // 以事件編號去重
});

test('內容變了（superseded）：清送審放行結果，回開發中，通知開發者要重新送審', async () => {
  const e = mkenv({ ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 }); await approved(e, 'c1', 'dev1');
  await ev(e, 'c1', { kind: 'tested_green', approved_sha: SHA });
  await ev(e, 'c1', { kind: 'superseded', approved_sha: SHA, detail: '新版本' });
  const c = await getCard(e, 'c1');
  assert.deepEqual([c.stage, c.submitted_commit, c.approved_commit, c.result], ['developing', null, null, null]);
  const texts = []; await withFetch(() => flush(e), async (u, init) => { texts.push(JSON.parse(init.body).text); return new Response('{}'); });
  assert.match(texts[0], /dev1[\s\S]*要重新送審/);
});

test('重新接到最新主線、內容沒變：排隊器回 pending，放行保留，退回回到自測中', async () => {
  const e = mkenv(); await approved(e, 'c1');
  await ev(e, 'c1', { kind: 'conflict', approved_sha: SHA, detail: '撞檔 x' });
  assert.equal(await stageOf(e, 'c1'), 'rejected');
  await ev(e, 'c1', { kind: 'pushed', approved_sha: SHA, commit: 'eeeeeee' });
  const c = await getCard(e, 'c1');
  assert.deepEqual([c.stage, c.approved_commit], ['selftesting', SHA]);
});

test('暫停（halted）：stage 仍是退回但標 halted，通知管理者；撞檔也是退回；重測可用', async () => {
  const e = mkenv({ ADMIN_THREADS: 'boss', ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 }); await approved(e, 'c1', 'dev1'); await approved(e, 'c2', 'dev2');
  await ev(e, 'c1', { kind: 'halted', approved_sha: SHA, detail: '批測紅兩次', failed_items: [{ item_id: 'u' }] });
  await ev(e, 'c2', { kind: 'conflict', approved_sha: SHA, detail: '撞檔 x' });
  const c1 = await getCard(e, 'c1'), c2 = await getCard(e, 'c2');
  assert.deepEqual([c1.stage, c1.rejected.halted, c1.rejected.reason, c1.rejected.failed_items[0].item_id], ['rejected', true, '批測紅兩次', 'u']);
  assert.deepEqual([c2.stage, c2.rejected.halted], ['rejected', false]);
  const texts = []; await withFetch(() => flush(e), async (u, init) => { texts.push(JSON.parse(init.body).text); return new Response('{}'); });
  assert.ok(texts.some(t => t.includes('boss') && t.includes('暫停')));      // 暫停通知管理者
  assert.ok(texts.some(t => t.includes('dev2') && t.includes('撞檔')));      // 撞檔通知開發者
  assert.ok(!texts.some(t => t.includes('dev1')));
  assert.equal((await (await act(e, 'c1', 'retest')).json()).stage, 'selftesting');
});

test('父卡擋住、排隊器自己出問題：通知管理者，卡片不動', async () => {
  const e = mkenv({ ADMIN_THREADS: 'boss', ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 }); await approved(e, 'c1', 'dev1');
  await ev(e, 'c1', { kind: 'parent_blocked', approved_sha: SHA });
  await hook(e, [{ id: 900, kind: 'queue_error', branch: null, detail: '磁碟滿' }]);
  assert.equal(await stageOf(e, 'c1'), 'selftesting');
  const texts = []; await withFetch(() => flush(e), async (u, init) => { texts.push(JSON.parse(init.body).text); return new Response('{}'); });
  assert.equal(texts.length, 1);
  assert.match(texts[0], /boss[\s\S]*父卡[\s\S]*磁碟滿/);
});

test('合併是結局：任何動作都 409（含放棄、還卡、送審）', async () => {
  const e = mkenv(); await approved(e, 'c1');
  await ev(e, 'c1', { kind: 'merged', approved_sha: SHA });
  for (const a of ['submit', 'reject', 'approve', 'revoke', 'retest']) assert.equal((await act(e, 'c1', a, { commit: SHA, reason: 'x' })).status, 409, a);
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped' })).status, 409);
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'open' })).status, 409);
});

test('還卡：審查中、退回可以；自測／批測中不行（要先撤回放行）；清送審放行結果、分支保留', async () => {
  const e = mkenv(); await approved(e, 'c1');
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'open' })).status, 409);
  await act(e, 'c1', 'revoke');
  const o = await (await call(e, 'PATCH', '/api/cards/c1', { status: 'open' })).json();
  assert.deepEqual([o.stage, o.owner, o.submitted_commit, o.branch], ['new', null, null, 'dev/c1']);
});

test('?stage= 過濾；送審叫醒審查串（攢批依串合成）', async () => {
  const e = mkenv({ REVIEW_THREADS: 'rv1, rv2', ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 });
  await claim(e, 'c1'); await claim(e, 'c2'); await act(e, 'c1', 'submit', { commit: SHA }); await act(e, 'c2', 'submit', { commit: SHA2 });
  assert.deepEqual((await (await call(e, 'GET', '/api/cards?stage=reviewing')).json()).map(c => c.id).sort(), ['c1', 'c2']);
  assert.equal((await (await call(e, 'GET', '/api/cards?stage=developing,merged')).json()).length, 0);
  const texts = []; await withFetch(() => flush(e), async (u, init) => { texts.push(JSON.parse(init.body).text); return new Response('{}'); });
  assert.equal(texts.length, 2);   // rv1、rv2 各一則（各含兩張卡）
  assert.match(texts.find(t => t.includes('rv1')), /c1[\s\S]*送審待審[\s\S]*c2/);
});

test('排隊器拉清單：已領、未合併、未放棄；帶父卡分支（父卡合併後省略）、放行版本、retest', async () => {
  const e = mkenv({ READ_TOKEN: 'rd' });
  await approved(e, 'P'); await claim(e, 'K', 'a', { parent: 'P' });
  await act(e, 'K', 'submit', { commit: SHA }); await act(e, 'K', 'approve', { commit: SHA, actor: 'rev' });
  await call(e, 'POST', '/api/cards', { id: 'O', title: 'o' });                                  // 未領：不在
  await claim(e, 'D'); await call(e, 'PATCH', '/api/cards/D', { status: 'dropped' });           // 放棄：不在
  await approved(e, 'M'); await ev(e, 'M', { kind: 'merged', approved_sha: SHA });              // 已合併：不在
  await claim(e, 'N');                                                                          // 開發中（沒放行）：在，但不帶 approved_sha
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 'bad')).status, 401);
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 't')).status, 401);       // 寫入權杖不能拉
  const a = await (await call(e, 'GET', '/api/queue/desired', undefined, 'h')).json();
  assert.deepEqual(a, [{ branch: 'dev/p', approved_sha: SHA, retest: 0 }, { branch: 'dev/k', parent: 'dev/p', approved_sha: SHA, retest: 0 }, { branch: 'dev/n', retest: 0 }]);
  assert.deepEqual(await (await call(e, 'GET', '/api/queue/desired', undefined, 'rd')).json(), a);
  await ev(e, 'P', { kind: 'merged', approved_sha: SHA });                                      // 父卡合併後子卡的 parent 省略
  assert.equal((await (await call(e, 'GET', '/api/queue/desired', undefined, 'h')).json()).find(x => x.branch === 'dev/k').parent, undefined);
});

test('拉清單出錯回非 200，不回空清單', async () => {
  const e = mkenv(); e.DB = { prepare() { throw new Error('db down'); } };
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 'h')).status, 500);
});

test('結果帶紅項、提交、to：存在卡片上，/tests 回存下來的最新結果；approved／revoked 事件不蓋結果', async () => {
  const e = mkenv(); await approved(e, 'c1');
  await ev(e, 'c1', { kind: 'queued', approved_sha: SHA, commit: 'deadbeef', from: 'selftest_green', to: 'batching' });
  await ev(e, 'c1', { kind: 'approved', approved_sha: SHA });
  const t = await (await call(e, 'GET', '/api/cards/c1/tests')).json();
  assert.deepEqual([t.phase, t.result.kind, t.result.commit, t.approved_commit], ['batching', 'queued', 'deadbeef', SHA]);
  assert.equal((await call(e, 'GET', '/api/cards/zzz/tests')).status, 404);
});

test('叫醒：沒設定靜默略過；沒滿批次不送；依負責串各一則；失敗保留；已送不重送；每小時上限', async () => {
  const e = mkenv(); await approved(e, 'c1', 'cmsg_a'); await approved(e, 'c2', 'cmsg_b');
  await ev(e, 'c1', { kind: 'tested_red', approved_sha: SHA, detail: '紅項 x' }); await ev(e, 'c2', { kind: 'conflict', approved_sha: SHA });
  assert.deepEqual(await flush(e), { skipped: 'not configured' });
  Object.assign(e, { ROUTINE_ID: 'routine-x', ROUTINE_KEY: 'k', WAKE_BATCH_S: 300 });
  assert.deepEqual(await flush(e), { waiting: true });
  e.WAKE_BATCH_S = 0;
  assert.equal((await withFetch(() => flush(e), async () => new Response('', { status: 500 }))).sent, 0);
  const calls = [];
  assert.equal((await withFetch(() => flush(e), async (u, init) => { calls.push({ u, init, b: JSON.parse(init.body) }); return new Response('{}'); })).sent, 2);
  assert.equal(calls[0].u, 'https://api.anthropic.com/v1/claude_code/routines/routine-x/fire');
  assert.equal(calls[0].init.headers['anthropic-beta'], 'experimental-cc-routine-2026-04-01');
  assert.match(calls.map(c => c.b.text).join('\n'), /cmsg_a[\s\S]*c1[\s\S]*dev\/c1[\s\S]*自測紅[\s\S]*紅項 x/);
  assert.equal((await withFetch(() => flush(e), async () => new Response('{}'))).sent ?? 0, 0);
  const e2 = mkenv({ ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0, WAKE_MAX_PER_HOUR: 1 });
  await approved(e2, 'c1', 'a'); await approved(e2, 'c2', 'b');
  await ev(e2, 'c1', { kind: 'rejected', approved_sha: SHA }); await ev(e2, 'c2', { kind: 'rejected', approved_sha: SHA });
  let n = 0; await withFetch(() => flush(e2), async () => { n++; return new Response('{}'); });
  assert.equal(n, 1);
});

test('合併不叫醒', async () => {
  const e = mkenv({ ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 }); await approved(e, 'c1', 'a');
  await ev(e, 'c1', { kind: 'merged', approved_sha: SHA });
  assert.equal((await e.DB.prepare('SELECT count(*) AS n FROM outbox').first()).n, 0);
});

test('卡住提醒：開發中、審查中、退回超過時數才通知管理串，同一次卡住只提醒一次；自測中不算', async () => {
  const e = mkenv({ ADMIN_THREADS: 'boss', STUCK_HOURS: 6 });
  await claim(e, 'c1'); await claim(e, 'c2'); await act(e, 'c2', 'submit', { commit: SHA }); await approved(e, 'c3');
  assert.deepEqual(await stuckCheck(e), { notified: 0 });
  await e.DB.prepare('UPDATE cards SET updated_at=updated_at-7*3600').run();
  assert.deepEqual(await stuckCheck(e), { notified: 2 });      // c1 開發中、c2 審查中；c3 自測中不算
  assert.deepEqual(await stuckCheck(e), { notified: 0 });
  assert.deepEqual(await stuckCheck(mkenv()), { skipped: true });
});
