import test from 'node:test';
import assert from 'node:assert/strict';
import { handle } from '../src/worker.js';
import { flush, stuckCheck } from '../src/queue.js';
import { fakeD1 } from './faked1.mjs';

const mkenv = (extra = {}) => ({ DB: fakeD1(), TOKEN: 't', HOOK_TOKEN: 'h', ...extra });
const call = (e, method, path, body, token = 't') =>
  handle(new Request('http://x' + path, { method, headers: { authorization: `Bearer ${token}` }, body: body ? JSON.stringify(method === 'GET' ? body : { actor: 'tester', ...body }) : undefined }), e, {});
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
const ev = (e, id, o) => hook(e, [{ id: ++ev.n, branch: br(id), card: id, ...o }]);
ev.n = 0;
// 放棄（放棄防呆）：帶原因進待查，再由排隊器回報查驗通過
const drop = async (e, id) => { const r = await call(e, 'PATCH', `/api/cards/${id}`, { status: 'dropped', discard_reason: '測試' }); if ((await getCard(e, id)).stage === 'dropping') await ev(e, id, { kind: 'drop_checked', unmerged: false, covered: true }); return r; };
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
  await drop(e, 'c2');
  assert.equal((await (await ev(e, 'c2', { kind: 'tested_red' })).json()).stale, 1);
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
  await claim(e, 'D'); await drop(e, 'D');           // 放棄：不在
  await approved(e, 'M'); await ev(e, 'M', { kind: 'merged', approved_sha: SHA });              // 已合併：不在
  await claim(e, 'N');                                                                          // 開發中（沒放行）：在，但不帶 approved_sha
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 'bad')).status, 401);
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 't')).status, 200);       // 寫入權杖也能讀（只讀端點）
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
  const e = mkenv(); await approved(e, 'c1', 'thread-a'); await approved(e, 'c2', 'thread-b');
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
  assert.match(calls.map(c => c.b.text).join('\n'), /thread-a[\s\S]*c1[\s\S]*dev\/c1[\s\S]*自測紅[\s\S]*紅項 x/);
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
  await act(e, 'c3', 'reject', { reason: 'x', actor: 'rev' }); await e.DB.prepare('UPDATE cards SET updated_at=updated_at-7*3600').run();   // c3 退回
  assert.deepEqual(await stuckCheck(e), { notified: 3 });      // c1 開發中、c2 審查中、c3 退回
  assert.deepEqual(await stuckCheck(e), { notified: 0 });
  assert.deepEqual(await stuckCheck(mkenv()), { skipped: true });
});

test('設父卡：可改可清空且放行保留；拒絕自己、不存在、已放棄、循環', async () => {
  const e = mkenv(); await approved(e, 'A'); await claim(e, 'B', 'b', { parent: 'A' }); await claim(e, 'C');
  const patch = (id, parent) => call(e, 'PATCH', `/api/cards/${id}`, { parent });
  assert.equal((await patch('A', 'A')).status, 409);          // 自己
  assert.equal((await patch('A', 'zzz')).status, 409);        // 不存在
  assert.equal((await patch('A', 'B')).status, 409);          // 循環：B 的父卡是 A
  await claim(e, 'D'); await drop(e, 'D');
  assert.equal((await patch('C', 'D')).status, 409);          // 已放棄
  assert.equal((await call(e, 'POST', '/api/cards', { id: 'E', title: 'e', parent: 'D' })).status, 409);
  assert.equal((await patch('B', 'C')).status, 200);          // 改成別張卡
  assert.equal((await getCard(e, 'B')).parent, 'C');
  const r = await (await patch('A', null)).json();            // 清空；放行保留
  assert.deepEqual([r.parent, r.stage, r.approved_commit], [null, 'selftesting', SHA]);
  assert.equal((await getCard(e, 'B')).parent, 'C');
});

test('對照文檔第 6 節「自己的事」補測：審查中改審新版本、還卡三種來源、批測紅、沒輪到不變、結局後拒絕', async () => {
  const e = mkenv();
  await claim(e, 'c1'); await act(e, 'c1', 'submit', { commit: SHA }); await act(e, 'c1', 'submit', { commit: SHA2 });
  const c = await getCard(e, 'c1');
  assert.deepEqual([c.stage, c.submitted_commit], ['reviewing', SHA2]);                    // 審查中：改審新版本
  await act(e, 'c1', 'approve', { commit: SHA2, actor: 'rev' });
  await ev(e, 'c1', { kind: 'tested_green', approved_sha: SHA2 });
  await ev(e, 'c1', { kind: 'deferred', approved_sha: SHA2, detail: '這批沒輪到' });          // 沒輪到、同批別人紅：不變
  assert.equal(await stageOf(e, 'c1'), 'batching');
  await ev(e, 'c1', { kind: 'rejected', approved_sha: SHA2, detail: '批測是它紅', failed_items: [{ item_id: 'x' }] });
  const r = await getCard(e, 'c1');
  assert.deepEqual([r.stage, r.rejected.source], ['rejected', 'queue']);                    // 批測找出是它紅
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'open' })).status, 200);   // 退回也能還卡
  await claim(e, 'c2'); assert.equal((await call(e, 'PATCH', '/api/cards/c2', { status: 'open' })).status, 200);   // 開發中還卡
  await claim(e, 'c3'); await drop(e, 'c3');
  for (const a of ['submit', 'approve', 'reject', 'revoke', 'retest']) assert.equal((await act(e, 'c3', a, { commit: SHA, reason: 'x' })).status, 409, a);   // 放棄後任何動作拒絕
});

test('對照文檔第 6 節「父卡的事」：等父卡、父卡進 main、父卡被還卡、父卡被放棄', async () => {
  const e = mkenv({ ADMIN_THREADS: 'boss', ROUTINE_ID: 'i', ROUTINE_KEY: 'k', WAKE_BATCH_S: 0 });
  await claim(e, 'P'); await approved(e, 'K', 'kt', { parent: 'P' });
  let k = await getCard(e, 'K');
  assert.deepEqual([k.stage, k.parent, k.parent_stage, k.waiting_parent], ['selftesting', 'P', 'developing', true]);   // 父卡還沒放行：子卡放行後停在自測中等，且看得到在等父卡
  const d = await (await call(e, 'GET', '/api/queue/desired', undefined, 'h')).json();
  assert.equal(d.find(x => x.branch === 'dev/k').parent, 'dev/p');                                                    // 排隊器靠 parent 排順序
  await call(e, 'PATCH', '/api/cards/P', { status: 'open' });                                                         // 父卡被還卡：子卡照樣等，不動
  k = await getCard(e, 'K');
  assert.deepEqual([k.stage, k.parent_stage, k.waiting_parent, k.approved_commit], ['selftesting', 'new', true, SHA]);
  await call(e, 'POST', '/api/claim', { id: 'P', owner: 'w2', owner_thread: 'pt' });
  await act(e, 'P', 'submit', { commit: SHA }); await act(e, 'P', 'approve', { commit: SHA, actor: 'rev' });
  await ev(e, 'P', { kind: 'merged', approved_sha: SHA });                                                            // 父卡進 main：子卡變普通卡，放行有效
  k = await getCard(e, 'K');
  assert.deepEqual([k.stage, k.waiting_parent, k.approved_commit], ['selftesting', false, SHA]);
  // 父卡被放棄：子卡被擋住，通知管理者改父卡
  await claim(e, 'Q'); await claim(e, 'R', 'rt', { parent: 'Q' });
  await drop(e, 'Q');
  const r = await getCard(e, 'R');
  assert.deepEqual([r.parent_stage, r.parent_blocked], ['dropped', true]);
  const texts = []; await withFetch(() => flush(e), async (u, init) => { texts.push(JSON.parse(init.body).text); return new Response('{}'); });
  assert.match(texts.join('\n'), /boss[\s\S]*R[\s\S]*父卡 Q 被放棄/);
});

const raw = (e, method, path, body) => handle(new Request('http://x' + path, { method, headers: { authorization: 'Bearer t' }, body: JSON.stringify(body) }), e, {});

test('審計①：父卡不在清單上（沒領、已釋放、已放棄）→ desired 帶 parent 與 parent_status；已合併省略', async () => {
  const e = mkenv();
  await call(e, 'POST', '/api/cards', { id: 'P', title: 'P' });
  await claim(e, 'K', 'tk', { parent: 'P' });
  const get = async () => (await (await call(e, 'GET', '/api/queue/desired', undefined, 'h')).json()).find(x => x.branch === br('K'));
  assert.deepEqual([(await get()).parent, (await get()).parent_status], [br('P'), 'open']);   // 父卡沒領過
  await call(e, 'POST', '/api/claim', { id: 'P', owner: 'p', owner_thread: 'tp' });
  assert.equal((await get()).parent_status, undefined);                                        // 父卡在清單上：只有 parent
  await call(e, 'PATCH', '/api/cards/P', { status: 'open' });
  assert.equal((await get()).parent_status, 'open');                                           // 已釋放
  await drop(e, 'P');
  assert.equal((await get()).parent_status, 'dropped');
  await e.DB.prepare("UPDATE cards SET phase='merged' WHERE id='P'").run();
  assert.equal((await get()).parent, undefined);                                               // 已合併：省略
});

test('審計②：merged 事件一律接受（不看放行版本與重測計數）', async () => {
  const e = mkenv(); await approved(e, 'c1');
  const r = await (await ev(e, 'c1', { kind: 'merged', approved_sha: 'fffffff', retest: 9 })).json();
  assert.equal(r.applied, 1); assert.equal(await stageOf(e, 'c1'), 'merged');
});

test('審計③：併發送審／放行，輸家 409，放行版本永遠等於送審版本', async () => {
  const e = mkenv(); await claim(e, 'c1'); await act(e, 'c1', 'submit', { commit: SHA, actor: 'w' });
  const [a, b] = await Promise.all([act(e, 'c1', 'approve', { commit: SHA, actor: 'rev' }), act(e, 'c1', 'submit', { commit: SHA2, actor: 'w' })]);
  const c = await getCard(e, 'c1');
  if (c.approved_commit) assert.equal(c.approved_commit, c.submitted_commit);
  assert.ok([a.status, b.status].includes(200));
  // 讀到舊狀態後卡被別人改掉：寫入落空回 409
  await claim(e, 'c2'); await act(e, 'c2', 'submit', { commit: SHA, actor: 'w' });
  const real = e.DB.prepare.bind(e.DB);
  await real("UPDATE cards SET submitted_commit='eeeeeee' WHERE id='c2'").run();   // 模擬輸掉競態：讀完之後版本被換
  const r = await act(e, 'c2', 'approve', { commit: SHA, actor: 'rev' });
  assert.equal(r.status, 409);
  assert.equal((await getCard(e, 'c2')).approved_commit, null);
});

test('審計④：actor 必填，空白或 unknown 回 400', async () => {
  const e = mkenv(); await claim(e, 'c1');
  for (const actor of [undefined, '', '  ', 'unknown', 'Unknown']) assert.equal((await raw(e, 'POST', '/api/cards/c1/submit', { commit: SHA, actor })).status, 400);
  assert.equal((await raw(e, 'POST', '/api/cards', { id: 'z', title: 'z' })).status, 400);
  assert.equal((await raw(e, 'PATCH', '/api/cards/c1', { title: 'x' })).status, 400);
  assert.equal((await raw(e, 'POST', '/api/cards/c1/submit', { commit: SHA, actor: 'w' })).status, 200);
});

test('審計⑤：攢批預設 120 秒', async () => {
  const e = mkenv({ ROUTINE_ID: 'r', ROUTINE_KEY: 'k' }); let n = 0;
  await claim(e, 'c1', 'tc'); await approved(e, 'c9', 'tc9');
  await ev(e, 'c9', { kind: 'tested_red' });
  await withFetch(async () => {
    await e.DB.prepare('UPDATE outbox SET at=at-100').run();
    assert.equal((await flush(e)).waiting, true);
    await e.DB.prepare('UPDATE outbox SET at=at-30').run();
    assert.notEqual((await flush(e)).waiting, true);
  }, async () => { n++; return new Response('{}', { status: 200 }); });
  assert.equal(n, 1);
});

test('審計⑥：去重鍵用完整事件編號字串（"12" 與 "123" 不互相吃）', async () => {
  const e = mkenv(); await approved(e, 'c1');
  const r = await (await hook(e, [{ id: 'q:12', kind: 'tested_green', branch: br('c1') }, { id: 'q:123', kind: 'tested_green', branch: br('c1') }, { id: 'q:12', kind: 'tested_green', branch: br('c1') }])).json();
  assert.deepEqual([r.applied, r.duplicate], [2, 1]);
});

test('審計⑦：父卡釋放不通知管理者，放棄才通知', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' }); await claim(e, 'P', 'tp'); await claim(e, 'K', 'tk', { parent: 'P' });
  await call(e, 'PATCH', '/api/cards/P', { status: 'open' });
  assert.equal((await e.DB.prepare('SELECT * FROM outbox').all()).results.length, 0);
  await call(e, 'POST', '/api/claim', { id: 'P', owner: 'p', owner_thread: 'tp' });
  await drop(e, 'P');
  assert.equal((await e.DB.prepare("SELECT * FROM outbox WHERE kind='parent_blocked'").all()).results.length, 1);
});

test('審計 L4／L6：已合併、已放棄的卡不能再改欄位；匯入重複分支被略過', async () => {
  const e = mkenv(); await claim(e, 'c1'); await claim(e, 'c2');
  await drop(e, 'c1');
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { title: 'x' })).status, 409);
  await e.DB.prepare("UPDATE cards SET phase='merged' WHERE id='c2'").run();
  assert.equal((await call(e, 'PATCH', '/api/cards/c2', { priority: 1 })).status, 409);
  const r = await (await call(e, 'POST', '/api/import', { cards: [{ id: 'z', title: 'z', branch: br('c2') }] })).json();
  assert.equal(r.added, 0);
});

test('信箱：claim 每收件串一則、領了不重給、5 分鐘沒 ack 再給、ack 後不再給；不再主動 fire', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm', ROUTINE_ID: 'r', ROUTINE_KEY: 'k' });
  await approved(e, 'c1', 'th1'); await approved(e, 'c2', 'th1'); await approved(e, 'c3', 'th3');
  await ev(e, 'c1', { kind: 'tested_red', detail: 'x', approved_sha: SHA }); await ev(e, 'c2', { kind: 'conflict', approved_sha: SHA }); await ev(e, 'c3', { kind: 'halted' });
  let fired = 0;
  await withFetch(async () => { const { autoFlush } = await import('../src/queue.js'); await autoFlush(e); }, async () => { fired++; return new Response('{}'); });
  assert.equal(fired, 0);                                                         // 預設不打 routine fire
  assert.equal((await raw(e, 'POST', '/api/outbox/claim', {})).status, 400);        // actor 必填
  const a = await (await call(e, 'POST', '/api/outbox/claim', { actor: '傳令' })).json();
  const th1 = a.messages.find(m => m.to_thread === 'th1');
  assert.equal(th1.card_ids.length, 2); assert.match(th1.text, /c1[\s\S]*下一步[\s\S]*c2/);
  assert.ok(a.messages.some(m => m.to_thread === 'adm'));                           // halted 給管理者
  assert.equal((await (await call(e, 'POST', '/api/outbox/claim', { actor: '傳令' })).json()).messages.length, 0);   // 領了不重給
  await e.DB.prepare('UPDATE outbox SET claimed_at=claimed_at-400').run();           // 5 分鐘沒 ack：再給
  const b = await (await call(e, 'POST', '/api/outbox/claim', { actor: '傳令' })).json();
  assert.equal(b.messages.length, a.messages.length);
  const ack = await (await call(e, 'POST', '/api/outbox/ack', { actor: '傳令', ids: b.messages.flatMap(m => m.ids) })).json();
  assert.ok(ack.acked >= 3);
  await e.DB.prepare('UPDATE outbox SET claimed_at=claimed_at-400').run();
  assert.equal((await (await call(e, 'POST', '/api/outbox/claim', { actor: '傳令' })).json()).messages.length, 0);   // ack 後不再給
  assert.equal((await call(e, 'POST', '/api/outbox/claim', { actor: 'x' }, 'bad')).status, 401);
});

test('信箱限額：min_gap_s 同收件串攢批、max_messages 分輪、release 放回去不丟', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' });
  await approved(e, 'c1', 'th1'); await approved(e, 'c2', 'th2'); await approved(e, 'c3', 'th3');
  for (const c of ['c1', 'c2', 'c3']) await ev(e, c, { kind: 'conflict', approved_sha: SHA });
  const cl = b => call(e, 'POST', '/api/outbox/claim', { actor: '傳令', ...b }).then(r => r.json());
  const a = await cl({ max_messages: 2 });
  assert.equal(a.messages.length, 2);                                            // 一次最多 2 個收件串
  const left = await cl({});
  assert.equal(left.messages.length, 1);                                         // 沒輪到的立刻能領，不用等 5 分鐘
  // 發送失敗（被限額）：release 放回去
  const rel = await (await call(e, 'POST', '/api/outbox/release', { actor: '傳令', ids: left.messages.flatMap(m => m.ids) })).json();
  assert.equal(rel.released, left.messages[0].ids.length);
  assert.equal((await cl({})).messages.length, 1);                               // 馬上又領得到
  // ack 前兩個後，同一收件串的新通知在 min_gap_s 內不給（攢到下一輪）
  await call(e, 'POST', '/api/outbox/ack', { actor: '傳令', ids: a.messages.flatMap(m => m.ids) });
  const first = a.messages[0].to_thread;
  const card = { th1: 'c1', th2: 'c2', th3: 'c3' }[first];
  await ev(e, card, { kind: 'tested_red', detail: 'y', approved_sha: SHA });
  const gap = await cl({ min_gap_s: 600 });
  assert.ok(!gap.messages.some(m => m.to_thread === first));                     // 剛發過，先攢著
  const later = await cl({});
  assert.ok(later.messages.some(m => m.to_thread === first));   // 沒帶 min_gap_s 就給（攢的東西沒丟）
  // hold_s：release 後 hold 秒內不再給
  const r2 = await cl({});
  assert.equal(r2.messages.length, 0);
});

test('信箱 pending：HOOK／READ／TOKEN 都能查，不改狀態；領走後變 0', async () => {
  const e = mkenv({ READ_TOKEN: 'rd' }); await approved(e, 'c1', 'th1'); await ev(e, 'c1', { kind: 'tested_red', approved_sha: SHA });
  const g = tk => call(e, 'GET', '/api/outbox/pending', undefined, tk);
  assert.equal((await g('bad')).status, 401);
  for (const tk of ['h', 'rd', 't']) { const r = await (await g(tk)).json(); assert.deepEqual([r.pending, r.latest_id > 0], [1, true]); }
  assert.equal((await (await g('h')).json()).pending, 1);                         // 查詢不改狀態
  assert.equal((await call(e, 'POST', '/api/outbox/pending', {}, 'h')).status, 405);
  await call(e, 'POST', '/api/outbox/claim', { actor: '傳令' });
  assert.deepEqual(await (await g('h')).json(), { pending: 0, latest_id: 0 });
});

test('放棄防呆：沒送審直接放棄；送審過要帶 superseded_by 或 discard_reason，否則 400；放棄待查不算已放棄', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' });
  await claim(e, 'n1');                                                                    // 領了沒送審：也要先查（舊分支補的卡、領了的卡都一樣）
  assert.equal((await call(e, 'PATCH', '/api/cards/n1', { status: 'dropped' })).status, 400);
  assert.equal((await (await call(e, 'PATCH', '/api/cards/n1', { status: 'dropped', discard_reason: '開錯' })).json()).stage, 'dropping');
  // 例外：建卡人（actor 相同）在卡還是 new、10 分鐘內放棄 → 直接放棄；別人、或超過 10 分鐘都要查
  await call(e, 'POST', '/api/cards', { id: 'x1', title: 'x1', actor: 'maker' });
  assert.equal((await call(e, 'PATCH', '/api/cards/x1', { status: 'dropped', actor: 'other' })).status, 400);
  await call(e, 'POST', '/api/cards', { id: 'x2', title: 'x2', actor: 'maker' }); await e.DB.prepare("UPDATE cards SET created_at=created_at-700 WHERE id='x2'").run();
  assert.equal((await call(e, 'PATCH', '/api/cards/x2', { status: 'dropped', actor: 'maker' })).status, 400);
  assert.equal((await (await call(e, 'PATCH', '/api/cards/x1', { status: 'dropped', actor: 'maker' })).json()).stage, 'dropped');
  // 沒人領、branch 欄空的卡：進待查，用 dev/<卡號小寫> 當分支名送查；待查中不能被領走
  await call(e, 'POST', '/api/cards', { id: 'Old-1', title: 'old', actor: 'maker' }); await e.DB.prepare("UPDATE cards SET created_at=created_at-700 WHERE id='Old-1'").run();
  await call(e, 'PATCH', '/api/cards/Old-1', { status: 'dropped', discard_reason: '舊的' });
  const dc = (await (await call(e, 'GET', '/api/queue/desired?with=drop_checks', undefined, 'h')).json()).drop_checks.find(x => x.card === 'Old-1');
  assert.equal(dc.branch, 'dev/old-1');
  assert.equal((await call(e, 'POST', '/api/claim', { id: 'Old-1', owner: 'z' })).status, 409);
  assert.equal((await (await ev(e, 'Old-1', { kind: 'drop_checked', unmerged: false, covered: true, dropping_at: dc.dropping_at })).json()).applied, 1);   // 分支不存在：自然放行
  assert.equal(await stageOf(e, 'Old-1'), 'dropped');
  await claim(e, 'c1', 'th1'); await act(e, 'c1', 'submit', { commit: SHA });
  const r = await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped' });
  assert.equal(r.status, 400); assert.match((await r.json()).error, /superseded_by|discard_reason/);
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', discard_reason: '  ' })).status, 400);
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'nope' })).status, 409);
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c1' })).status, 409);
  await claim(e, 'c2', 'th2');
  const d = await (await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c2' })).json();
  assert.equal(d.stage, 'dropping'); assert.equal(d.dropped.superseded_by, 'c2'); assert.notEqual(d.status, 'dropped');
  assert.equal((await act(e, 'c1', 'submit', { commit: SHA2 })).status, 409);               // 待查中不能動
  const list = await (await call(e, 'GET', '/api/queue/desired', undefined, 'h')).json();
  assert.ok(!list.some(x => x.branch === br('c1')));                                         // 不再被測或合
  const v2 = await (await call(e, 'GET', '/api/queue/desired?with=drop_checks', undefined, 'h')).json();
  assert.deepEqual(v2.drop_checks.filter(x => x.card === 'c1').map(({ dropping_at, ...r }) => r), [{ card: 'c1', branch: br('c1'), superseded_by_branch: br('c2') }]);
  assert.ok(v2.cards.some(x => x.branch === br('c2')));
});

test('放棄防呆：drop_checked——covered 才放棄，否則回原階段並通知；只帶 discard_reason 一律放棄，有漏的通知總指揮', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' });
  await claim(e, 'k', 'thk', { parent: 'c1' }).catch(() => {});
  await claim(e, 'c1', 'th1'); await claim(e, 'c2', 'th2'); await act(e, 'c1', 'submit', { commit: SHA });
  await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c2' });
  const bad = await (await ev(e, 'c1', { kind: 'drop_checked', unmerged: true, covered: false, missing_count: 2, missing_files: ['a.gd', 'b.gd'] })).json();
  assert.equal(bad.applied, 1);
  const c1 = await getCard(e, 'c1');
  assert.notEqual(c1.stage, 'dropped'); assert.equal(c1.stage, 'reviewing'); assert.equal(c1.dropped, null);   // 回到原階段
  const box = (await e.DB.prepare("SELECT * FROM outbox WHERE kind='drop_rejected'").all()).results;
  assert.ok(box.some(b => b.owner_thread === 'th1') && box.some(b => b.owner_thread === 'adm') && /a\.gd、b\.gd/.test(box[0].detail));
  await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c2' });
  await ev(e, 'c1', { kind: 'drop_checked', unmerged: true, covered: true });
  const ok = await getCard(e, 'c1'); assert.equal(ok.stage, 'dropped'); assert.equal(ok.dropped.unmerged, true);
  // 過期／重複的查驗結果不動卡
  assert.equal((await (await ev(e, 'c1', { kind: 'drop_checked', unmerged: true, covered: false })).json()).stale, 1);
  assert.equal((await getCard(e, 'c1')).stage, 'dropped');
  // 只帶 discard_reason：一律放棄；有漏的通知總指揮
  await claim(e, 'd1', 'thd'); await act(e, 'd1', 'submit', { commit: SHA });
  await call(e, 'PATCH', '/api/cards/d1', { status: 'dropped', discard_reason: '方向作廢' });
  await ev(e, 'd1', { kind: 'drop_checked', unmerged: true, covered: false, missing_count: 3, missing_files: ['x'] });
  assert.equal(await stageOf(e, 'd1'), 'dropped');
  assert.equal((await e.DB.prepare("SELECT * FROM outbox WHERE kind='drop_discarded'").all()).results.length, 1);
  // 沒漏：不通知
  await claim(e, 'd2', 'thd2'); await act(e, 'd2', 'submit', { commit: SHA });
  await call(e, 'PATCH', '/api/cards/d2', { status: 'dropped', discard_reason: 'x' });
  await ev(e, 'd2', { kind: 'drop_checked', unmerged: false, covered: true });
  assert.equal((await e.DB.prepare("SELECT * FROM outbox WHERE kind='drop_discarded'").all()).results.length, 1);
});

test('放棄防呆補強：待查中擋改欄位；晚到的上一輪查驗結果不套到這一輪', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' });
  await claim(e, 'c1', 'th1'); await claim(e, 'c2', 'th2'); await claim(e, 'c3', 'th3'); await act(e, 'c1', 'submit', { commit: SHA });
  await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c2' });
  for (const b of [{ priority: 1 }, { title: 'x' }, { parent: 'c3' }]) assert.equal((await call(e, 'PATCH', '/api/cards/c1', b)).status, 409);
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c2' })).status, 200);   // 重送放棄請求不算錯
  const first = (await (await call(e, 'GET', '/api/queue/desired?with=drop_checks', undefined, 'h')).json()).drop_checks[0];
  await ev(e, 'c1', { kind: 'drop_checked', unmerged: true, covered: false, dropping_at: first.dropping_at });   // 第一輪被駁回
  assert.equal(await stageOf(e, 'c1'), 'reviewing');
  await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c3' });                            // 第二輪改別張接手
  const late = await (await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true, dropping_at: first.dropping_at - 1 })).json();   // 上一輪晚到
  assert.equal(late.stale, 1); assert.equal(await stageOf(e, 'c1'), 'dropping');
  const late2 = await (await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true, target: br('c2') })).json();   // 沒帶 dropping_at 但 target 不是現在的接手卡
  assert.equal(late2.stale, 1); assert.equal(await stageOf(e, 'c1'), 'dropping');
  const cur = (await (await call(e, 'GET', '/api/queue/desired?with=drop_checks', undefined, 'h')).json()).drop_checks[0];
  await ev(e, 'c1', { kind: 'drop_checked', unmerged: true, covered: true, target: br('c3'), dropping_at: cur.dropping_at });
  assert.equal(await stageOf(e, 'c1'), 'dropped');
});

test('放棄防呆：接手分支還不存在時 target 是 main，仍算對得上這一輪', async () => {
  const e = mkenv(); await claim(e, 'c1', 'th1'); await call(e, 'POST', '/api/cards', { id: 'c9', title: 'c9' });
  await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c9' });
  const r = await (await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true, target: 'main' })).json();
  assert.equal(r.applied, 1); assert.equal(await stageOf(e, 'c1'), 'dropped');
});

test('desired 唯讀：TOKEN 也能讀，壞權杖 401', async () => {
  const e = mkenv({ READ_TOKEN: 'rd' });
  for (const tk of ['t', 'h', 'rd']) assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, tk)).status, 200);
  assert.equal((await call(e, 'GET', '/api/queue/desired', undefined, 'bad')).status, 401);
});

test('放棄查驗事件的處理結果記進 hook_log（過期與收下都留原文）', async () => {
  const e = mkenv(); await claim(e, 'c1', 'th1'); await act(e, 'c1', 'submit', { commit: SHA });
  await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true });   // 不在待查：過期
  await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', discard_reason: 'x' });
  await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true });
  const rows = (await e.DB.prepare('SELECT outcome FROM hook_log ORDER BY n').all()).results.map(r => r.outcome);
  assert.match(rows[0], /過期/); assert.equal(rows[1], '收下');
});

test('hook 一批 50 筆：D1 往返只有個位數、同批多事件依序生效、整批重送不重複', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' });
  for (let i = 0; i < 50; i++) await approved(e, `b${i}`, `th${i}`);
  // 計 D1 往返：prepare 出來的 run／all／first 各算一次，batch 算一次
  let trips = 0; const real = e.DB;
  e.DB = { prepare: sql => { const st = real.prepare(sql); const w = { bind: (...a) => { st.bind(...a); return w; }, _run: () => st._run(), run: () => { trips++; return st.run(); }, all: () => { trips++; return st.all(); }, first: () => { trips++; return st.first(); } }; return w; }, batch: l => { trips++; return real.batch(l.map(x => x)); } };
  const events = [];
  for (let i = 0; i < 50; i++) events.push({ id: `bt:${i}:1`, branch: br(`b${i}`), kind: 'tested_red', approved_sha: SHA, commit: SHA, detail: 'x', failed_items: [{ item_id: 'f' + i }] }, { id: `bt:${i}:2`, branch: br(`b${i}`), kind: 'rejected', approved_sha: SHA, commit: SHA });
  const r = await (await hook(e, events)).json();
  assert.deepEqual([r.applied, r.processed, r.remaining], [100, 100, 0]);
  assert.ok(trips <= 10, `D1 往返 ${trips} 次`);
  const c = await getCard(e, 'b7');
  assert.equal(c.stage, 'rejected'); assert.equal(c.rejected.failed_items[0].item_id, 'f7');   // 同批後面的事件沒帶紅項，沿用前面的
  const again = await (await hook(e, events)).json();
  assert.deepEqual([again.applied, again.duplicate], [0, 100]);                                  // 整批重送：全部判重複，不重複寫
  assert.equal((await e.DB.prepare("SELECT count(*) n FROM outbox WHERE card_id='b7' AND kind='tested_red'").first()).n, 1);
  const over = await (await hook(e, Array.from({ length: 205 }, (_, i) => ({ id: `ov:${i}`, kind: 'queue_error' })))).json();
  assert.deepEqual([over.processed, over.remaining], [200, 5]);                                  // 超過 200 筆回報還剩幾筆讓對方續傳
});

test('hook 併發：同一批事件同時送兩次，通知與紀錄只寫一份，後到的判重複', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' });
  await approved(e, 'p1', 'th1'); await approved(e, 'p2', 'th2');
  const events = [{ id: 'cc:1', branch: br('p1'), kind: 'tested_red', approved_sha: SHA, commit: SHA }, { id: 'cc:2', branch: br('p2'), kind: 'conflict', approved_sha: SHA, commit: SHA }];
  const [a, b] = await Promise.all([hook(e, events).then(r => r.json()), hook(e, events).then(r => r.json())]);
  assert.equal(a.applied + b.applied, 2); assert.equal(a.duplicate + b.duplicate, 2);
  assert.equal((await e.DB.prepare("SELECT count(*) n FROM outbox WHERE kind IN ('tested_red','conflict')").first()).n, 2);
  assert.equal((await e.DB.prepare("SELECT count(*) n FROM events WHERE note LIKE 'tested_red%'").first()).n, 1);
});

test('放棄查驗：送出前卡被人取消放棄，結果不覆蓋、不發通知', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' }); await claim(e, 'c1', 'th1'); await claim(e, 'k', 'thk', { parent: 'c1' });
  await act(e, 'c1', 'submit', { commit: SHA });
  await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', discard_reason: 'x' });
  const real = e.DB.batch.bind(e.DB);
  e.DB.batch = async l => { await e.DB.prepare("UPDATE cards SET dropping_at=NULL,discard_reason=NULL WHERE id='c1'").run(); return real(l); };   // 預載之後、送出之前，有人把放棄取消了
  await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true });
  assert.notEqual(await stageOf(e, 'c1'), 'dropped');
  assert.equal((await e.DB.prepare("SELECT count(*) n FROM outbox WHERE kind='parent_blocked'").first()).n, 0);
});

test('放棄待查中再放棄一次：換原因或接手卡會覆蓋並開新一輪；內容一樣當重送；缺欄位的查驗結果不放行', async () => {
  const e = mkenv(); await claim(e, 'c1', 'th1'); await claim(e, 'c2', 'th2'); await act(e, 'c1', 'submit', { commit: SHA });
  const d1 = await (await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c2' })).json();
  const same = await (await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c2' })).json();
  assert.equal(same.dropping_at, d1.dropping_at);                                              // 一樣：重送
  const d2 = await (await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', discard_reason: '真的不要' })).json();
  assert.equal(d2.stage, 'dropping'); assert.equal(d2.dropped.superseded_by, null); assert.equal(d2.dropped.discard_reason, '真的不要');
  assert.ok(d2.dropping_at > d1.dropping_at);                                                  // 新一輪
  assert.equal((await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', superseded_by: 'c1' })).status, 409);   // 覆蓋也照樣驗證
  const bad = await (await ev(e, 'c1', { kind: 'drop_checked', detail: '有 None 個檔不在 None', dropping_at: d2.dropping_at })).json();   // 缺 unmerged／covered
  assert.equal(bad.stale, 1); assert.equal(await stageOf(e, 'c1'), 'dropping');
  assert.equal((await (await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true, dropping_at: d1.dropping_at })).json()).stale, 1);   // 上一輪的
  await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true, dropping_at: d2.dropping_at });
  assert.equal(await stageOf(e, 'c1'), 'dropped');
});

test('放棄查驗事件沒帶 card、card 的 branch 欄是空的：用推導的分支名認得', async () => {
  const e = mkenv(); await call(e, 'POST', '/api/cards', { id: 'Old-2', title: 'o', actor: 'maker' }); await e.DB.prepare("UPDATE cards SET created_at=created_at-700 WHERE id='Old-2'").run();
  await call(e, 'PATCH', '/api/cards/Old-2', { status: 'dropped', discard_reason: 'x' });
  const r = await (await hook(e, [{ id: 'dn:1', kind: 'drop_checked', branch: 'dev/old-2', unmerged: false, covered: true }])).json();
  assert.equal(r.applied, 1); assert.equal(await stageOf(e, 'Old-2'), 'dropped');
});

test('放棄待查太久沒結果通知管理者一次；舊一輪的查驗結果不會套到新一輪', async () => {
  const e = mkenv({ ADMIN_THREADS: 'adm' }); await claim(e, 'c1', 'th1'); await act(e, 'c1', 'submit', { commit: SHA });
  await call(e, 'PATCH', '/api/cards/c1', { status: 'dropped', discard_reason: 'x' });
  assert.equal((await stuckCheck(e)).notified, 0);
  await e.DB.prepare('UPDATE cards SET dropping_at=dropping_at-3600').run();
  assert.equal((await stuckCheck(e)).notified, 1);
  assert.equal((await stuckCheck(e)).notified, 0);                                         // 同一輪不重複
  const old = (await getCard(e, 'c1')).dropping_at;
  // 預載後、送出前有人開了新一輪：UPDATE 條件帶 dropping_at，舊一輪的結果落空
  const real = e.DB.batch.bind(e.DB);
  e.DB.batch = async l => { await e.DB.prepare("UPDATE cards SET dropping_at=? WHERE id='c1'").bind(old + 100).run(); return real(l); };
  await ev(e, 'c1', { kind: 'drop_checked', unmerged: false, covered: true, dropping_at: old });
  assert.equal(await stageOf(e, 'c1'), 'dropping');
});

test('desired：一次讀卡，D1 往返只有一次（卡再多也不慢）', async () => {
  const e = mkenv();
  for (let i = 0; i < 40; i++) await claim(e, `d${i}`, `t${i}`, i ? { parent: `d${i - 1}` } : {});
  let trips = 0; const real = e.DB;
  e.DB = { prepare: sql => { const st = real.prepare(sql); const w = { bind: (...a) => { st.bind(...a); return w; }, all: () => { trips++; return st.all(); }, first: () => { trips++; return st.first(); }, run: () => { trips++; return st.run(); } }; return w; }, batch: l => real.batch(l) };
  const r = await (await call(e, 'GET', '/api/queue/desired?with=drop_checks', undefined, 'h')).json();
  assert.equal(r.cards.length, 40); assert.equal(trips, 1);
});

test('superseded 的現版跟放行版本是同一個提交：不採信，送審與放行保留', async () => {
  const e = mkenv(); await approved(e, 'c1', 'th1');
  const r = await (await ev(e, 'c1', { kind: 'superseded', commit: SHA, approved_sha: SHA, detail: '放行版本 abc1234 的內容跟目前 abc1234 不同' })).json();
  assert.equal(r.stale, 1); assert.equal(await stageOf(e, 'c1'), 'selftesting');
  const r2 = await (await ev(e, 'c1', { kind: 'superseded', commit: SHA2, approved_sha: SHA })).json();   // 真的換了版本：照樣失效
  assert.equal(r2.applied, 1); assert.equal(await stageOf(e, 'c1'), 'developing');
});
