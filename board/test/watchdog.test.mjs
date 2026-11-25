import test from 'node:test';
import assert from 'node:assert/strict';
import { handle } from '../src/worker.js';
import { watchdog, reviewerFor, cleanRoster } from '../src/watchdog.js';
import { fakeD1 } from './faked1.mjs';

const mkenv = (extra = {}) => ({ DB: fakeD1(), TOKEN: 't', HOOK_TOKEN: 'h', ...extra });
const call = (e, method, path, body, token = 't') =>
  handle(new Request('http://x' + path, { method, headers: { authorization: `Bearer ${token}` }, body: body ? JSON.stringify(method === 'GET' ? body : { actor: 'tester', ...body }) : undefined }), e);
const SHA = 'abc1234'.padEnd(40, '0');
const T = 2000000000;
const ROSTER = {
  commander: 'cmd',
  reviewers: [{ name: '審查一', thread: 'r1', letters: 'A-E' }, { name: '審查二', thread: 'r2', letters: 'F-N' }, { name: '審查三', thread: 'r3', letters: 'O-Z' }, { name: '審查四', thread: 'r4', letters: '*', role: '補位' }],
  devs: [{ name: '甲', thread: 'd1' }, { name: '乙', thread: 'd2' }],
};
const outbox = async (e, kind) => (await e.DB.prepare('SELECT owner_thread, card_id, detail FROM outbox WHERE kind=? ORDER BY n').bind(kind).all()).results;

test('串名冊：POST 驗證、GET 讀回、整份取代', async () => {
  const e = mkenv();
  assert.equal(await (await call(e, 'GET', '/api/roster')).json(), null);
  assert.equal((await call(e, 'POST', '/api/roster', { roster: ROSTER }, 'bad')).status, 401);
  assert.equal((await call(e, 'POST', '/api/roster', { actor: 'unknown', roster: ROSTER })).status, 400);
  assert.equal((await call(e, 'POST', '/api/roster', { roster: { reviewers: [] } })).status, 400);
  const r = await call(e, 'POST', '/api/roster', { roster: ROSTER });
  assert.equal(r.status, 200);
  assert.deepEqual(await r.json(), { ok: true, reviewers: 4, devs: 2, messengers: 0 });
  const got = await (await call(e, 'GET', '/api/roster')).json();
  assert.equal(got.commander, 'cmd');
  assert.deepEqual(got.reviewers.map(x => x.thread), ['r1', 'r2', 'r3', 'r4']);
  await call(e, 'POST', '/api/roster', { roster: { ...ROSTER, reviewers: [ROSTER.reviewers[3]] } });          // 整份取代
  assert.equal((await (await call(e, 'GET', '/api/roster')).json()).reviewers.length, 1);
  const [x, err] = cleanRoster({ reviewers: [{ name: 'x' }] });                                               // reviewers 每位要有 thread
  assert.equal(x, null); assert.ok(err);
});

test('reviewerFor：依卡號首字母分審查串，沒有範圍符合就補位', () => {
  const ro = ROSTER;
  assert.equal(reviewerFor(ro, 'ART-01').thread, 'r1');
  assert.equal(reviewerFor(ro, 'ev-3').thread, 'r1');          // 小寫也行（E 在 A-E）
  assert.equal(reviewerFor(ro, 'fx-3').thread, 'r2');
  assert.equal(reviewerFor(ro, 'NS-F09').thread, 'r2');
  assert.equal(reviewerFor(ro, 'OB-02').thread, 'r3');
  assert.equal(reviewerFor(ro, 'ZZ').thread, 'r3');
  assert.equal(reviewerFor(ro, '9abc').thread, 'r4');          // 不是字母：補位
  assert.equal(reviewerFor({ reviewers: [{ thread: 'only', letters: 'A-C' }] }, 'X1').thread, 'only');   // 沒補位也沒符合：回第一位，不丟
});

test('送審通知：有名冊就叫名冊裡的全部審查串，沒有名冊用環境變數', async () => {
  const e = mkenv({ REVIEW_THREADS: 'env1,env2' });
  await call(e, 'POST', '/api/cards', { id: 'A1', title: 'a1' }); await call(e, 'POST', '/api/claim', { id: 'A1', owner: 'w', owner_thread: 'dd' });
  await call(e, 'POST', '/api/cards/A1/submit', { commit: SHA, actor: 'w' });
  assert.deepEqual((await outbox(e, 'review_requested')).map(r => r.owner_thread).sort(), ['env1', 'env2']);
  await call(e, 'POST', '/api/roster', { roster: ROSTER });
  await call(e, 'POST', '/api/cards', { id: 'B1', title: 'b1' }); await call(e, 'POST', '/api/claim', { id: 'B1', owner: 'w', owner_thread: 'dd' });
  await call(e, 'POST', '/api/cards/B1/submit', { commit: SHA, actor: 'w' });
  const b = (await e.DB.prepare("SELECT owner_thread FROM outbox WHERE kind='review_requested' AND card_id='B1'").all()).results.map(r => r.owner_thread).sort();
  assert.deepEqual(b, ['r1', 'r2', 'r3', 'r4']);
});

test('看門狗：送審 15 分鐘叫該審的審查串、30 分鐘再叫審查四，同一版本各一次', async () => {
  const e = mkenv();
  await call(e, 'POST', '/api/roster', { roster: ROSTER });
  await call(e, 'POST', '/api/cards', { id: 'F1', title: 'f1' }); await call(e, 'POST', '/api/claim', { id: 'F1', owner: 'w', owner_thread: 'dd' });
  await call(e, 'POST', '/api/cards/F1/submit', { commit: SHA, actor: 'w' });
  await e.DB.prepare("UPDATE cards SET submitted_at=? WHERE id='F1'").bind(T).run();
  assert.deepEqual(await watchdog(e, T + 10 * 60), { review: 0, dev: 0, nocard: 0 });                  // 10 分鐘：還早
  const r15 = await watchdog(e, T + 16 * 60);
  assert.equal(r15.review, 1);
  assert.deepEqual((await outbox(e, 'watchdog_review')).map(r => r.owner_thread), ['r2']);             // F 歸審查二
  assert.equal((await watchdog(e, T + 20 * 60)).review, 0);                                            // 同一版本不重叫
  assert.equal((await watchdog(e, T + 31 * 60)).review, 1);
  assert.deepEqual((await outbox(e, 'watchdog_review')).map(r => r.owner_thread), ['r2', 'r4']);       // 30 分鐘補位叫審查四
  assert.equal((await watchdog(e, T + 50 * 60)).review, 0);
  // 已放行的不叫
  await call(e, 'POST', '/api/cards', { id: 'G1', title: 'g1' }); await call(e, 'POST', '/api/claim', { id: 'G1', owner: 'w', owner_thread: 'dd' });
  await call(e, 'POST', '/api/cards/G1/submit', { commit: SHA, actor: 'w' });
  await e.DB.prepare("UPDATE cards SET submitted_at=? WHERE id='G1'").bind(T).run();
  await call(e, 'POST', '/api/cards/G1/approve', { commit: SHA, actor: 'rev' });
  assert.equal((await watchdog(e, T + 60 * 60)).review, 0);
});

test('看門狗：開發串沒卡在做 15 分鐘 → 叫它領下一張；沒卡可領 → 叫總指揮補卡；30 分鐘內不重叫', async () => {
  const e = mkenv();
  await call(e, 'POST', '/api/roster', { roster: ROSTER });
  await call(e, 'POST', '/api/cards', { id: 'N1', title: '下一張', priority: 5 });
  await call(e, 'POST', '/api/cards', { id: 'N2', title: '第二張', priority: 50 });
  assert.deepEqual(await watchdog(e, T), { review: 0, dev: 0, nocard: 0 });                            // 第一次看到：從現在開始算
  assert.deepEqual(await watchdog(e, T + 5 * 60), { review: 0, dev: 0, nocard: 0 });
  const r = await watchdog(e, T + 16 * 60);
  assert.equal(r.dev, 2);
  const w = await outbox(e, 'watchdog_dev');
  assert.deepEqual(w.map(x => [x.owner_thread, x.card_id]).sort(), [['d1', 'N1'], ['d2', 'N2']]);      // 兩個閒置的各領不同的卡，優先序小的先
  assert.match(w[0].detail, /請領下一張/);
  assert.deepEqual(await watchdog(e, T + 20 * 60), { review: 0, dev: 0, nocard: 0 });                  // 30 分鐘內不重叫
  // 30 分鐘後仍沒領、卡被領光 → 沒卡可領：叫總指揮
  await e.DB.prepare("UPDATE cards SET status='claimed', owner_thread='other', updated_at=? WHERE id IN ('N1','N2')").bind(T + 30 * 60).run();
  const r2 = await watchdog(e, T + 50 * 60);
  assert.equal(r2.nocard, 2);
  assert.deepEqual((await outbox(e, 'watchdog_nocard')).map(x => x.owner_thread), ['cmd', 'cmd']);
});

test('看門狗：開發串手上有在做的卡就不叫；沒有名冊就跳過', async () => {
  const e = mkenv();
  assert.deepEqual(await watchdog(e, T), { skipped: 'no roster' });
  await call(e, 'POST', '/api/roster', { roster: { ...ROSTER, devs: [ROSTER.devs[0]] } });
  await call(e, 'POST', '/api/cards', { id: 'X1', title: 'x1' }); await call(e, 'POST', '/api/claim', { id: 'X1', owner: 'w', owner_thread: 'd1' });   // d1 手上有一張開發中
  await call(e, 'POST', '/api/cards', { id: 'X2', title: 'x2' });
  await watchdog(e, T);
  assert.deepEqual(await watchdog(e, T + 3600), { review: 0, dev: 0, nocard: 0 });
  assert.equal((await outbox(e, 'watchdog_dev')).length, 0);
});
