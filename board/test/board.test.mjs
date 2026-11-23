import test from 'node:test';
import assert from 'node:assert/strict';
import { handle } from '../src/worker.js';

import { fakeD1 } from './faked1.mjs';
const env = () => ({ DB: fakeD1(), TOKEN: 't' });
const call = (e, method, path, body, token = 't') =>
  handle(new Request('http://x' + path, { method, headers: { authorization: `Bearer ${token}` }, body: body ? JSON.stringify(body) : undefined }), e);
const mk = (e, id, priority = 100, area = '') => call(e, 'POST', '/api/cards', { id, title: id, priority, area });

test('沒權杖不能寫，能讀', async () => {
  const e = env();
  assert.equal((await call(e, 'POST', '/api/cards', { id: 'a', title: 'a' }, 'bad')).status, 401);
  assert.equal((await call(e, 'GET', '/api/cards')).status, 200);
});

test('建立、重複 id 回 409', async () => {
  const e = env();
  assert.equal((await mk(e, 'c1')).status, 201);
  assert.equal((await mk(e, 'c1')).status, 409);
});

test('依優先序認領，同一張只有一人拿到', async () => {
  const e = env();
  await mk(e, 'low', 200); await mk(e, 'high', 1);
  const a = await (await call(e, 'POST', '/api/claim', { owner: 'A' })).json();
  assert.equal(a.id, 'high');
  const b = await (await call(e, 'POST', '/api/claim', { owner: 'B' })).json();
  assert.equal(b.id, 'low');
  assert.equal((await call(e, 'POST', '/api/claim', { owner: 'C' })).status, 404);
});

test('指定 id 搶同一張：第二個 409', async () => {
  const e = env();
  await mk(e, 'x');
  const rs = await Promise.all([call(e, 'POST', '/api/claim', { id: 'x', owner: 'A' }), call(e, 'POST', '/api/claim', { id: 'x', owner: 'B' })]);
  assert.deepEqual(rs.map(r => r.status).sort(), [200, 409]);
});

test('依區域認領', async () => {
  const e = env();
  await mk(e, 'm1', 1, 'map'); await mk(e, 'u1', 2, 'ui');
  const r = await (await call(e, 'POST', '/api/claim', { owner: 'A', area: 'ui' })).json();
  assert.equal(r.id, 'u1');
});

test('卡片只有 open／claimed／dropped，中段狀態不能手改', async () => {
  const e = env();
  await mk(e, 'c');
  assert.equal((await call(e, 'PATCH', '/api/cards/c', { status: 'merged' })).status, 400);
  assert.equal((await call(e, 'PATCH', '/api/cards/c', { status: 'rejected' })).status, 400);
  await call(e, 'POST', '/api/claim', { id: 'c', owner: 'A', branch: 'dev/c' });
  assert.equal((await call(e, 'PATCH', '/api/cards/c', { status: 'dropped' })).status, 200);
  assert.equal((await call(e, 'PATCH', '/api/cards/c', { status: 'claimed' })).status, 409);
  assert.equal((await call(e, 'PATCH', '/api/cards/c', { status: 'open' })).status, 200);
});

test('釋放回 open 會清掉負責人與分支', async () => {
  const e = env();
  await mk(e, 'c'); await call(e, 'POST', '/api/claim', { id: 'c', owner: 'A', branch: 'dev/c' });
  await call(e, 'PATCH', '/api/cards/c', { status: 'open' });
  const c = await (await call(e, 'GET', '/api/cards/c')).json();
  assert.deepEqual([c.owner, c.branch, c.phase], [null, null, null]);
});

test('退回後換新分支重做：phase 清掉', async () => {
  const e = env();
  await mk(e, 'c'); await call(e, 'POST', '/api/claim', { id: 'c', owner: 'A', branch: 'dev/c1' });
  await e.DB.prepare("UPDATE cards SET phase='rejected' WHERE id='c'").run();
  await call(e, 'PATCH', '/api/cards/c', { branch: 'dev/c2' });
  const c = await (await call(e, 'GET', '/api/cards/c')).json();
  assert.deepEqual([c.branch, c.phase, c.status], ['dev/c2', null, 'claimed']);
});

test('通用匯入：略過重複與不合法', async () => {
  const e = env();
  const r = await (await call(e, 'POST', '/api/import', { cards: [{ id: 'a', title: 'A', status: 'claimed' }, { id: 'a', title: 'A2' }, { title: 'x' }, { id: 'b', title: 'B', status: 'zzz' }] })).json();
  assert.deepEqual(r, { added: 1, skipped: 3 });
});
