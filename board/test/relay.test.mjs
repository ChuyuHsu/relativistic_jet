import test from 'node:test';
import assert from 'node:assert/strict';
import { handle } from '../src/worker.js';
import { allowedUpstream } from '../src/relay.js';
import { fakeD1 } from './faked1.mjs';

const env = () => ({ DB: fakeD1(), REGISTER_TOKEN: 'reg' });
const req = (method, path, body, token) =>
  new Request('http://x' + path, { method, headers: token ? { authorization: `Bearer ${token}` } : {}, body });
const reg = (e, name, url, token = 'reg') => handle(req('PUT', `/relay/${name}`, url, token), e);

test('白名單：只收 https 與指定後綴', () => {
  const s = ['trycloudflare.com'];
  assert.equal(allowedUpstream('https://a-b.trycloudflare.com', s), 'https://a-b.trycloudflare.com');
  assert.equal(allowedUpstream('http://a.trycloudflare.com', s), null);
  assert.equal(allowedUpstream('https://evil.com', s), null);
  assert.equal(allowedUpstream('https://trycloudflare.com.evil.com', s), null);
  assert.equal(allowedUpstream('https://x.trycloudflare.com:8443', s), null);
  assert.equal(allowedUpstream('https://u:p@x.trycloudflare.com', s), null);
});

test('登記要權杖、網址要合法', async () => {
  const e = env();
  assert.equal((await reg(e, 'q', 'https://a.trycloudflare.com', 'bad')).status, 401);
  assert.equal((await reg(e, 'q', 'https://evil.com')).status, 400);
  assert.equal((await reg(e, 'q', 'https://a.trycloudflare.com')).status, 200);
});

test('沒登記回 503，_status 不洩漏網址', async () => {
  const e = env();
  assert.equal((await handle(req('GET', '/relay/q/x'), e)).status, 503);
  assert.deepEqual(await (await handle(req('GET', '/relay/q/_status'), e)).json(), { registered: false, updated_at: null });
  await reg(e, 'q', 'https://a.trycloudflare.com');
  const st = await (await handle(req('GET', '/relay/q/_status'), e)).json();
  assert.equal(st.registered, true);
  assert.ok(!JSON.stringify(st).includes('trycloudflare'));
});

test('原樣轉送方法、路徑、查詢、內容與 Authorization', async () => {
  const e = env(); await reg(e, 'q', 'https://a.trycloudflare.com');
  const real = globalThis.fetch; let seen;
  globalThis.fetch = async (u, init) => { seen = { u, init, body: await new Response(init.body).text() }; return new Response('pong', { status: 201, headers: { 'x-up': '1' } }); };
  try {
    const r = await handle(new Request('http://x/relay/q/api/a/b?x=1&y=2', { method: 'POST', headers: { authorization: 'Bearer k', 'content-type': 'text/plain' }, body: 'hello' }), e);
    assert.equal(r.status, 201); assert.equal(await r.text(), 'pong'); assert.equal(r.headers.get('x-up'), '1');
    assert.equal(seen.u, 'https://a.trycloudflare.com/api/a/b?x=1&y=2');
    assert.equal(seen.init.method, 'POST'); assert.equal(seen.body, 'hello');
    assert.equal(seen.init.headers.get('authorization'), 'Bearer k');
  } finally { globalThis.fetch = real; }
});

test('上游連不上回 503', async () => {
  const e = env(); await reg(e, 'q', 'https://a.trycloudflare.com');
  const real = globalThis.fetch; globalThis.fetch = async () => { throw new Error('down'); };
  try { assert.equal((await handle(req('GET', '/relay/q/x'), e)).status, 503); } finally { globalThis.fetch = real; }
});

test('登記也收 JSON {url}', async () => {
  const e = env();
  assert.equal((await reg(e, 'q', JSON.stringify({ url: 'https://a.trycloudflare.com' }))).status, 200);
  assert.equal((await handle(req('GET', '/relay/q/_status'), e).then(r => r.json())).registered, true);
  assert.equal((await reg(e, 'r', JSON.stringify({ url: 'https://evil.com' }))).status, 400);
  assert.equal((await reg(e, 'r', '{bad')).status, 400);
});
