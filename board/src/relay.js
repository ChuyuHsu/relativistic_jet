// 門牌轉送：固定網址 /relay/<名字>/* 反向代理到「目前登記的上游」（例如會變動的臨時隧道網址）。
// env.REGISTER_TOKEN：登記權杖（Worker 機密）。env.RELAY_SUFFIXES：允許的上游網域後綴，逗號分隔。
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8' } });
const NAME = /^[a-z0-9][a-z0-9-]{0,39}$/;
const DROP = new Set(['host', 'connection', 'content-length', 'transfer-encoding', 'cf-connecting-ip', 'cf-ray', 'x-forwarded-for', 'x-real-ip']);

export function allowedUpstream(raw, suffixes) {
  let u;
  try { u = new URL(raw); } catch { return null; }
  if (u.protocol !== 'https:' || u.username || u.password || u.port) return null;
  const h = u.hostname.toLowerCase();
  const ok = suffixes.some(s => h.endsWith('.' + s.toLowerCase()));
  return ok ? u.origin : null;
}

export async function handleRelay(req, env) {
  const url = new URL(req.url);
  const m = url.pathname.match(/^\/relay\/([^/]+)(\/.*)?$/);
  if (!m || !NAME.test(m[1])) return json({ error: 'bad relay name' }, 404);
  const [, name, rest = '/'] = m;
  const row = await env.DB.prepare('SELECT upstream,updated_at FROM relays WHERE name=?').bind(name).first();

  if (!m[2] && req.method === 'PUT') {
    const auth = req.headers.get('authorization') || '';
    if (!env.REGISTER_TOKEN || auth !== `Bearer ${env.REGISTER_TOKEN}`) return json({ error: 'unauthorized' }, 401);
    let raw = (await req.text()).trim();
    if (raw.startsWith('{')) { try { raw = String(JSON.parse(raw).url || '').trim(); } catch { raw = ''; } }
    const suffixes = (env.RELAY_SUFFIXES || 'trycloudflare.com').split(',').map(s => s.trim()).filter(Boolean);
    const origin = allowedUpstream(raw, suffixes);
    if (!origin) return json({ error: '上游必須是 https 且網域在白名單內' }, 400);
    await env.DB.prepare('INSERT INTO relays(name,upstream,updated_at) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET upstream=excluded.upstream,updated_at=excluded.updated_at')
      .bind(name, origin, Math.floor(Date.now() / 1000)).run();
    return json({ ok: true });
  }
  if (rest === '/_status' && req.method === 'GET')
    return json({ registered: !!row, updated_at: row ? row.updated_at : null });
  if (!row) return json({ error: `門牌 ${name} 還沒有登記上游` }, 503);

  const headers = new Headers();
  for (const [k, v] of req.headers) if (!DROP.has(k.toLowerCase())) headers.set(k, v);
  const hasBody = !['GET', 'HEAD'].includes(req.method);
  try {
    const res = await fetch(row.upstream + rest + url.search, {
      method: req.method, headers, body: hasBody ? req.body : undefined, redirect: 'manual',
      signal: AbortSignal.timeout(25000), ...(hasBody ? { duplex: 'half' } : {}),
    });
    return new Response(res.body, { status: res.status, headers: res.headers });
  } catch {
    return json({ error: `門牌 ${name} 的上游連不上（可能已換網址，等排隊器重新登記）` }, 503);
  }
}
