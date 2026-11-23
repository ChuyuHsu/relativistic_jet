// 任務卡服務：Cloudflare Worker + D1。env.DB 是 D1，env.TOKEN 是寫入權杖。
import { bearerIs } from './auth.js';
import { handleRelay } from './relay.js';
import { handleHook, queueCall, flush, refreshPhase } from './queue.js';
// 卡片自己只管前段與結局：open 空、claimed 已認領、dropped 放棄。
// 中段（待自測、自測綠、排隊、合入、退回…）以上游排隊器的分支狀態為準，存在 phase 欄位只是快取。
const STATUSES = ['open', 'claimed', 'dropped'];
function canMove(from, to) { return from !== to && (from !== 'dropped' || to === 'open'); }
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8' } });
const now = () => Math.floor(Date.now() / 1000);

async function log(db, id, actor, note) {
  await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)').bind(id, now(), actor, note).run();
}

export async function handle(req, env, ctx) {
  const url = new URL(req.url);
  const path = url.pathname;
  const db = env.DB;
  if (path.startsWith('/relay/')) return handleRelay(req, env);
  if (path === '/hooks/queue') return handleHook(req, env, ctx);
  const write = req.method !== 'GET';
  // 寫入要 TOKEN；讀取要 TOKEN 或 READ_TOKEN（讀取也會用看板自己的 QUEUE_TOKEN 代查排隊器，不能公開）
  if (!(write ? bearerIs(req, env.TOKEN) : bearerIs(req, env.TOKEN, env.READ_TOKEN))) return json({ error: 'unauthorized' }, 401);
  let m;
  if (req.method === 'GET' && path === '/api/cards') {
    const status = url.searchParams.get('status');
    const rows = status
      ? await db.prepare('SELECT * FROM cards WHERE status=? ORDER BY priority,created_at').bind(status).all()
      : await db.prepare('SELECT * FROM cards ORDER BY priority,created_at').all();
    let out = rows.results;
    if (url.searchParams.get('live') === '1') {   // 向上游排隊器查最新狀態（最多 40 張未合入的），查不到用快取
      const todo = out.filter(c => c.status === 'claimed' && c.branch && c.phase !== 'merged').slice(0, 40);
      const fresh = new Map(await Promise.all(todo.map(async c => [c.id, await refreshPhase(env, c)])));
      out = out.map(c => (fresh.has(c.id) ? fresh.get(c.id) : c));
    }
    return json(out);
  }
  if (req.method === 'GET' && (m = path.match(/^\/api\/cards\/([^/]+)\/tests$/))) {
    const c = await db.prepare('SELECT branch FROM cards WHERE id=?').bind(m[1]).first();
    if (!c || !c.branch) return json({ error: '卡不存在或還沒有分支' }, 404);
    return queueCall(env, 'GET', '/branches/' + encodeURIComponent(c.branch));
  }
  if (req.method === 'GET' && (m = path.match(/^\/api\/cards\/([^/]+)$/))) {
    let c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c) return json({ error: 'not found' }, 404);
    if (c.status === 'claimed' && c.branch) c = await refreshPhase(env, c);
    const ev = await db.prepare('SELECT at,actor,note FROM events WHERE card_id=? ORDER BY n').bind(m[1]).all();
    return json({ ...c, events: ev.results });
  }
  const body = write ? await req.json().catch(() => ({})) : {};
  const actor = String(body.actor || 'unknown');
  if (req.method === 'POST' && (m = path.match(/^\/api\/cards\/([^/]+)\/(approve|revoke)$/))) {
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c || !c.branch) return json({ error: '卡不存在或還沒有分支' }, 404);
    if (m[2] === 'approve' && !body.commit) return json({ error: '放行要帶審過的提交 hash（commit）' }, 400);
    const r = await queueCall(env, 'POST', '/' + m[2], m[2] === 'approve' ? { branch: c.branch, sha: String(body.commit), by: actor } : { branch: c.branch });
    if (r.ok) await log(db, c.id, actor, m[2] === 'approve' ? `放行 ${String(body.commit).slice(0, 12)}` : '撤回放行');
    return r;
  }
  if (req.method === 'POST' && path === '/api/cards') {
    if (!body.id || !body.title) return json({ error: 'id 與 title 必填' }, 400);
    const t = now();
    try {
      await db.prepare('INSERT INTO cards(id,title,body,area,priority,parent,owner_thread,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)')
        .bind(String(body.id), String(body.title), String(body.body || ''), String(body.area || ''), Number(body.priority ?? 100), body.parent ?? null, body.owner_thread ?? null, t, t).run();
    } catch (e) { return json({ error: 'id 已存在' }, 409); }
    await log(db, body.id, actor, '建立');
    return json({ ok: true }, 201);
  }
  // 通用匯入格式：{cards:[{id,title,body?,area?,priority?,status?,owner?,branch?}]}；已存在的 id 略過
  if (req.method === 'POST' && path === '/api/import') {
    let added = 0, skipped = 0;
    for (const c of body.cards || []) {
      if (!c.id || !c.title || (c.status && !STATUSES.includes(c.status))) { skipped++; continue; }
      const t = now();
      const r = await db.prepare('INSERT OR IGNORE INTO cards(id,title,body,area,priority,status,owner,branch,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)')
        .bind(String(c.id), String(c.title), String(c.body || ''), String(c.area || ''), Number(c.priority ?? 100), c.status || 'open', c.owner ?? null, c.branch ?? null, t, t).run();
      r.meta.changes ? added++ : skipped++;
    }
    return json({ added, skipped });
  }
  if (req.method === 'POST' && path === '/api/claim') {
    // 原子認領：指定 id，或依優先序（可加 area）挑第一張未認領的。UPDATE 帶 status='open' 條件，只有一條會成功。
    if (!body.owner) return json({ error: 'owner 必填' }, 400);
    for (let i = 0; i < 5; i++) {
      let id = body.id;
      if (!id) {
        const r = body.area
          ? await db.prepare("SELECT id FROM cards WHERE status='open' AND area=? ORDER BY priority,created_at LIMIT 1").bind(body.area).first()
          : await db.prepare("SELECT id FROM cards WHERE status='open' ORDER BY priority,created_at LIMIT 1").first();
        if (!r) return json({ error: '沒有可認領的卡' }, 404);
        id = r.id;
      }
      const res = await db.prepare("UPDATE cards SET status='claimed',owner=?,branch=?,owner_thread=?,updated_at=? WHERE id=? AND status='open'")
        .bind(String(body.owner), body.branch ? String(body.branch) : null, body.owner_thread ? String(body.owner_thread) : null, now(), id).run();
      if (res.meta.changes === 1) {
        await log(db, id, actor === 'unknown' ? body.owner : actor, `認領 ${body.owner}`);
        return json(await db.prepare('SELECT * FROM cards WHERE id=?').bind(id).first());
      }
      if (body.id) return json({ error: '這張卡已被別人拿走或不存在' }, 409);
    }
    return json({ error: '搶不到，請重試' }, 409);
  }
  if (req.method === 'PATCH' && (m = path.match(/^\/api\/cards\/([^/]+)$/))) {
    const id = m[1];
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(id).first();
    if (!c) return json({ error: 'not found' }, 404);
    const sets = [], vals = [];
    for (const k of ['title', 'body', 'area', 'branch', 'parent', 'owner_thread']) if (k in body) { sets.push(`${k}=?`); vals.push(String(body[k])); }
    if ('branch' in body && body.branch !== c.branch) sets.push('phase=NULL', 'phase_at=NULL');   // 換新分支重做
    if ('priority' in body) { sets.push('priority=?'); vals.push(Number(body.priority)); }
    let note = '修改';
    if ('status' in body) {
      if (!STATUSES.includes(body.status)) return json({ error: '狀態不合法' }, 400);
      if (!canMove(c.status, body.status)) return json({ error: `不能從 ${c.status} 到 ${body.status}` }, 409);
      sets.push('status=?'); vals.push(body.status);
      if (body.status === 'open') sets.push('owner=NULL', 'owner_thread=NULL', 'branch=NULL', 'phase=NULL', 'phase_at=NULL');
      note = `${c.status} → ${body.status}` + (body.note ? `：${body.note}` : '');
    }
    if (!sets.length) return json({ error: '沒有要改的欄位' }, 400);
    sets.push('updated_at=?'); vals.push(now());
    // 狀態轉移也帶舊狀態條件，避免兩個人同時改
    await db.prepare(`UPDATE cards SET ${sets.join(',')} WHERE id=? AND status=?`).bind(...vals, id, c.status).run();
    await log(db, id, actor, note);
    return json(await db.prepare('SELECT * FROM cards WHERE id=?').bind(id).first());
  }
  return json({ error: 'not found' }, 404);
}

export default {
  fetch: (req, env, ctx) => handle(req, env, ctx),
  scheduled: (_ev, env, ctx) => ctx.waitUntil(flush(env)),
};
