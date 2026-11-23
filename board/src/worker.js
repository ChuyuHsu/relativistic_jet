// 任務卡服務：Cloudflare Worker + D1。env.DB 是 D1，env.TOKEN 是寫入權杖。
import { bearerIs } from './auth.js';
import { handleHook, flush, resultOf, stuckCheck } from './queue.js';
import { stageOf, sameCommit, STAGES } from './stage.js';
// 卡片自己只管前段與結局：open 空、claimed 已認領、dropped 放棄；放行是卡片上的欄位（approved_*）。
// 中段（待自測、自測綠、排隊、合入、退回…）是排隊器的分支狀態，由排隊器的事件（/hooks/queue）寫進 phase 與 last_result，
// 排隊器自己定時來拉 /api/queue/desired 取得該管哪些分支與放行；看板不主動連排隊器。
const STATUSES = ['open', 'claimed', 'dropped'];
function canMove(from, to) { return from !== to && (from !== 'dropped' || to === 'open'); }
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8' } });
const now = () => Math.floor(Date.now() / 1000);
// 分支名由看板給定：<前綴><卡號小寫>，第二次起加 -r<次數>。前綴可用 BRANCH_PREFIX 設定（預設 dev/）。
const branchFor = (env, id, attempt = 1) =>
  `${env.BRANCH_PREFIX ?? 'dev/'}${String(id).toLowerCase().replace(/[^a-z0-9._-]+/g, '-')}${attempt > 1 ? `-r${attempt}` : ''}`;
async function parentBranch(db, c) {
  if (!c.parent) return undefined;
  const p = await db.prepare('SELECT branch FROM cards WHERE id=?').bind(c.parent).first();
  return p && p.branch ? p.branch : undefined;
}
const view = c => {
  const result = resultOf(c), stage = stageOf(c);
  let rejected = null;   // 退回的來源與原因（審查退回，或排隊器 rejected／halted／撞檔，附紅項）
  if (stage === 'rejected') {
    rejected = c.rejected_reason
      ? { source: 'review', reason: c.rejected_reason, by: c.rejected_by }
      : { source: 'queue', halted: c.phase === 'halted', reason: result ? result.detail : null, failed_items: result ? result.failed_items : null };
  }
  return { ...c, last_result: undefined, result, stage, rejected };
};
const CLEAR_REVIEW = 'submitted_commit=NULL,submitted_at=NULL,rejected_reason=NULL,rejected_by=NULL,rejected_at=NULL,approved_commit=NULL,approved_by=NULL,approved_at=NULL';
const HEX = /^[0-9a-f]{7,64}$/i;
// 通知名單（用逗號分隔的負責串 id 設定）攢批叫醒
async function notify(db, owners, c, kind, detail) {
  for (const o of owners) await db.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)').bind(o, c.id, c.branch, kind, detail, now()).run();
}
const list = v => String(v || '').split(',').map(x => x.trim()).filter(Boolean);

async function log(db, id, actor, note) {
  await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)').bind(id, now(), actor, note).run();
}

// 給排隊器定時拉：所有「已認領、有分支、還沒合入」的卡 = 排隊器該管的分支（已放棄、釋放的不在內）。出錯一律回非 200。
async function desired(req, env) {
  if (req.method !== 'GET') return json({ error: 'GET only' }, 405);
  if (!bearerIs(req, env.HOOK_TOKEN, env.READ_TOKEN)) return json({ error: 'unauthorized' }, 401);
  try {
    const rows = (await env.DB.prepare("SELECT * FROM cards WHERE status='claimed' AND branch IS NOT NULL AND (phase IS NULL OR phase!='merged') ORDER BY priority,created_at").all()).results;
    const out = [];
    for (const c of rows) {
      const e = { branch: c.branch };
      const p = await parentBranch(env.DB, c);
      if (p) e.parent = p;
      if (c.approved_commit) e.approved_sha = c.approved_commit;
      out.push(e);
    }
    return json(out);
  } catch { return json({ error: '看板內部錯誤' }, 500); }
}

export async function handle(req, env, ctx) {
  const url = new URL(req.url);
  const path = url.pathname;
  const db = env.DB;
  if (path === '/hooks/queue') return handleHook(req, env, ctx);
  const write = req.method !== 'GET';
  // 排隊器拉清單：HOOK_TOKEN 或 READ_TOKEN；其餘寫入要 TOKEN，讀取要 TOKEN 或 READ_TOKEN
  if (path === '/api/queue/desired') return desired(req, env);
  if (!(write ? bearerIs(req, env.TOKEN) : bearerIs(req, env.TOKEN, env.READ_TOKEN))) return json({ error: 'unauthorized' }, 401);
  let m;
  if (req.method === 'GET' && path === '/api/cards') {
    const status = url.searchParams.get('status');
    const rows = status
      ? await db.prepare('SELECT * FROM cards WHERE status=? ORDER BY priority,created_at').bind(status).all()
      : await db.prepare('SELECT * FROM cards ORDER BY priority,created_at').all();
    let out = rows.results.map(view);
    const sf = url.searchParams.get('stage');
    if (sf) { const want = new Set(sf.split(',')); out = out.filter(c => want.has(c.stage)); }
    return json(out);
  }
  if (req.method === 'GET' && (m = path.match(/^\/api\/cards\/([^/]+)\/tests$/))) {
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c || !c.branch) return json({ error: '卡不存在或還沒有分支' }, 404);
    return json({ branch: c.branch, phase: c.phase, phase_at: c.phase_at, approved_commit: c.approved_commit, result: resultOf(c) });
  }
  if (req.method === 'GET' && (m = path.match(/^\/api\/cards\/([^/]+)$/))) {
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c) return json({ error: 'not found' }, 404);
    const ev = await db.prepare('SELECT at,actor,note FROM events WHERE card_id=? ORDER BY n').bind(m[1]).all();
    return json({ ...view(c), events: ev.results });
  }
  const body = write ? await req.json().catch(() => ({})) : {};
  const actor = String(body.actor || 'unknown');
  // 送審：記下送審的版本，進「審查中」；退回後改好再送審，會清掉上一次的退回與放行與舊結果。
  if (req.method === 'POST' && (m = path.match(/^\/api\/cards\/([^/]+)\/submit$/))) {
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c || c.status !== 'claimed' || !c.branch) return json({ error: '只有已認領的卡能送審' }, 409);
    if (!HEX.test(String(body.commit || ''))) return json({ error: '送審要帶提交 hash（commit，7 到 64 碼十六進位）' }, 400);
    await db.prepare(`UPDATE cards SET rejected_reason=NULL,rejected_by=NULL,rejected_at=NULL,approved_commit=NULL,approved_by=NULL,approved_at=NULL,phase=NULL,phase_at=NULL,last_result=NULL,submitted_commit=?,submitted_at=?,updated_at=? WHERE id=?`).bind(String(body.commit), now(), now(), c.id).run();
    await log(db, c.id, actor, `送審 ${String(body.commit).slice(0, 12)}`);
    await notify(db, list(env.REVIEW_THREADS), c, 'review_requested', String(body.commit).slice(0, 12));
    return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(c.id).first()));
  }
  // 審查退回：進「退回」，原因存卡片並叫醒負責串；放行一併清掉。
  if (req.method === 'POST' && (m = path.match(/^\/api\/cards\/([^/]+)\/reject$/))) {
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c || c.status !== 'claimed' || !c.submitted_commit) return json({ error: '只有已送審的卡能退回' }, 409);
    if (!body.reason) return json({ error: '退回要寫原因（reason）' }, 400);
    const reason = String(body.reason).slice(0, 1000);
    await db.prepare('UPDATE cards SET rejected_reason=?,rejected_by=?,rejected_at=?,approved_commit=NULL,approved_by=NULL,approved_at=NULL,updated_at=? WHERE id=?').bind(reason, actor, now(), now(), c.id).run();
    await log(db, c.id, actor, `審查退回：${reason}`);
    if (c.owner_thread) await notify(db, [c.owner_thread], c, 'review_rejected', reason);
    return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(c.id).first()));
  }
  // 放行就是卡片上的欄位：approved_commit／by／at；撤回清空。放行的 commit 必須等於送審的 commit。排隊器自己來拉（/api/queue/desired）。
  if (req.method === 'POST' && (m = path.match(/^\/api\/cards\/([^/]+)\/(approve|revoke)$/))) {
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c || !c.branch) return json({ error: '卡不存在或還沒有分支' }, 404);
    if (m[2] === 'approve') {
      if (!HEX.test(String(body.commit || ''))) return json({ error: '放行要帶審過的提交 hash（commit，7 到 64 碼十六進位）' }, 400);
      if (!c.submitted_commit) return json({ error: '還沒送審' }, 409);
      if (c.rejected_reason) return json({ error: '已被審查退回，要先重新送審' }, 409);
      if (!sameCommit(String(body.commit), c.submitted_commit)) return json({ error: `放行的版本與送審的版本不同（送審 ${c.submitted_commit.slice(0, 12)}），送審後可能又有新版本` }, 409);
      await db.prepare('UPDATE cards SET approved_commit=?,approved_by=?,approved_at=?,updated_at=? WHERE id=?').bind(String(body.commit), actor, now(), now(), c.id).run();
      await log(db, c.id, actor, `放行 ${String(body.commit).slice(0, 12)}`);
    } else {
      await db.prepare('UPDATE cards SET approved_commit=NULL,approved_by=NULL,approved_at=NULL,updated_at=? WHERE id=?').bind(now(), c.id).run();
      await log(db, c.id, actor, '撤回放行');
    }
    return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(c.id).first()));
  }
  // 退回後換新分支重做：取消登記舊分支、登記新分支（<卡號>-r2、-r3…），phase 清空
  if (req.method === 'POST' && (m = path.match(/^\/api\/cards\/([^/]+)\/rebranch$/))) {
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c) return json({ error: 'not found' }, 404);
    if (c.status !== 'claimed' || !c.branch) return json({ error: '只有已認領的卡能換分支' }, 409);
    const attempt = c.attempt + 1, branch = branchFor(env, c.id, attempt);
    const r = await db.prepare(`UPDATE cards SET branch=?,attempt=?,phase=NULL,phase_at=NULL,last_result=NULL,${CLEAR_REVIEW},updated_at=? WHERE id=? AND branch=?`).bind(branch, attempt, now(), c.id, c.branch).run();
    if (r.meta.changes !== 1) return json({ error: '同時有人在改，請重試' }, 409);
    await log(db, c.id, actor, `換分支 ${c.branch} → ${branch}`);
    return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(c.id).first()));
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
      const branch = branchFor(env, id);
      const res = await db.prepare("UPDATE cards SET status='claimed',owner=?,branch=?,attempt=1,owner_thread=?,updated_at=? WHERE id=? AND status='open'")
        .bind(String(body.owner), branch, body.owner_thread ? String(body.owner_thread) : null, now(), id).run();
      if (res.meta.changes === 1) {
        await log(db, id, actor === 'unknown' ? body.owner : actor, `認領 ${body.owner}，分支 ${branch}`);
        return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(id).first()));
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
    if ('branch' in body) return json({ error: '分支名由看板給定；退回後重做請用 POST /api/cards/<id>/rebranch' }, 400);
    for (const k of ['title', 'body', 'area', 'parent', 'owner_thread']) if (k in body) { sets.push(`${k}=?`); vals.push(String(body[k])); }
    if ('priority' in body) { sets.push('priority=?'); vals.push(Number(body.priority)); }
    let note = '修改';
    if ('status' in body) {
      if (!STATUSES.includes(body.status)) return json({ error: '狀態不合法' }, 400);
      if (c.phase === 'merged') return json({ error: '已合併，不能再改狀態' }, 409);
      if (!canMove(c.status, body.status)) return json({ error: `不能從 ${c.status} 到 ${body.status}` }, 409);
      sets.push('status=?'); vals.push(body.status);
      if (body.status === 'open') sets.push('owner=NULL', 'owner_thread=NULL', 'branch=NULL', 'phase=NULL', 'phase_at=NULL', 'last_result=NULL', ...CLEAR_REVIEW.split(','), 'stuck_notified_at=NULL');
      note = `${c.status} → ${body.status}` + (body.note ? `：${body.note}` : '');
    }
    if (!sets.length) return json({ error: '沒有要改的欄位' }, 400);
    sets.push('updated_at=?'); vals.push(now());
    // 狀態轉移也帶舊狀態條件，避免兩個人同時改
    await db.prepare(`UPDATE cards SET ${sets.join(',')} WHERE id=? AND status=?`).bind(...vals, id, c.status).run();
    await log(db, id, actor, note);
    return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(id).first()));
  }
  return json({ error: 'not found' }, 404);
}

export default {
  fetch: (req, env, ctx) => handle(req, env, ctx),
  scheduled: (_ev, env, ctx) => ctx.waitUntil(stuckCheck(env).then(() => flush(env))),
};
