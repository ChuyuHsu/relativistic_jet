// 看板不連排隊器：排隊器自己來拉清單與放行（/api/queue/desired），結果事件存在卡片上；攢批叫醒負責串（routine fire）。
// 機密：HOOK_TOKEN（排隊器送事件、拉清單用）、ROUTINE_KEY（叫醒用）。
// 設定：ROUTINE_ID、WAKE_BATCH_S（預設 300）、WAKE_MAX_PER_HOUR（預設 10）。
import { bearerIs } from './auth.js';
import { stageOf, sameCommit } from './stage.js';
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8' } });
const now = () => Math.floor(Date.now() / 1000);

// 事件種類 → 上游分支狀態快取（phase；用上游自己的狀態名）。事件只當快取與叫醒，不當真相；沒列的只記錄。
const PHASE_OF = { pushed: 'pending', tested_green: 'selftest_green', tested_red: 'rejected', queued: 'queued', merged: 'merged', rejected: 'rejected', conflict: 'rejected', halted: 'halted' };
// 需要人處理、要叫醒負責串的種類
const ATTENTION = new Set(['tested_red', 'rejected', 'conflict', 'halted', 'queue_error', 'superseded']);
const KIND_TEXT = { superseded: '送審後內容又改了，要重新送審', stuck: '卡住沒動靜', review_requested: '送審待審', review_rejected: '審查退回，請修改後重新送審', tested_red: '自測紅', rejected: '被退回', conflict: '跟 main 撞檔', halted: '排隊器暫停待人處理', queue_error: '排隊器出狀況' };

export async function handleHook(req, env, ctx) {
  if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
  if (!bearerIs(req, env.HOOK_TOKEN)) return json({ error: 'unauthorized' }, 401);
  const body = await req.json().catch(() => null);
  if (!body || !Array.isArray(body.events)) return json({ error: 'body 要有 events 陣列' }, 400);
  const db = env.DB, source = String(body.source || 'queue');
  let applied = 0, unmatched = 0, dup = 0;
  for (const e of body.events.slice(0, 200)) {
    let superseded = false;
    const kind = String(e.kind || ''), branch = e.branch ? String(e.branch) : null;
    if (e.id != null) {
      const r = await db.prepare('INSERT OR IGNORE INTO hook_seen(key) VALUES(?)').bind(`${source}:${e.id}`).run();
      if (!r.meta.changes) { dup++; continue; }
    }
    const card = branch ? await db.prepare('SELECT * FROM cards WHERE branch=?').bind(branch).first() : null;
    const detail = e.detail ? String(e.detail).slice(0, 500) : '';
    if (card) {
      applied++;
      // 送審後分支又推了內容不同的新提交（排隊器送 pushed 帶 commit）也算「內容又改了」
      superseded = kind === 'superseded' || (kind === 'pushed' && e.commit && card.submitted_commit && !sameCommit(String(e.commit), card.submitted_commit));
      if (superseded) {   // 內容又改了：送審、放行、舊結果作廢，回到開發中
        await db.prepare('UPDATE cards SET submitted_commit=NULL,submitted_at=NULL,rejected_reason=NULL,rejected_by=NULL,rejected_at=NULL,approved_commit=NULL,approved_by=NULL,approved_at=NULL,phase=NULL,phase_at=NULL,last_result=NULL,updated_at=? WHERE id=?').bind(now(), card.id).run();
      }
      // 優先用事件自帶的上游狀態名（state，或排隊器狀態事件的 to），否則依 kind 對照
      const to = kind === 'superseded' ? null : e.state ? String(e.state) : (typeof e.to === 'string' && /^[a-z_]{3,20}$/.test(e.to) ? e.to : PHASE_OF[kind]);
      if (to) await db.prepare('UPDATE cards SET phase=?,phase_at=?,updated_at=? WHERE id=?').bind(to, now(), now(), card.id).run();
      if (!['approved', 'revoked', 'superseded'].includes(kind)) {   // 放行是看板自己的欄位，排隊器回報的放行事件不覆蓋「最新結果」
        const res = { kind, at: e.at ?? now(), commit: e.commit ?? null, detail: detail || null, from: e.from ?? null, to: e.to ?? null, failed_items: Array.isArray(e.failed_items) ? e.failed_items.slice(0, 50) : null };
        await db.prepare('UPDATE cards SET last_result=? WHERE id=?').bind(JSON.stringify(res), card.id).run();
      }
      await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)')
        .bind(card.id, now(), source, `${kind}${to ? ` → ${to}` : ''}${detail ? `：${detail}` : ''}`).run();
    } else unmatched++;
    if (ATTENTION.has(kind) || (card && superseded)) {
      await db.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)')
        .bind(card?.owner_thread || '', card?.id ?? null, branch, superseded ? 'superseded' : kind, detail, now()).run();
    }
  }
  if (ctx && ctx.waitUntil) ctx.waitUntil(flush(env));
  return json({ ok: true, applied, unmatched, duplicate: dup });
}

// 卡片上存的排隊器最新結果（事件原樣整理後存成 JSON 字串）
export function resultOf(c) {
  if (!c.last_result) return null;
  try { return JSON.parse(c.last_result); } catch { return null; }
}

export function buildWakeText(owner, items) {
  const head = owner ? `負責串 ${owner}` : '（沒有負責串的事件）';
  return `${head}：\n` + items.map(i =>
    `- 卡 ${i.card_id || '（無）'}｜分支 ${i.branch || '（無）'}｜${KIND_TEXT[i.kind] || i.kind}${i.detail ? `｜${i.detail}` : ''}`).join('\n');
}

// 攢批叫醒：最舊的待送事件滿 WAKE_BATCH_S 才送；同負責串合成一則；每小時最多 WAKE_MAX_PER_HOUR 次；失敗保留下次重送。
export async function flush(env) {
  if (!env.ROUTINE_ID || !env.ROUTINE_KEY) return { skipped: 'not configured' };
  const db = env.DB, t = now();
  const rows = (await db.prepare('SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY n LIMIT 200').all()).results;
  if (!rows.length) return { sent: 0 };
  if (t - Math.min(...rows.map(r => r.at)) < Number(env.WAKE_BATCH_S ?? 300)) return { waiting: true };
  const groups = new Map();
  for (const r of rows) (groups.get(r.owner_thread) || groups.set(r.owner_thread, []).get(r.owner_thread)).push(r);
  const cap = Number(env.WAKE_MAX_PER_HOUR ?? 10);
  let sent = 0;
  for (const [owner, items] of groups) {
    const used = (await db.prepare('SELECT count(*) AS n FROM fires WHERE at>?').bind(t - 3600).first()).n;
    if (used >= cap) break;   // 超過上限：留到下一輪（自然併進下一則）
    let ok = false;
    try {
      const res = await fetch(`https://api.anthropic.com/v1/claude_code/routines/${env.ROUTINE_ID}/fire`, {
        method: 'POST', signal: AbortSignal.timeout(20000),
        headers: { authorization: `Bearer ${env.ROUTINE_KEY}`, 'anthropic-beta': 'experimental-cc-routine-2026-04-01', 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
        body: JSON.stringify({ text: buildWakeText(owner, items) }),
      });
      ok = res.ok;
    } catch { ok = false; }
    if (!ok) break;   // 失敗：保留，下次重送
    await db.prepare('INSERT INTO fires(at) VALUES(?)').bind(t).run();
    const ids = items.map(i => i.n);
    await db.prepare(`UPDATE outbox SET sent_at=? WHERE n IN (${ids.map(() => '?').join(',')})`).bind(t, ...ids).run();
    sent++;
  }
  return { sent };
}

// 卡住提醒：開發中或退回的卡太久沒更新（STUCK_HOURS，預設 6），通知 ADMIN_THREADS（逗號分隔的串 id，設定值，不進公開庫）。
// 有新動靜（updated_at 比上次提醒新）之後再卡住才會再提醒，不重複吵。
export async function stuckCheck(env) {
  const owners = String(env.ADMIN_THREADS || '').split(',').map(x => x.trim()).filter(Boolean);
  if (!owners.length) return { skipped: true };
  const hours = Number(env.STUCK_HOURS ?? 6), t = now();
  const rows = (await env.DB.prepare("SELECT * FROM cards WHERE status='claimed' AND branch IS NOT NULL AND updated_at<? AND (stuck_notified_at IS NULL OR stuck_notified_at<updated_at)").bind(t - hours * 3600).all()).results;
  let n = 0;
  for (const c of rows) {
    const st = stageOf(c);
    if (st !== 'developing' && st !== 'rejected') continue;
    const detail = `${st === 'developing' ? '開發中' : '退回'}超過 ${hours} 小時沒動靜（負責串 ${c.owner_thread || '無'}）`;
    for (const o of owners) await env.DB.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)').bind(o, c.id, c.branch, 'stuck', detail, t).run();
    await env.DB.prepare('UPDATE cards SET stuck_notified_at=? WHERE id=?').bind(t, c.id).run();
    n++;
  }
  return { notified: n };
}
