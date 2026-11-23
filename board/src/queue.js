// 排隊器／CI 對接：收事件（/hooks/queue）、代呼叫排隊器的放行（經門牌）、攢批叫醒負責串（routine fire）。
// 機密：HOOK_TOKEN（排隊器送事件用）、QUEUE_TOKEN（看板呼叫排隊器寫入用）、ROUTINE_KEY（叫醒用）。
// 設定：QUEUE_RELAY（門牌名，預設 queue）、ROUTINE_ID、WAKE_BATCH_S（預設 300）、WAKE_MAX_PER_HOUR（預設 10）。
import { bearerIs } from './auth.js';
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8' } });
const now = () => Math.floor(Date.now() / 1000);

// 事件種類 → 上游分支狀態快取（phase；用上游自己的狀態名）。事件只當快取與叫醒，不當真相；沒列的只記錄。
const PHASE_OF = { pushed: 'pending', tested_green: 'selftest_green', tested_red: 'rejected', queued: 'queued', merged: 'merged', rejected: 'rejected' };
// 需要人處理、要叫醒負責串的種類
const ATTENTION = new Set(['tested_red', 'rejected', 'conflict', 'halted', 'queue_error']);
const KIND_TEXT = { tested_red: '自測紅', rejected: '被退回', conflict: '跟 main 撞檔', halted: '排隊器暫停待人處理', queue_error: '排隊器出狀況' };

export async function handleHook(req, env, ctx) {
  if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
  if (!bearerIs(req, env.HOOK_TOKEN)) return json({ error: 'unauthorized' }, 401);
  const body = await req.json().catch(() => null);
  if (!body || !Array.isArray(body.events)) return json({ error: 'body 要有 events 陣列' }, 400);
  const db = env.DB, source = String(body.source || 'queue');
  let applied = 0, unmatched = 0, dup = 0;
  for (const e of body.events.slice(0, 200)) {
    const kind = String(e.kind || ''), branch = e.branch ? String(e.branch) : null;
    if (e.id != null) {
      const r = await db.prepare('INSERT OR IGNORE INTO hook_seen(key) VALUES(?)').bind(`${source}:${e.id}`).run();
      if (!r.meta.changes) { dup++; continue; }
    }
    const card = branch ? await db.prepare('SELECT * FROM cards WHERE branch=?').bind(branch).first() : null;
    const detail = e.detail ? String(e.detail).slice(0, 500) : '';
    if (card) {
      applied++;
      // 優先用事件自帶的上游狀態名（state，或排隊器狀態事件的 to），否則依 kind 對照
      const to = e.state ? String(e.state) : (typeof e.to === 'string' && /^[a-z_]{3,20}$/.test(e.to) ? e.to : PHASE_OF[kind]);
      if (to) await db.prepare('UPDATE cards SET phase=?,phase_at=?,updated_at=? WHERE id=?').bind(to, now(), now(), card.id).run();
      await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)')
        .bind(card.id, now(), source, `${kind}${to ? ` → ${to}` : ''}${detail ? `：${detail}` : ''}`).run();
    } else unmatched++;
    if (ATTENTION.has(kind)) {
      await db.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)')
        .bind(card?.owner_thread || '', card?.id ?? null, branch, kind, detail, now()).run();
    }
  }
  if (ctx && ctx.waitUntil) ctx.waitUntil(flush(env));
  return json({ ok: true, applied, unmatched, duplicate: dup });
}

// 呼叫排隊器（經自己的門牌登記的上游），用看板自己存的寫入權杖
export async function queueCall(env, method, path, payload) {
  const name = env.QUEUE_RELAY || 'queue';
  const row = await env.DB.prepare('SELECT upstream FROM relays WHERE name=?').bind(name).first();
  if (!row) return json({ error: `排隊器門牌 ${name} 還沒登記` }, 503);
  if (!env.QUEUE_TOKEN) return json({ error: '看板還沒設定 QUEUE_TOKEN' }, 503);
  try {
    const res = await fetch(row.upstream + path, {
      method, signal: AbortSignal.timeout(25000),
      headers: { authorization: `Bearer ${env.QUEUE_TOKEN}`, 'content-type': 'application/json' },
      body: method === 'GET' ? undefined : JSON.stringify(payload || {}),
    });
    return new Response(res.body, { status: res.status, headers: { 'content-type': res.headers.get('content-type') || 'application/json' } });
  } catch { return json({ error: '排隊器連不上（可能換網址，等它重新登記）' }, 503); }
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

// 向上游排隊器查這張卡分支的真實狀態並更新快取；查不到（沒門牌、連不上、沒這條分支）就回原樣。
export async function refreshPhase(env, card) {
  try {
    const res = await queueCall(env, 'GET', '/branches/' + encodeURIComponent(card.branch));
    if (!res.ok) return { ...card, phase_live: false };
    const b = await res.json();
    if (!b || typeof b.state !== 'string') return { ...card, phase_live: false };
    const t = now();
    await env.DB.prepare('UPDATE cards SET phase=?,phase_at=? WHERE id=?').bind(b.state, t, card.id).run();
    return { ...card, phase: b.state, phase_at: t, approved: !!b.approved, phase_live: true };
  } catch { return { ...card, phase_live: false }; }
}
