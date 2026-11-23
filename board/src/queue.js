// 看板不連排隊器：排隊器自己來拉清單與放行（/api/queue/desired），結果事件存在卡片上；攢批叫醒負責串（routine fire）。
// 機密：HOOK_TOKEN（排隊器送事件、拉清單用）、ROUTINE_KEY（叫醒用）。
// 設定：ROUTINE_ID、WAKE_BATCH_S（預設 120）、WAKE_MAX_PER_HOUR（預設 10）。
import { bearerIs } from './auth.js';
import { stageOf, sameCommit } from './stage.js';
const list = v => String(v || '').split(',').map(x => x.trim()).filter(Boolean);
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8' } });
const now = () => Math.floor(Date.now() / 1000);

// 事件種類 → 上游分支狀態快取（phase；用上游自己的狀態名）。事件只當快取與叫醒，不當真相；沒列的只記錄。
const PHASE_OF = { pushed: 'pending', tested_green: 'selftest_green', tested_red: 'rejected', queued: 'queued', merged: 'merged', rejected: 'rejected', conflict: 'rejected', halted: 'halted' };
// 需要人處理、要叫醒負責串的種類
const ATTENTION = new Set(['tested_red', 'rejected', 'conflict', 'superseded']);   // → 領卡的開發者
const ADMIN_KINDS = new Set(['halted', 'parent_blocked', 'queue_error']);        // → 管理者
const KIND_TEXT = { drop_rejected: '放棄沒成立：這張卡還有改動不在 main 也不在接手的卡裡', drop_discarded: '卡被放棄，且仍有改動不在 main（事後請看一眼）',  parent_blocked: '父卡被放棄或不存在，子卡被擋住',  superseded: '送審後內容又改了，要重新送審', stuck: '卡住沒動靜', review_requested: '送審待審', review_rejected: '審查退回，請修改後重新送審', tested_red: '自測紅', rejected: '被退回', conflict: '跟 main 撞檔', halted: '排隊器暫停待人處理', queue_error: '排隊器出狀況' };

export async function handleHook(req, env, ctx) {
  if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
  if (!bearerIs(req, env.HOOK_TOKEN)) return json({ error: 'unauthorized' }, 401);
  const body = await req.json().catch(() => null);
  if (!body || !Array.isArray(body.events)) return json({ error: 'body 要有 events 陣列' }, 400);
  const db = env.DB, source = String(body.source || 'queue');
  const admins = list(env.ADMIN_THREADS);
  let applied = 0, unmatched = 0, dup = 0, stale = 0;
  const post = (owners, card, kind, branch, detail) => Promise.all(owners.map(o => db.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)')
    .bind(o, card?.id ?? null, branch, kind, detail, now()).run()));
  for (const e of body.events.slice(0, 200)) {
    const kind = String(e.kind || ''), branch = e.branch ? String(e.branch) : null;
    if (e.id != null && e.id !== '') {
      const r = await db.prepare('INSERT OR IGNORE INTO hook_seen(key) VALUES(?)').bind(`${source}:${String(e.id)}`).run();
      if (!r.meta.changes) { dup++; continue; }
    }
    let card = branch ? await db.prepare('SELECT * FROM cards WHERE branch=?').bind(branch).first() : null;
    if (!card && kind === 'drop_checked' && e.card) card = await db.prepare('SELECT * FROM cards WHERE id=?').bind(String(e.card)).first();
    const detail = e.detail ? String(e.detail).slice(0, 500) : '';
    if (!card) {
      unmatched++;
      if (ADMIN_KINDS.has(kind)) await post(admins.length ? admins : [''], null, kind, branch, detail);   // 排隊器自己的問題：通知管理者
      continue;
    }
    if (kind === 'drop_checked') {   // 放棄待查的結果：只對「放棄待查」的卡有效，其他一律當過期丟掉
      if (!card || !card.dropping_at || card.status === 'dropped') { stale++; continue; }
      // 比對是哪一次放棄的結果：事件帶回 dropping_at 就必須相同；沒帶但有 target 就必須是現在接手卡的分支
      if (e.dropping_at != null && Number(e.dropping_at) !== card.dropping_at) { stale++; continue; }
      if (e.dropping_at == null && card.superseded_by && e.target != null) {
        const sc = await db.prepare('SELECT id,branch FROM cards WHERE id=?').bind(card.superseded_by).first();
        if (!sc || String(e.target) !== (sc.branch || `dev/${String(sc.id).toLowerCase()}`)) { stale++; continue; }
      }
      const unmerged = e.unmerged === true, covered = e.covered === true, mc = Number(e.missing_count ?? 0) || 0;
      const missing = Array.isArray(e.missing_files) ? e.missing_files.slice(0, 20).map(String).join('、') : '';
      applied++;
      // 帶 superseded_by：沒有不在 main 的改動，或接手的卡全都涵蓋了才放棄；只帶 discard_reason：一律放棄（有漏的另發通知）
      const ok = !card.superseded_by ? true : (!unmerged || covered);
      if (ok) {
        const r = await db.prepare("UPDATE cards SET status='dropped',drop_unmerged=?,drop_checked_at=?,updated_at=? WHERE id=? AND dropping_at IS NOT NULL AND status!='dropped'").bind(unmerged ? 1 : 0, now(), now(), card.id).run();
        if (r.meta.changes === 1) {
          await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)').bind(card.id, now(), source, `放棄查驗通過（${unmerged ? `有改動不在 main，缺 ${mc} 個檔` : '沒有漏掉的改動'}）`).run();
          await notifyKids(env, card.id);
          if (unmerged && !card.superseded_by) await post(admins.length ? admins : [''], card, 'drop_discarded', card.branch, `缺 ${mc} 個檔${missing ? `：${missing}` : ''}；放棄原因：${card.discard_reason}`);
        }
      } else {   // 放棄不成立：回到原階段，通知負責串與管理者
        const r = await db.prepare('UPDATE cards SET dropping_at=NULL,superseded_by=NULL,discard_reason=NULL,drop_unmerged=1,drop_checked_at=?,updated_at=? WHERE id=? AND dropping_at IS NOT NULL').bind(now(), now(), card.id).run();
        if (r.meta.changes === 1) {
          const why = `這張卡還有改動不在 main，也不在 ${card.superseded_by} 裡${mc ? `（缺 ${mc} 個檔）` : ''}${missing ? `，缺的檔：${missing}` : ''}`;
          await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)').bind(card.id, now(), source, `放棄沒成立：${why}`).run();
          await post([...new Set([card.owner_thread || '', ...(admins.length ? admins : [])])], card, 'drop_rejected', card.branch, why);
        }
      }
      continue;
    }
    // 過期回報不算數：卡片沒在跑（沒放行、已放棄），或事件依據的放行版本／重測計數跟卡片現在的不符，一律丟掉（仍回 200）
    const claimedAndApproved = card.status === 'claimed' && card.approved_commit;
    const sameApproval = e.approved_sha == null || sameCommit(String(e.approved_sha), card.approved_commit);
    const sameRetest = e.retest == null || Number(e.retest) === card.retest;
    if (kind !== 'merged' && (!claimedAndApproved || !sameApproval || !sameRetest)) { stale++; continue; }   // 已合併事實一律接受（main 已經有了）
    applied++;
    if (kind === 'superseded') {   // 內容變了：送審、放行、舊結果作廢，回到開發中
      await db.prepare('UPDATE cards SET submitted_commit=NULL,submitted_at=NULL,rejected_reason=NULL,rejected_by=NULL,rejected_at=NULL,approved_commit=NULL,approved_by=NULL,approved_at=NULL,phase=NULL,phase_at=NULL,last_result=NULL,updated_at=? WHERE id=?').bind(now(), card.id).run();
    }
    // 優先用事件自帶的上游狀態名（state，或排隊器狀態事件的 to），否則依 kind 對照
    const to = kind === 'superseded' ? null : e.state ? String(e.state) : (typeof e.to === 'string' && /^[a-z_]{3,20}$/.test(e.to) ? e.to : PHASE_OF[kind]);
    if (to) await db.prepare('UPDATE cards SET phase=?,phase_at=?,updated_at=? WHERE id=?').bind(to, now(), now(), card.id).run();
    if (!['approved', 'revoked', 'superseded', 'parent_blocked'].includes(kind)) {   // 放行是看板自己的欄位；父卡擋住只是註記，都不覆蓋「最新結果」
      let fi = Array.isArray(e.failed_items) && e.failed_items.length ? e.failed_items.slice(0, 50) : null;
      if (!fi) { const prev = resultOf(card); if (prev && prev.failed_items && prev.failed_items.length && e.commit && prev.commit === e.commit) fi = prev.failed_items; }   // 同一版本後來的事件（如 rejected）沒帶紅項就沿用先前的
      const res = { kind, at: e.at ?? now(), commit: e.commit ?? null, detail: detail || null, from: e.from ?? null, to: e.to ?? null, failed_items: fi };
      await db.prepare('UPDATE cards SET last_result=? WHERE id=?').bind(JSON.stringify(res), card.id).run();
    }
    await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)')
      .bind(card.id, now(), source, `${kind}${to ? ` → ${to}` : ''}${detail ? `：${detail}` : ''}`).run();
    // 通知：被排隊器退回、內容變了 → 領卡的開發者；暫停、父卡擋住 → 管理者
    if (ADMIN_KINDS.has(kind)) await post(admins.length ? admins : [card.owner_thread || ''], card, kind, branch, detail);
    else if (ATTENTION.has(kind)) await post([card.owner_thread || ''], card, kind, branch, detail);
  }
  if (ctx && ctx.waitUntil) ctx.waitUntil(autoFlush(env));
  return json({ ok: true, applied, unmatched, duplicate: dup, stale });
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
// 叫醒失敗的原因記一筆（只留最近 50 筆），查問題用
async function fireLog(db, t, status, body) {
  try {
    await db.prepare('INSERT INTO fire_log(at,status,body) VALUES(?,?,?)').bind(t, status, String(body).slice(0, 300)).run();
    await db.prepare('DELETE FROM fire_log WHERE n NOT IN (SELECT n FROM fire_log ORDER BY n DESC LIMIT 50)').run();
  } catch {}
}

export async function flush(env) {
  if (!env.ROUTINE_ID || !env.ROUTINE_KEY) return { skipped: 'not configured' };
  const db = env.DB, t = now();
  const rows = (await db.prepare('SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY n LIMIT 200').all()).results;
  if (!rows.length) return { sent: 0 };
  if (t - Math.min(...rows.map(r => r.at)) < Number(env.WAKE_BATCH_S ?? 120)) return { waiting: true };
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
      if (!ok) await fireLog(db, t, res.status, await res.text().catch(() => ''));
    } catch (e) { ok = false; await fireLog(db, t, 0, String(e && e.message || e)); }
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
    if (!['developing', 'rejected', 'reviewing'].includes(st)) continue;
    const detail = `${{ developing: '開發中', rejected: '退回', reviewing: '審查中' }[st]}超過 ${hours} 小時沒動靜（負責串 ${c.owner_thread || '無'}）`;
    for (const o of owners) await env.DB.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)').bind(o, c.id, c.branch, 'stuck', detail, t).run();
    await env.DB.prepare('UPDATE cards SET stuck_notified_at=? WHERE id=?').bind(t, c.id).run();
    n++;
  }
  return { notified: n };
}

// 主動叫醒（routine fire）預設關閉：外部 token 打排程任務可能被擋。要開才設 FIRE_ENABLED=1。預設改由收信端自己來領（/api/outbox/claim）。
export async function autoFlush(env) { return env.FIRE_ENABLED === '1' ? flush(env) : { skipped: 'fire disabled' }; }

// ---- 收信：傳令串自己來領。每個收件串一則（已合併好），領了標記時間；5 分鐘內沒 ack 會再給一次；ack 過的不再給。 ----
const NEXT = {
  drop_rejected: '要放棄請先把內容接過去（接手的卡要包含缺的檔），或跟總指揮說真的不要（帶 discard_reason）。卡已回到原階段。',
  drop_discarded: '請事後看一眼：卡號、不在 main 的改動筆數與放棄原因都在上面。',
  review_requested: '請看 git diff 審查；通過就在看板對這張卡 approve（帶審過的 commit），要改就 reject 並寫原因。',
  review_rejected: '審查退回：請依原因修改，改好 push 後對這張卡 submit 新版本。',
  tested_red: '自測紅：請看紅項修好，push 後 submit 新版本（若只是環境問題，請審查者 retest）。',
  rejected: '被排隊器退回：看原因修好 push 後 submit 新版本；沒改程式只想重跑，請審查者 retest。',
  conflict: '跟 main 撞檔：請 rebase 最新 main、解衝突後 push，再 submit 新版本。',
  superseded: '送審後內容又改了，放行作廢：請確認要審的版本，重新 submit。',
  parent_blocked: '父卡被放棄或不存在，子卡被擋：請管理者決定改接別張卡（PATCH parent）或直接接 main。',
  halted: '排隊器暫停待人處理：請管理者查排隊器，處理後對該卡 retest。',
  queue_error: '排隊器自己出狀況：請管理者查排隊器。',
  stuck: '這張卡很久沒動靜：請確認負責串是否還在做，必要時還卡或放棄。',
};
const CLAIM_TTL = 300;

export async function outboxClaim(req, env) {
  if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
  const body = await req.json().catch(() => ({}));
  const actor = String(body.actor ?? '').trim();
  if (!actor || actor.toLowerCase() === 'unknown') return json({ error: 'actor 必填' }, 400);
  const db = env.DB, t = now(), token = `${t}-${crypto.randomUUID()}`;
  await db.prepare('UPDATE outbox SET claimed_at=?,claim_id=? WHERE sent_at IS NULL AND (claimed_at IS NULL OR claimed_at<?)').bind(t, token, t - CLAIM_TTL).run();
  const rows = (await db.prepare('SELECT * FROM outbox WHERE sent_at IS NULL AND claim_id=? ORDER BY n').bind(token).all()).results;
  const fallback = list(env.ADMIN_THREADS)[0] || '';
  const groups = new Map();
  for (const r of rows) { const to = r.owner_thread || fallback; (groups.get(to) || groups.set(to, []).get(to)).push(r); }
  const out = [...groups].map(([to, items]) => ({
    to_thread: to || null,
    ids: items.map(i => i.n),
    kinds: [...new Set(items.map(i => i.kind))],
    card_ids: [...new Set(items.map(i => i.card_id).filter(Boolean))],
    created_at: Math.min(...items.map(i => i.at)),
    text: items.map(i => `【${i.card_id || '無卡'}】分支 ${i.branch || '（無）'}：${KIND_TEXT[i.kind] || i.kind}${i.detail ? `（${i.detail}）` : ''}\n  下一步：${NEXT[i.kind] || '請到看板查詳情。'}`).join('\n'),
  }));
  return json({ messages: out });
}

export async function outboxAck(req, env) {
  if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
  const body = await req.json().catch(() => ({}));
  const actor = String(body.actor ?? '').trim();
  if (!actor || actor.toLowerCase() === 'unknown') return json({ error: 'actor 必填' }, 400);
  const ids = (Array.isArray(body.ids) ? body.ids : []).map(Number).filter(Number.isInteger).slice(0, 500);
  if (!ids.length) return json({ error: 'ids 要是非空的數字陣列（claim 回傳的 ids）' }, 400);
  const r = await env.DB.prepare(`UPDATE outbox SET sent_at=? WHERE sent_at IS NULL AND n IN (${ids.map(() => '?').join(',')})`).bind(now(), ...ids).run();
  return json({ acked: r.meta.changes });
}

// 只讀：現在領得到幾筆（沒 ack 且沒被領走，或領走超過 5 分鐘沒 ack）與其中最大的信箱編號。給「有信才叫醒」的哨兵用，不改任何狀態。
export async function outboxPending(req, env) {
  if (req.method !== 'GET') return json({ error: 'GET only' }, 405);
  if (!bearerIs(req, env.HOOK_TOKEN, env.READ_TOKEN, env.TOKEN)) return json({ error: 'unauthorized' }, 401);
  const r = await env.DB.prepare('SELECT count(*) AS n, COALESCE(max(n),0) AS m FROM outbox WHERE sent_at IS NULL AND (claimed_at IS NULL OR claimed_at<?)').bind(now() - CLAIM_TTL).first();
  return json({ pending: r.n, latest_id: r.m });
}

// 卡被放棄後：領了這張卡當父卡的子卡被擋住，通知管理者
export async function notifyKids(env, id) {
  const admins = list(env.ADMIN_THREADS);
  const kids = (await env.DB.prepare("SELECT * FROM cards WHERE parent=? AND status='claimed'").bind(id).all()).results;
  for (const k of kids) for (const o of (admins.length ? admins : [k.owner_thread || ''])) await env.DB.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)').bind(o, k.id, k.branch, 'parent_blocked', `父卡 ${id} 被放棄，請改接別張卡或直接接 main`, now()).run();
}
