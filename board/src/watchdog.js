// 串名冊與閒置看門狗（JET-17）。
// 串名冊：全專案唯一一份，總指揮維護，雲端串 POST /api/roster 推上來（看板存在 meta 'roster'）；看板的審查串清單、看門狗都讀它，
// 沒有名冊時退回環境變數 REVIEW_THREADS／ADMIN_THREADS（原行為）。
// 看門狗（cron 每 5 分鐘）：
//   1. 送審超過 15 分鐘沒人審 → 叫「這張卡首字母對應的審查串」；超過 30 分鐘 → 再叫審查四（role 補位，letters='*'）。同一個送審版本各只叫一次。
//   2. 開發串手上沒有在做的卡（developing／rejected）、而且 15 分鐘沒動 → 叫它開工並附下一張可領的卡號；看板沒有可領的卡 → 叫總指揮補卡。
//      同一個開發串最多每 30 分鐘叫一次。
import { stageOf } from './stage.js';

const now = () => Math.floor(Date.now() / 1000);
const list = v => String(v || '').split(',').map(x => x.trim()).filter(Boolean);
export const REVIEW_WAIT_S = 15 * 60, REVIEW_ESCALATE_S = 30 * 60, DEV_IDLE_S = 15 * 60, DEV_REPEAT_S = 30 * 60;

export async function loadRoster(db) {
  const r = await db.prepare("SELECT value, updated_at FROM meta WHERE key='roster'").first();
  if (!r) return null;
  try { const j = JSON.parse(r.value); return j && typeof j === 'object' ? { ...j, _stored_at: r.updated_at } : null; } catch { return null; }
}

// 驗證並整理名冊；回 [roster, error]
export function cleanRoster(body) {
  const src = body && typeof body.roster === 'object' && body.roster ? body.roster : body;
  if (!src || typeof src !== 'object') return [null, '要帶名冊（reviewers／devs…）'];
  const str = (v, n = 200) => String(v ?? '').trim().slice(0, n);
  const person = (x, needLetters) => {
    if (!x || typeof x !== 'object' || !str(x.thread)) return null;
    const p = { name: str(x.name, 60), thread: str(x.thread) };
    if (needLetters) p.letters = str(x.letters || '*', 40);
    if (x.role) p.role = str(x.role, 200);
    return p;
  };
  const arr = (a, needLetters) => (Array.isArray(a) ? a : []).slice(0, 60).map(x => person(x, needLetters)).filter(Boolean);
  const reviewers = arr(src.reviewers, true);
  if (!reviewers.length) return [null, 'reviewers 至少要有一位（每位要有 thread）'];
  const out = { updated: str(src.updated, 60) || new Date().toISOString(), commander: str(src.commander), reviewers, devs: arr(src.devs, false),
    others: arr(src.others, false), messengers: arr(src.messengers, false) };
  if (src._說明) out._說明 = str(src._說明, 600);
  return [out, null];
}

// 審查串清單：名冊優先，沒有就用環境變數
export const reviewerThreads = (env, roster) => (roster && roster.reviewers && roster.reviewers.length
  ? [...new Set(roster.reviewers.map(r => r.thread))] : list(env.REVIEW_THREADS));

// 「A-E」「F-N」「*」→ 這張卡（以卡號首字母）歸誰審；沒有範圍符合就回補位（letters='*'）
export function reviewerFor(roster, cardId) {
  const ch = String(cardId || '').trim().charAt(0).toUpperCase();
  const inRange = (spec) => String(spec || '').split(',').some(part => {
    const m = part.trim().toUpperCase().match(/^([A-Z])(?:\s*-\s*([A-Z]))?$/);
    return m && ch >= m[1] && ch <= (m[2] || m[1]);
  });
  const rs = (roster && roster.reviewers) || [];
  return rs.find(r => r.letters !== '*' && inRange(r.letters)) || rs.find(r => r.letters === '*') || rs[0] || null;
}

const getMeta = async (db, key) => { const r = await db.prepare('SELECT value FROM meta WHERE key=?').bind(key).first(); try { return r ? JSON.parse(r.value) : null; } catch { return null; } };
const setMeta = (db, key, val, t) => db.prepare("INSERT INTO meta(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at").bind(key, JSON.stringify(val), t);
const push = (db, owner, card, kind, branch, detail, t) => db.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)').bind(owner, card ? card.id : null, branch ?? (card ? card.branch : null), kind, String(detail).slice(0, 500), t);

export async function watchdog(env, tNow) {
  const db = env.DB, t = tNow ?? now();
  const roster = await loadRoster(db);
  if (!roster) return { skipped: 'no roster' };
  const stmts = [];
  let review = 0, dev = 0, nocard = 0;
  // ---- 1. 送審沒人審 ----
  const subs = (await db.prepare("SELECT * FROM cards WHERE status='claimed' AND submitted_commit IS NOT NULL AND approved_commit IS NULL AND rejected_reason IS NULL AND dropping_at IS NULL").all()).results;
  const fallback = (roster.reviewers || []).find(r => r.letters === '*');
  for (const c of subs) {
    if (stageOf(c) !== 'reviewing') continue;
    const age = t - (c.submitted_at || c.updated_at || t);
    if (age < REVIEW_WAIT_S) continue;
    const key = `wd_rev:${c.id}:${String(c.submitted_commit).slice(0, 12)}`;
    const done = Number((await getMeta(db, key)) || 0);
    const mine = reviewerFor(roster, c.id);
    const mins = Math.floor(age / 60);
    if (age >= REVIEW_WAIT_S && done < 1 && mine) {
      stmts.push(push(db, mine.thread, c, 'watchdog_review', c.branch, `卡 ${c.id} 送審 ${mins} 分鐘還沒人審（版本 ${String(c.submitted_commit).slice(0, 8)}）：請先審這張`, t), setMeta(db, key, 1, t));
      review++;
    }
    if (age >= REVIEW_ESCALATE_S && done < 2 && fallback && (!mine || fallback.thread !== mine.thread)) {
      stmts.push(push(db, fallback.thread, c, 'watchdog_review', c.branch, `卡 ${c.id} 送審 ${mins} 分鐘仍沒人審（本來歸${mine ? mine.name : '無人'}）：請補位審`, t), setMeta(db, key, 2, t));
      review++;
    }
  }
  // ---- 2. 開發串閒置 ----
  const open = (await db.prepare("SELECT * FROM cards WHERE status='open' AND dropping_at IS NULL ORDER BY priority,created_at LIMIT 20").all()).results;
  const claimed = (await db.prepare("SELECT * FROM cards WHERE status='claimed' AND owner_thread IS NOT NULL AND dropping_at IS NULL").all()).results;
  const taken = new Set();
  for (const d of roster.devs || []) {
    const mine = claimed.filter(c => c.owner_thread === d.thread);
    const doing = mine.some(c => ['developing', 'rejected'].includes(stageOf(c)));
    const wd = (await getMeta(db, `wd_dev:${d.thread}`)) || {};
    if (doing) { if (wd.since) stmts.push(setMeta(db, `wd_dev:${d.thread}`, { notified_at: wd.notified_at || 0 }, t)); continue; }
    const lastMove = mine.length ? Math.max(...mine.map(c => c.updated_at || 0)) : 0;
    const since = Math.max(wd.since || 0, lastMove) || t;
    if (!wd.since && !lastMove) { stmts.push(setMeta(db, `wd_dev:${d.thread}`, { since: t, notified_at: wd.notified_at || 0 }, t)); continue; }   // 第一次看到它沒事做：從現在開始算
    if (t - since < DEV_IDLE_S) { if (!wd.since && lastMove) stmts.push(setMeta(db, `wd_dev:${d.thread}`, { since: lastMove, notified_at: wd.notified_at || 0 }, t)); continue; }
    if (t - (wd.notified_at || 0) < DEV_REPEAT_S) continue;
    const next = open.find(c => !taken.has(c.id));
    if (next) {
      taken.add(next.id);
      stmts.push(push(db, d.thread, next, 'watchdog_dev', null, `${d.name}：你手上沒有在做的卡已超過 ${Math.floor((t - since) / 60)} 分鐘，請領下一張：${next.id}（${String(next.title).slice(0, 60)}）`, t));
      dev++;
    } else if (roster.commander) {
      stmts.push(push(db, roster.commander, null, 'watchdog_nocard', null, `開發串 ${d.name} 手上沒卡已超過 ${Math.floor((t - since) / 60)} 分鐘，看板沒有可領的新卡：請補卡`, t));
      nocard++;
    }
    stmts.push(setMeta(db, `wd_dev:${d.thread}`, { since, notified_at: t }, t));
  }
  for (let i = 0; i < stmts.length; i += 40) await db.batch(stmts.slice(i, i + 40));
  return { review, dev, nocard };
}
