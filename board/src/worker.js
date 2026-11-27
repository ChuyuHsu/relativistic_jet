// 任務卡服務：Cloudflare Worker + D1。env.DB 是 D1，env.TOKEN 是寫入權杖。
import { bearerIs } from './auth.js';
import { loadRoster, cleanRoster, reviewerThreads, reviewerFor, watchdog } from './watchdog.js';
import { loadRedirects, resolveThread, handleHook, notifyKids, autoFlush, outboxClaim, outboxAck, outboxRelease, outboxPending, outboxSignal, resultOf, stuckCheck, housekeeping } from './queue.js';
import { stageOf, sameCommit, STAGES } from './stage.js';
// 卡片自己只管前段與結局：open 空、claimed 已認領、merged 已合併、dropped 放棄；放行是卡片上的欄位（approved_*）。
// 中段（待自測、自測綠、排隊、合入、退回…）是排隊器的分支狀態，由排隊器的事件（/hooks/queue）寫進 phase 與 last_result，
// 排隊器自己定時來拉 /api/queue/desired 取得該管哪些分支與放行；看板不主動連排隊器。
const STATUSES = ['open', 'claimed', 'dropped'];
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8' } });
const now = () => Math.floor(Date.now() / 1000);
// 分支名由看板給定：<前綴><卡號小寫>，第二次起加 -r<次數>。前綴可用 BRANCH_PREFIX 設定（預設 dev/）。
const branchFor = (env, id, attempt = 1) =>
  `${env.BRANCH_PREFIX ?? 'dev/'}${String(id).toLowerCase().replace(/[^a-z0-9._-]+/g, '-')}${attempt > 1 ? `-r${attempt}` : ''}`;
// 父卡資訊給排隊器：父卡已合併就省略；父卡不在清單上（還沒認領、已釋放、已放棄、不存在）也要給 parent 與 parent_status，
// 子卡才知道要等（open）或被擋（dropped）。
function parentInfo(env, c, by) {
  if (!c.parent) return null;
  const p = by.get(c.parent);
  if (p && p.phase === 'merged') return null;
  const onList = p && p.status === 'claimed' && p.branch;
  const branch = p ? (p.branch || branchFor(env, p.id)) : branchFor(env, c.parent);
  if (onList) return { parent: branch };
  return { parent: branch, parent_status: p && p.status !== 'dropped' ? 'open' : 'dropped' };
}
const view = c => {
  const result = resultOf(c), stage = stageOf(c);
  let rejected = null;   // 退回的來源與原因（審查退回，或排隊器 rejected／halted／撞檔，附紅項）
  if (stage === 'rejected') {
    rejected = c.rejected_reason
      ? { source: 'review', reason: c.rejected_reason, by: c.rejected_by }
      : { source: 'queue', halted: c.phase === 'halted', reason: result ? result.detail : null, failed_items: result ? result.failed_items : null };
  }
  const dropped = ['dropping', 'dropped', 'waiting_parent'].includes(stage) ? { superseded_by: c.superseded_by ?? null, discard_reason: c.discard_reason ?? null, unmerged: c.drop_unmerged == null ? null : !!c.drop_unmerged, checked_at: c.drop_checked_at ?? null } : null;
  return { ...c, last_result: undefined, result, stage, rejected, dropped };
};
const CLEAR_REVIEW = 'submitted_commit=NULL,submitted_at=NULL,rejected_reason=NULL,rejected_by=NULL,rejected_at=NULL,approved_commit=NULL,approved_by=NULL,approved_at=NULL';
const HEX = /^[0-9a-f]{7,64}$/i;
// 通知名單（用逗號分隔的負責串 id 設定）攢批叫醒
async function notify(db, owners, c, kind, detail) {
  for (const o of owners) await db.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)').bind(o, c.id, c.branch, kind, detail, now()).run();
}
const list = v => String(v || '').split(',').map(x => x.trim()).filter(Boolean);

// 附上父卡資訊：parent_stage（父卡現在的階段，不存在回 missing）、waiting_parent（父卡還沒合併，子卡就算放行也要等）
async function withParent(db, cards, pool) {
  const need = cards.filter(c => c.parent);
  if (!need.length) return cards;
  const by = new Map((pool || []).map(r => [r.id, r]));
  // 父卡不在手上的（例如已合併、列表預設不回）用 IN 補查，一次最多 80 個
  const ids = [...new Set(need.map(c => c.parent))].filter(id => !by.has(id));
  for (let i = 0; i < ids.length; i += 80) {
    const ch = ids.slice(i, i + 80);
    for (const r of (await db.prepare(`SELECT * FROM cards WHERE id IN (${ch.map(() => '?').join(',')})`).bind(...ch).all()).results) by.set(r.id, r);
  }
  return cards.map(c => {
    if (!c.parent) return c;
    const p = by.get(c.parent), ps = p ? stageOf(p) : 'missing';
    return { ...c, parent_stage: ps, waiting_parent: ps !== 'merged', parent_blocked: ps === 'dropped' || ps === 'missing' };
  });
}

// 列表的 SQL 預篩：每個階段對到哪些 status／欄位（最後仍用 stageOf 精確比對）
const STAGE_SQL = { merged: "(status='merged' OR phase='merged')", dropped: "status='dropped'", waiting_parent: "status='waiting'", dropping: 'dropping_at IS NOT NULL' };
const ACTIVE_SQL = "(status IN ('open','claimed','waiting') OR (dropping_at IS NOT NULL AND status NOT IN ('dropped','merged')))";   // 預設：進行中（不含已合併、已放棄）
const sqlForStages = ss => ss.length ? `(${[...new Set(ss.map(x => STAGE_SQL[x] ?? "status IN ('open','claimed')"))].join(' OR ')})` : ACTIVE_SQL;

// 版本號條件式讀取（BD-03）：meta.rev 由資料庫觸發器在任何卡片、流程狀態、效能表變動時加一。
// 輪詢的人帶 If-None-Match 或 ?rev=，版本沒變就只讀 rev 一列、回 304；回應的 etag 用「產生答案之前」讀到的版本，
// 產生中途有寫入也只會讓下一次多拉一次，不會拿到舊資料。權杖必須在呼叫前已檢查。
async function revGate(req, env, produce) {
  let r;
  try { r = await env.DB.prepare("SELECT value FROM meta WHERE key='rev'").first(); } catch (err) { if (isQuota(err)) throw err; return json({ error: '看板內部錯誤' }, 500); }
  const rev = r ? String(r.value) : null;
  if (rev == null) return produce();   // 還沒跑 migrate_016：不支援，照常回答
  const etag = `W/"${rev}"`;
  const inm = req.headers.get('if-none-match'), q = new URL(req.url).searchParams.get('rev');
  if ((inm && inm.split(',').some(x => x.trim() === etag || x.trim() === `"${rev}"`)) || q === rev) return new Response(null, { status: 304, headers: { etag, 'cache-control': 'no-cache' } });
  const res = await produce();
  if (res.status === 200) { const h = new Headers(res.headers); h.set('etag', etag); h.set('cache-control', 'no-cache'); return new Response(res.body, { status: 200, headers: h }); }
  return res;
}

// 設父卡：不能是自己、不存在、已放棄，也不能成環（沿父鏈走回自己）。回錯誤訊息或 null。
async function badParent(db, id, parent) {
  if (parent == null || parent === '') return null;
  parent = String(parent);
  if (parent === id) return '父卡不能是自己';
  const p = await db.prepare('SELECT id,status,parent FROM cards WHERE id=?').bind(parent).first();
  if (!p) return '父卡不存在';
  if (p.status === 'dropped') return '父卡已放棄';
  let cur = p, hops = 0;
  while (cur && cur.parent && hops++ < 100) {
    if (cur.parent === id) return '父卡會造成循環';
    cur = await db.prepare('SELECT id,parent FROM cards WHERE id=?').bind(cur.parent).first();
  }
  return null;
}

// 放棄理由要寫清楚：只寫卡號或太短的不算（要寫被哪張取代、哪個提交已在 main、或哪項定案撤掉）
function reasonProblem(why) {
  const t = String(why || '').trim();
  if (/父卡|等[^，。；]{0,8}合併|等待/.test(t)) return '放棄理由不能是「等父卡／等待合併」這類：還在等的卡請帶 superseded_by（接手卡），接手卡進 main 前會列在「等待父卡」，進 main 後自動結案；放棄只給真的不再需要的卡';
  if (t.length < 10 || /^[A-Za-z0-9_.\/-]+$/.test(t)) return 'discard_reason 要寫清楚為什麼這張卡本身不再需要（被哪張取代、哪個提交已在 main、或哪項定案撤掉）；只寫卡號或太短的不算，父卡放棄也不是理由';
  return null;
}
async function getTriage(db) {
  const r = await db.prepare("SELECT value FROM meta WHERE key='triage'").first();
  try { return r ? JSON.parse(r.value) : null; } catch { return null; }
}
async function log(db, id, actor, note) {
  await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)').bind(id, now(), actor, note).run();
}

// 給排隊器定時拉：所有「已認領、有分支、還沒合入」的卡 = 排隊器該管的分支（已放棄、釋放的不在內）。出錯一律回非 200。
async function desired(req, env) {
  if (req.method !== 'GET') return json({ error: 'GET only' }, 405);
  if (!bearerIs(req, env.HOOK_TOKEN, env.READ_TOKEN, env.TOKEN)) return json({ error: 'unauthorized' }, 401);
  try {
    // 一次把卡讀進記憶體再算（原本每張卡多查一兩次 D1，卡多了會超過 10 秒）
    // 只讀要用到的卡（用索引）：已認領的、放棄待查的，再補它們的父卡與接手卡
    const mine = (await env.DB.prepare("SELECT * FROM cards WHERE status='claimed' ORDER BY priority,created_at").all()).results;
    const withDrops = new URL(req.url).searchParams.get('with') === 'drop_checks';
    const dropRows = withDrops ? (await env.DB.prepare("SELECT * FROM cards WHERE dropping_at IS NOT NULL AND status!='dropped'").all()).results : [];
    const all = mine.concat(dropRows.filter(d => !mine.some(m => m.id === d.id)));
    const by = new Map(all.map(c => [c.id, c]));
    const want = [...new Set(all.flatMap(c => [c.parent, c.superseded_by]).filter(x => x && !by.has(x)))];
    for (let i = 0; i < want.length; i += 80) {
      const ch = want.slice(i, i + 80);
      for (const r of (await env.DB.prepare(`SELECT * FROM cards WHERE id IN (${ch.map(() => '?').join(',')})`).bind(...ch).all()).results) by.set(r.id, r);
    }
    const out = [];
    for (const c of mine) {
      if (!(c.status === 'claimed' && c.branch && c.phase !== 'merged' && !c.dropping_at)) continue;
      const e = { branch: c.branch };
      const p = parentInfo(env, c, by);
      if (p) Object.assign(e, p);
      if (c.approved_commit) e.approved_sha = c.approved_commit;
      e.retest = c.retest;
      e.priority = c.priority;   // 數字小的先（跟看板排序一致）；排隊器拿來排自測與批測順序
      out.push(e);
    }
    if (withDrops) {   // 放棄待查：排隊器要查這些分支有沒有改動漏掉
      const drop_checks = dropRows.sort((a, b) => a.dropping_at - b.dropping_at).map(c => {
        const sc = c.superseded_by ? by.get(c.superseded_by) : null;
        return { card: c.id, branch: c.branch || branchFor(env, c.id), superseded_by_branch: sc ? (sc.branch || branchFor(env, sc.id)) : null, dropping_at: c.dropping_at };   // dropping_at 請原樣帶回事件，用來認是哪一次放棄
      });
      const tri = await getTriage(env.DB);   // 總指揮判斷出的嫌疑分支：排隊器只驗證一次（與 cards、drop_checks 同層）
      const loop_suspect = tri && tri.status === 'answered' && tri.verdict === 'suspects' && tri.suspects && tri.suspects.length ? { main_sha: tri.main_sha, branch: tri.suspects[0].branch } : null;
      return json(loop_suspect ? { cards: out, drop_checks, loop_suspect } : { cards: out, drop_checks });
    }
    return json(out);
  } catch (err) { if (isQuota(err)) throw err; return json({ error: '看板內部錯誤' }, 500); }
}

// 資料庫額度（讀取列數、寫入列數）用完：回明確的 503 與原因，不要讓 Cloudflare 回 1101（各串看不懂）。其他錯誤照舊往上丟。
const QUOTA = /(D1|database)[^]{0,120}(limit|quota|exceed)|(exceed|reach)[^]{0,60}(rows? (read|written)|daily|free)|rows_(read|written)/i;
const isQuota = err => QUOTA.test(String((err && (err.message || err.cause?.message)) || err));
export async function handle(req, env, ctx) {
  try { return await handleInner(req, env, ctx); }
  catch (err) {
    if (isQuota(err)) return new Response(JSON.stringify({ error: 'd1_quota', detail: '資料庫額度用完，等額度重置或升級後再試' }), { status: 503, headers: { 'content-type': 'application/json; charset=utf-8', 'retry-after': '3600' } });
    throw err;
  }
}
const GATED = new Set(['/api/cards', '/api/workflow', '/api/stats', '/api/perf', '/api/nightly']);   // 帶 etag、版本沒變回 304 的讀取端點
async function handleInner(req, env, ctx, gated = false) {
  const url = new URL(req.url);
  const path = url.pathname;
  const db = env.DB;
  if (!gated && req.method === 'GET' && GATED.has(path)) {
    if (!bearerIs(req, env.TOKEN, env.READ_TOKEN)) return json({ error: 'unauthorized' }, 401);
    return revGate(req, env, () => handleInner(req, env, ctx, true));
  }
  if (path === '/hooks/queue') return handleHook(req, env, ctx);
  const write = req.method !== 'GET';
  // 排隊器拉清單：HOOK_TOKEN 或 READ_TOKEN；其餘寫入要 TOKEN，讀取要 TOKEN 或 READ_TOKEN
  if (path === '/api/queue/desired') {
    if (!bearerIs(req, env.HOOK_TOKEN, env.READ_TOKEN, env.TOKEN)) return json({ error: 'unauthorized' }, 401);
    return revGate(req, env, () => desired(req, env));   // 排隊器輪詢：版本沒變回 304
  }
  if (path === '/api/outbox/pending') return outboxPending(req, env);
  if (path === '/api/outbox/signal') return outboxSignal(req, env);   // 公開唯讀、只回數字（傳令串訂閱用）
  if (path === '/api/triage' && req.method === 'GET') {   // 排隊器讀：目前的紅燈判斷（含總指揮的回報）
    if (!bearerIs(req, env.HOOK_TOKEN, env.READ_TOKEN, env.TOKEN)) return json({ error: 'unauthorized' }, 401);
    return json({ triage: await getTriage(db) });
  }
  if (!(write ? bearerIs(req, env.TOKEN) : bearerIs(req, env.TOKEN, env.READ_TOKEN))) return json({ error: 'unauthorized' }, 401);
  if (path === '/api/outbox/claim' || path === '/api/outbox/ack' || path === '/api/outbox/release') return (path.endsWith('claim') ? outboxClaim : path.endsWith('ack') ? outboxAck : outboxRelease)(req.clone(), env);
  if (req.method === 'GET' && path === '/api/workflow') {   // 目前流程模式（gate_batch｜direct_merge）與背景循環狀態；排隊器用 workflow_status 事件回報
    const r = await db.prepare("SELECT value,updated_at FROM meta WHERE key='workflow'").first();
    let wf = null; try { wf = r ? JSON.parse(r.value) : null; } catch { /* 壞值當沒有 */ }
    const hotRows = (await db.prepare("SELECT * FROM cards WHERE area='hotfix' AND phase IS NOT 'merged' AND status!='dropped' ORDER BY created_at").all()).results;
    const hot = hotRows.map(c => ({ id: c.id, title: c.title, status: c.status, owner: c.owner, priority: c.priority }));
    const mr = await db.prepare("SELECT value,updated_at FROM meta WHERE key='main_red'").first();
    let main_red = null; try { main_red = mr ? JSON.parse(mr.value) : null; } catch { /* 壞值當沒有 */ }
    const triage = await getTriage(db);
    // main 現在的紅項，以及每個紅項誰在修：急修卡（內文提到該紅項）、被指出的嫌疑分支
    const itemName = f => String((f && (f.item ?? f.item_id ?? f.name)) ?? f);
    const redList = (main_red && main_red.failed_items && main_red.failed_items.length ? main_red.failed_items : (triage && triage.failed_items) || []);
    const red_items = redList.slice(0, 50).map(f => {
      const item = itemName(f);
      return { item, summary: f && typeof f === 'object' && f.summary ? String(f.summary).slice(0, 300) : null,
        fixers: hotRows.filter(c => item && new RegExp(`(^|[^A-Za-z0-9_.-])${item.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}($|[^A-Za-z0-9_.-])`).test(String(c.body || ''))).map(c => ({ id: c.id, stage: stageOf(c), owner: c.owner, owner_thread: c.owner_thread })),
        suspects: triage && triage.suspects ? triage.suspects.map(s => s.branch) : [] };
    });
    return json({ mode: wf?.mode ?? null, loop: wf?.loop ?? null, main_red, red_items, triage: triage ? { main_sha: triage.main_sha, status: triage.status, verdict: triage.verdict ?? null, suspects: triage.suspects ?? null } : null, reported_at: r?.updated_at ?? null, open_hotfix: hot });
  }
  let m;
  if (req.method === 'GET' && path === '/api/cards') {
    // 預設只回進行中的卡（open／claimed／waiting 與放棄待查）；?all=1 回全部；?stage=merged,dropped 指名歷史；?status= 照舊
    const sp = url.searchParams, status = sp.get('status'), all = sp.get('all') === '1';
    const want = sp.get('stage') ? sp.get('stage').split(',').filter(Boolean) : [];
    let sql = 'SELECT * FROM cards', args = [];
    if (status) { sql += ' WHERE status=?'; args = [status]; }
    else if (!all) sql += ' WHERE ' + sqlForStages(want);
    const rows = await db.prepare(sql + ' ORDER BY priority,created_at').bind(...args).all();
    let out = (await withParent(db, rows.results.map(view), rows.results));
    if (want.length) { const w = new Set(want); out = out.filter(c => w.has(c.stage)); }
    else if (!all && !status) out = out.filter(c => c.stage !== 'merged' && c.stage !== 'dropped');
    return json(out);
  }
  if (path === '/api/roster' && req.method === 'GET') {   // 串名冊（審查串、開發串、傳令；總指揮維護、雲端串推上來）；沒有時回 null
    const r = await loadRoster(db);
    return json(r ? { ...r, stored_at: r._stored_at, _stored_at: undefined } : null);
  }
  if (path === '/api/roster' && req.method === 'POST') {   // 整份取代：{actor, roster:{commander,reviewers:[{name,thread,letters}],devs,others,messengers}}（或直接帶名冊本體）
    const body = await req.clone().json().catch(() => ({}));
    if (!String(body.actor ?? '').trim() || String(body.actor).toLowerCase() === 'unknown') return json({ error: 'actor 必填' }, 400);
    const [roster, err] = cleanRoster(body);
    if (err) return json({ error: err }, 400);
    await db.prepare("INSERT INTO meta(key,value,updated_at) VALUES('roster',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at").bind(JSON.stringify(roster), now()).run();
    return json({ ok: true, reviewers: roster.reviewers.length, devs: roster.devs.length, messengers: roster.messengers.length });
  }
  if (path === '/api/redirect' && req.method === 'GET') {   // 目前的串號轉址表
    return json((await db.prepare('SELECT from_thread AS "from",to_thread AS "to",at FROM thread_redirect ORDER BY at DESC').all()).results);
  }
  if (path === '/api/redirect' && req.method === 'POST') {   // 登記轉址：{from,to,actor}；同時把未結案卡片的 owner_thread 改成新串
    const body = await req.clone().json().catch(() => ({}));
    const from = String(body.from ?? '').trim(), to = String(body.to ?? '').trim();
    if (!String(body.actor ?? '').trim() || String(body.actor).toLowerCase() === 'unknown') return json({ error: 'actor 必填' }, 400);
    if (!from || !to) return json({ error: 'from 與 to 都要填' }, 400);
    if (from === to) return json({ error: 'from 不能等於 to' }, 400);
    const map = await loadRedirects(db);
    const dest = resolveThread(map, to);
    if (dest === from) return json({ error: '這樣會繞圈（to 最後又轉回 from）' }, 409);
    const t = now();
    await db.prepare('INSERT INTO thread_redirect(from_thread,to_thread,at) VALUES(?,?,?) ON CONFLICT(from_thread) DO UPDATE SET to_thread=excluded.to_thread,at=excluded.at').bind(from, to, t).run();
    // 所有指向 from 的舊轉址不用動（解析時會一路走到新的）；卡片直接改成最終的串
    const r = await db.prepare("UPDATE cards SET owner_thread=?,updated_at=? WHERE owner_thread=? AND status IN ('open','claimed','waiting') RETURNING id").bind(dest, t, from).all();
    return json({ from, to, resolved: dest, cards_updated: r.results.length });
  }
  if (path === '/api/nightly' && req.method === 'GET') {   // 每晚完整層：最新一次與歷史（最近 14 次）；沒有結果時 latest 是 null
    const rows = (await db.prepare('SELECT * FROM nightly ORDER BY n DESC LIMIT 14').all()).results;
    const one = r => { const j = v => { try { return JSON.parse(v || '[]'); } catch { return []; } }; return { at: r.at, main_sha: r.main_sha, started_at: r.started_at, finished_at: r.finished_at, total: r.total, red: j(r.red), timeouts: j(r.timeouts) }; };
    const hist = rows.map(one);
    return json({ latest: hist[0] ?? null, history: hist });
  }
  if (req.method === 'GET' && path === '/api/stats') {   // 各階段張數（頁首用）：進行中的卡算出階段，已合併與已放棄只數張數不讀內容
    const act = (await db.prepare('SELECT * FROM cards WHERE ' + ACTIVE_SQL).all()).results;
    const stages = Object.fromEntries(STAGES.map(k => [k, 0]));
    for (const c of act) stages[stageOf(c)]++;
    for (const r of (await db.prepare("SELECT status, count(*) AS n FROM cards WHERE status IN ('merged','dropped') GROUP BY status").all()).results) stages[r.status] += r.n;
    return json({ stages, total: Object.values(stages).reduce((a, b) => a + b, 0) });
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
    return json({ ...(await withParent(db, [view(c)]))[0], events: ev.results });
  }
  const body = write ? await req.json().catch(() => ({})) : {};
  let actor = String(body.actor ?? '').trim();
  if (write && path !== '/api/claim' && (!actor || actor.toLowerCase() === 'unknown')) return json({ error: 'actor 必填（誰做的），不能空白或 unknown' }, 400);
  if (path === '/api/claim') actor = actor && actor.toLowerCase() !== 'unknown' ? actor : String(body.owner || '').trim();
  // ---- 卡片動作（轉移表見 docs/CARD_FLOW.md）。表外的動作一律 409 並回現在的 stage；同一動作重送結果一樣。----
  if (req.method === 'POST' && (m = path.match(/^\/api\/cards\/([^/]+)\/(submit|reject|approve|revoke|retest)$/))) {
    const c = await db.prepare('SELECT * FROM cards WHERE id=?').bind(m[1]).first();
    if (!c) return json({ error: 'not found' }, 404);
    const stage = stageOf(c), act = m[2];
    const no = msg => json({ error: msg || `現在是「${stage}」，不能${act}`, stage }, 409);
    const done = async note => { await log(db, c.id, actor, note); return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(c.id).first())); };
    // 條件式更新：只有卡片還是讀到的那個狀態／送審版本／放行版本／重測次數時才寫入，輸家得到 409（不會出現放行版本≠送審版本）
    const upd = async (set, ...vals) => {
      // 用 RETURNING 數列，不看 meta.changes：D1 的 changes 會把觸發器（rev）改的列也算進去，贏家會被當成輸家
      const r = await db.prepare(`UPDATE cards SET ${set},updated_at=? WHERE id=? AND status=? AND IFNULL(submitted_commit,'')=? AND IFNULL(approved_commit,'')=? AND retest=? AND IFNULL(rejected_reason,'')=? RETURNING id`)
        .bind(...vals, now(), c.id, c.status, c.submitted_commit ?? '', c.approved_commit ?? '', c.retest, c.rejected_reason ?? '').all();
      return r.results.length === 1;
    };
    const lost = () => json({ error: '同時有別人改了這張卡，請重讀後再試', stage }, 409);
    const QUEUE_CLEAR = 'phase=NULL,phase_at=NULL,last_result=NULL';
    const APPROVAL_CLEAR = 'approved_commit=NULL,approved_by=NULL,approved_at=NULL';
    if (act === 'submit') {
      if (!HEX.test(String(body.commit || ''))) return json({ error: '送審要帶提交 hash（commit，7 到 64 碼十六進位）' }, 400);
      if (!['developing', 'reviewing', 'selftesting', 'batching', 'rejected'].includes(stage)) return no();
      if (c.submitted_commit && sameCommit(String(body.commit), c.submitted_commit)) {
        if (stage !== 'rejected') return json(view(c));   // 同一版本重送：結果一樣
        return no('版本沒變。被排隊器退回要重測請用 retest；被審查退回請改好再送審新版本');
      }
      if (!await upd(`submitted_commit=?,submitted_at=?,rejected_reason=NULL,rejected_by=NULL,rejected_at=NULL,${APPROVAL_CLEAR},${QUEUE_CLEAR}`, String(body.commit), now())) return lost();
      // 把上次被退回的原因帶進審查通知：審查要先確認它真的改掉再放行（送審會清掉 rejected_reason，所以從紀錄找最近一筆審查退回）
      const lastRej = c.rejected_reason ? { note: `審查退回：${c.rejected_reason}` } : await db.prepare("SELECT note FROM events WHERE card_id=? AND note LIKE '審查退回：%' ORDER BY n DESC LIMIT 1").bind(c.id).first();
      const prev = lastRej ? `｜上次退回原因（請確認已改掉才放行）：${String(lastRej.note).replace(/^審查退回：/, '').slice(0, 300)}` : '';
      const ros = await loadRoster(db);       // 有名冊：只寄給卡號首字母對應的那條審查串（補位審查只由看門狗 30 分鐘時叫）；沒名冊用環境變數全寄
      const to = ros && ros.reviewers && ros.reviewers.length ? [reviewerFor(ros, c.id).thread] : reviewerThreads(env, ros);
      await notify(db, to, c, 'review_requested', String(body.commit).slice(0, 12) + prev);
      return done(`送審 ${String(body.commit).slice(0, 12)}`);
    }
    if (act === 'reject') {
      if (!body.reason) return json({ error: '退回要寫原因（reason）' }, 400);
      const reason = String(body.reason).slice(0, 1000);
      if (stage === 'rejected' && c.rejected_reason === reason) return json(view(c));
      if (!['reviewing', 'selftesting', 'batching'].includes(stage)) return no();
      if (!await upd(`rejected_reason=?,rejected_by=?,rejected_at=?,${APPROVAL_CLEAR},${QUEUE_CLEAR}`, reason, actor, now())) return lost();
      if (c.owner_thread) await notify(db, [c.owner_thread], c, 'review_rejected', reason);
      return done(`審查退回：${reason}`);
    }
    if (act === 'approve') {
      if (!HEX.test(String(body.commit || ''))) return json({ error: '放行要帶審過的提交 hash（commit，7 到 64 碼十六進位）' }, 400);
      if (['selftesting', 'batching'].includes(stage) && sameCommit(String(body.commit), c.approved_commit)) return json(view(c));   // 重送：結果一樣
      if (stage !== 'reviewing') return no();
      if (!sameCommit(String(body.commit), c.submitted_commit)) return no(`放行的版本與送審的版本不同（送審 ${c.submitted_commit.slice(0, 12)}），送審後可能又有新版本`);
      if (c.owner && actor === c.owner) return no('放行的人不能是領卡的人');
      // 排隊器要完整 hash 才認：放行帶短 hash 時，若送審的是完整 hash 且前綴對得上，就存送審的完整版；兩邊都是短的就拒收
      const fullHash = String(body.commit).length >= 40 ? String(body.commit) : (c.submitted_commit && c.submitted_commit.length >= 40 ? c.submitted_commit : null);
      if (!fullHash) return json({ error: '放行要帶完整的提交 hash（40 碼）：排隊器不認短 hash，短的會讓卡默默卡住', stage }, 400);
      if (!await upd(`approved_commit=?,approved_by=?,approved_at=?,${QUEUE_CLEAR}`, fullHash, actor, now())) return lost();
      return done(`放行 ${fullHash.slice(0, 12)}`);
    }
    if (act === 'revoke') {
      if (stage === 'reviewing') return json(view(c));
      if (!['selftesting', 'batching'].includes(stage)) return no();
      if (!await upd(`${APPROVAL_CLEAR},${QUEUE_CLEAR}`)) return lost();
      return done('撤回放行');
    }
    // retest：只有被排隊器退回（含暫停）的卡；放行保留、重測計數加一、清排隊器結果，回到審查通過自測中
    // force：看板沒收到排隊器的退回（事件被當過期丟掉）而卡在自測中／批測中時，強制重測：重測計數加一，排隊器看到計數變大就從待自測重來
    const forced = body.force === true && (stage === 'selftesting' || stage === 'batching') && !c.rejected_reason;
    if (!forced && (stage !== 'rejected' || c.rejected_reason)) return no(c.rejected_reason ? '被審查退回的卡不能重測，要改好再送審' : undefined);
    if (!await upd(`retest=retest+1,${QUEUE_CLEAR}`)) return lost();
    return done(`重測（第 ${c.retest + 1} 次）`);
  }
  if (path === '/api/perf' && req.method === 'GET') {   // 背景測試效能：最近各輪總耗時與 CPU 使用率、最慢項目排行（最近 N 輪平均與最大）
    const n = Math.min(Math.max(Number(url.searchParams.get('n')) || 30, 1), 200);
    const rows = (await db.prepare('SELECT * FROM loop_perf ORDER BY n DESC LIMIT ?').bind(n).all()).results;
    const runs = rows.map(r => { let top = []; try { top = JSON.parse(r.top || '[]'); } catch { /* 壞值當空 */ } return { at: r.at, main_sha: r.main_sha, kind: r.kind, wall_s: r.wall_s, cores: r.cores, cpu_util: r.cpu_util, items: r.items, top, metrics: (() => { try { return r.metrics ? JSON.parse(r.metrics) : null; } catch { return null; } })() }; });
    const agg = new Map();
    for (const r of runs) for (const t of r.top) { const a = agg.get(t.item) || { item: t.item, runs: 0, sum: 0, max: 0, last: null }; a.runs++; a.sum += t.secs; a.max = Math.max(a.max, t.secs); if (a.last == null) a.last = t.secs; agg.set(t.item, a); }   // runs 由新到舊，第一個遇到的是最近一次
    const slowest = [...agg.values()].map(a => ({ item: a.item, runs: a.runs, avg_s: Math.round(a.sum / a.runs * 10) / 10, max_s: a.max, last_s: a.last })).sort((a, b) => b.avg_s - a.avg_s).slice(0, 20);
    // 畫面量測排行：各項目在這 n 輪裡的 FPS（低的在前）與繪製數（高的在前）
    const fa = new Map(), da = new Map();
    const add = (m, list, r) => { for (const t of list || []) { const a = m.get(t.item) || { item: t.item, runs: 0, sum: 0, min: Infinity, max: -Infinity, last: null }; a.runs++; a.sum += t.value; a.min = Math.min(a.min, t.value); a.max = Math.max(a.max, t.value); if (a.last == null) a.last = t.value; m.set(t.item, a); } };
    for (const r of runs) if (r.metrics) { add(fa, r.metrics.lowest_fps); add(da, r.metrics.most_draw_calls); }
    const r1 = x => Math.round(x * 10) / 10;
    const lowest_fps = [...fa.values()].map(a => ({ item: a.item, runs: a.runs, avg: r1(a.sum / a.runs), min: a.min, last: a.last })).sort((a, b) => a.avg - b.avg).slice(0, 15);
    const most_draw_calls = [...da.values()].map(a => ({ item: a.item, runs: a.runs, avg: r1(a.sum / a.runs), max: a.max, last: a.last })).sort((a, b) => b.avg - a.avg).slice(0, 15);
    return json({ runs, slowest, lowest_fps, most_draw_calls });
  }
  if (path === '/api/triage' && req.method === 'POST') {   // 總指揮讀完 diff 回報嫌疑分支（或看不出來）
    const tri = await getTriage(db);
    if (!tri) return json({ error: '現在沒有待判斷的紅燈' }, 404);
    if (body.main_sha && !String(tri.main_sha).startsWith(String(body.main_sha)) && !String(body.main_sha).startsWith(String(tri.main_sha))) return json({ error: `main_sha 對不上現在待判斷的紅燈（${String(tri.main_sha).slice(0, 12)}）` }, 409);
    if (!['suspects', 'unknown'].includes(body.verdict)) return json({ error: 'verdict 要是 suspects（有嫌疑分支）或 unknown（看不出來）' }, 400);
    const sus = (Array.isArray(body.suspects) ? body.suspects : []).slice(0, 20).map(s => ({ branch: String(s && s.branch || '').slice(0, 120), commit: s && s.commit ? String(s.commit).slice(0, 40) : null, reason: s && s.reason ? String(s.reason).slice(0, 300) : null })).filter(s => s.branch);
    if (body.verdict === 'suspects' && !sus.length) return json({ error: 'suspects 至少要有一個帶 branch 的嫌疑' }, 400);
    const next = { ...tri, status: 'answered', verdict: body.verdict, suspects: body.verdict === 'suspects' ? sus : [], answered_by: actor, answered_at: now(), note: body.note ? String(body.note).slice(0, 500) : null };
    await db.prepare("UPDATE meta SET value=?,updated_at=? WHERE key='triage'").bind(JSON.stringify(next), now()).run();
    return json({ ok: true, triage: next });
  }
  if (req.method === 'POST' && path === '/api/cards') {
    if (!body.id || !body.title) return json({ error: 'id 與 title 必填' }, 400);
    { const bad = await badParent(db, String(body.id), body.parent); if (bad) return json({ error: bad }, 409); }
    const t = now();
    try {
      await db.prepare('INSERT INTO cards(id,title,body,area,priority,parent,owner_thread,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)')
        .bind(String(body.id), String(body.title), String(body.body || ''), String(body.area || ''), Number(body.priority ?? 100), body.parent || null, body.owner_thread ?? null, t, t).run();
    } catch (e) {
      // 只有真的撞到同 id 才說已存在；其他錯誤（資料庫暫時失敗、欄位問題）原樣回報，不要誤導
      const dup = await db.prepare('SELECT 1 FROM cards WHERE id=?').bind(String(body.id)).first().catch(() => null);
      if (dup) return json({ error: 'id 已存在' }, 409);
      return json({ error: '建立失敗：' + String(e && e.message || e).slice(0, 200) }, 500);
    }
    await log(db, body.id, actor, '建立');
    return json({ ok: true }, 201);
  }
  // 通用匯入格式：{cards:[{id,title,body?,area?,priority?,status?,owner?,branch?}]}；已存在的 id 略過
  if (req.method === 'POST' && path === '/api/import') {
    let added = 0, skipped = 0;
    for (const c of body.cards || []) {
      if (!c.id || !c.title || (c.status && !STATUSES.includes(c.status))) { skipped++; continue; }
      const t = now();
      const r = await db.prepare('INSERT OR IGNORE INTO cards(id,title,body,area,priority,status,owner,branch,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) RETURNING id')
        .bind(String(c.id), String(c.title), String(c.body || ''), String(c.area || ''), Number(c.priority ?? 100), c.status || 'open', c.owner ?? null, c.branch ?? null, t, t).all();
      r.results.length ? added++ : skipped++;   // 分支名有唯一索引：同名分支會被略過
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
          ? await db.prepare("SELECT id FROM cards WHERE status='open' AND dropping_at IS NULL AND area=? ORDER BY priority,created_at LIMIT 1").bind(body.area).first()
          : await db.prepare("SELECT id FROM cards WHERE status='open' AND dropping_at IS NULL ORDER BY priority,created_at LIMIT 1").first();
        if (!r) return json({ error: '沒有可認領的卡' }, 404);
        id = r.id;
      }
      const branch = branchFor(env, id);
      const res = await db.prepare("UPDATE cards SET status='claimed',owner=?,branch=COALESCE(branch,?),owner_thread=?,updated_at=? WHERE id=? AND status='open' AND dropping_at IS NULL RETURNING id")
        .bind(String(body.owner), branch, body.owner_thread ? String(body.owner_thread) : null, now(), id).all();
      if (res.results.length === 1) {
        await log(db, id, actor, `認領 ${body.owner}`);
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
    if (c.status === 'dropped' && body.status === undefined && 'discard_reason' in body && Object.keys(body).every(k => ['actor', 'discard_reason', 'note'].includes(k))) {   // 已放棄的卡只允許補寫放棄理由這一欄
      const why = String(body.discard_reason ?? '').trim();
      const bad = reasonProblem(why);
      if (bad) return json({ error: bad }, 400);
      await db.prepare("UPDATE cards SET discard_reason=?,updated_at=? WHERE id=? AND status='dropped'").bind(why, now(), id).run();
      await log(db, id, actor, '補寫放棄理由' + (body.note ? `：${body.note}` : ''));
      return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(id).first()));
    }
    if ((c.status === 'dropped' && body.status !== 'dropped') || c.phase === 'merged' || c.status === 'merged') return json({ error: `現在是「${stageOf(c)}」，已結束的卡不能再改`, stage: stageOf(c) }, 409);
    const resend = body.status === 'dropped' && Object.keys(body).every(k => ['status', 'actor', 'superseded_by', 'discard_reason', 'note'].includes(k));
    let deadSuccessor = false;
    if (stageOf(c) === 'waiting_parent') {   // 等待父卡：等接手卡進 main 自動結案；只允許重送放棄（回現況）
      if (body.status === 'dropped' && c.superseded_by) {   // 接手卡已經放棄（或不存在）：永遠等不到它進 main，允許改放棄（換接手卡或給原因），不回 409
        const sc = await db.prepare('SELECT status FROM cards WHERE id=?').bind(c.superseded_by).first();
        deadSuccessor = !sc || sc.status === 'dropped';
      }
      if (!deadSuccessor) {
        if (body.status === 'dropped' && resend) return json(view(c));
        return json({ error: '等待父卡中（接手卡還沒進 main），接手卡進 main 後自動結案', stage: 'waiting_parent' }, 409);
      }
    }
    if (stageOf(c) === 'dropping' && !resend) return json({ error: '放棄待查中，等排隊器查完才能動這張卡', stage: 'dropping' }, 409);   // 待查中一律不能改（重送放棄請求除外，見下）
    const sets = [], vals = [];
    if ('branch' in body) return json({ error: '分支名由看板給定，領卡後不會變' }, 400);
    for (const k of ['title', 'body', 'area', 'owner_thread']) if (k in body) { sets.push(`${k}=?`); vals.push(String(body[k])); }
    if ('parent' in body) {   // 可改成別張卡或清空（null／空字串）；放行保留
      const bad = await badParent(db, id, body.parent);
      if (bad) return json({ error: bad }, 409);
      sets.push('parent=?'); vals.push(body.parent ? String(body.parent) : null);
    }
    if ('priority' in body) { sets.push('priority=?'); vals.push(Number(body.priority)); }
    let note = '修改';
    let after = null, dropKids = false, dropping = false;
    if ('status' in body) {
      const stage = stageOf(c);
      const no = () => json({ error: `現在是「${stage}」，不能改成 ${body.status}`, stage }, 409);
      if (body.status === 'open') {          // 還卡：分支名保留；清負責人、送審、放行與結果
        if (stage === 'new') return json(view(c));
        if (!['developing', 'reviewing', 'rejected'].includes(stage)) return no();
        sets.push('status=?', 'owner=NULL', 'owner_thread=NULL', 'phase=NULL', 'phase_at=NULL', 'last_result=NULL', 'stuck_notified_at=NULL', ...CLEAR_REVIEW.split(','));
        vals.push('open');
      } else if (body.status === 'dropped') { // 放棄：除已合併外任何階段都可；所有卡都要先查有沒有改動漏掉（放棄防呆），只有建卡人 10 分鐘內放棄還是 new 的卡例外
        if (stage === 'dropped') return json(view(c));
        if (stage === 'merged') return no();
        if (stage === 'dropping') {   // 待查中再放棄一次：換了接手卡或原因就覆蓋（重新開一輪，上一輪晚到的結果作廢）；內容一樣就當重送
          const sup0 = body.superseded_by ? String(body.superseded_by) : '', why0 = String(body.discard_reason ?? '').trim();
          if ((!sup0 && !why0) || (sup0 === (c.superseded_by || '') && why0 === (c.discard_reason || ''))) return json(view(c));
        }
        // 唯一例外（開錯卡用）：建卡的人在卡還是 new、建卡 10 分鐘內放棄，直接放棄；其餘所有卡都先進放棄待查
        const creator = (await db.prepare("SELECT actor FROM events WHERE card_id=? AND note='建立' ORDER BY n LIMIT 1").bind(id).first())?.actor;
        if (stage === 'new' && creator && creator === actor && now() - c.created_at <= 600) {
          sets.push('status=?'); vals.push('dropped');
          dropKids = true;
        } else {
          let sup = body.superseded_by ? String(body.superseded_by) : '', why = String(body.discard_reason ?? '').trim();
          if (deadSuccessor && !sup && !why) why = String(c.discard_reason || '接手卡已放棄，這張卡一併放棄').trim();   // 接手卡已放棄、呼叫端沒給新接手卡也沒給原因：沿用原因
          if (!sup && !why) return json({ error: '放棄要帶 superseded_by（由哪張未放棄的卡接手）或 discard_reason（真的不要了的原因）', stage }, 400);
          if (!sup) { const bad = reasonProblem(why); if (bad) return json({ error: bad, stage }, 400); }
          if (sup) {
            const sc = sup === id ? null : await db.prepare('SELECT status FROM cards WHERE id=?').bind(sup).first();
            if (!sc || sc.status === 'dropped') return json({ error: 'superseded_by 必須是另一張存在且未放棄的卡' }, 409);
          }
          sets.push('dropping_at=?', 'superseded_by=?', 'discard_reason=?', 'drop_unmerged=NULL', 'drop_checked_at=NULL'); vals.push(Math.max(now(), (c.dropping_at || 0) + 1), sup || null, why || null);   // 新的一輪：dropping_at 一定比上一輪大，上一輪晚到的結果對不上
          if (deadSuccessor) sets.push("status='claimed'");   // 離開「等待父卡」，回到放棄待查（查驗通過後依新接手卡或原因結案）
          dropping = true;
        }
      } else return json({ error: 'status 只能改成 open（還卡）或 dropped（放棄）；認領請用 /api/claim' }, 400);
      after = (dropping ? `${stage} → 放棄待查` : `${stage} → ${body.status}`) + (body.note ? `：${body.note}` : '');
    }
    if (!sets.length) return json({ error: '沒有要改的欄位' }, 400);
    sets.push('updated_at=?'); vals.push(now());
    // 狀態轉移也帶舊狀態條件，避免兩個人同時改
    await db.prepare(`UPDATE cards SET ${sets.join(',')} WHERE id=? AND status=?`).bind(...vals, id, c.status).run();
    await log(db, id, actor, after || note);
    if (dropKids) await notifyKids(env, id);
    return json(view(await db.prepare('SELECT * FROM cards WHERE id=?').bind(id).first()));
  }
  return json({ error: 'not found' }, 404);
}

export default {
  fetch: (req, env, ctx) => handle(req, env, ctx),
  scheduled: (ev, env, ctx) => {
    const m = new Date((ev && ev.scheduledTime) || Date.now()).getUTCMinutes();
    ctx.waitUntil((m % 10 === 0 ? housekeeping(env) : Promise.resolve())
      .then(() => (m % 5 === 0 ? watchdog(env).catch(() => null) : null))   // 閒置看門狗每 5 分鐘（JET-17）
      .then(() => autoFlush(env)));
  },
};
