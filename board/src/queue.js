// 看板不連排隊器：排隊器自己來拉清單與放行（/api/queue/desired），結果事件存在卡片上；攢批叫醒負責串（routine fire）。
// 機密：HOOK_TOKEN（排隊器送事件、拉清單用）、ROUTINE_KEY（叫醒用）。
// 設定：ROUTINE_ID、WAKE_BATCH_S（預設 120）、WAKE_MAX_PER_HOUR（預設 10）。
import { bearerIs } from './auth.js';
import { stageOf, sameCommit } from './stage.js';
const list = v => String(v || '').split(',').map(x => x.trim()).filter(Boolean);
const json = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { 'content-type': 'application/json; charset=utf-8' } });
const now = () => Math.floor(Date.now() / 1000);

// 事件種類 → 上游分支狀態快取（phase；用上游自己的狀態名）。事件只當快取與叫醒，不當真相；沒列的只記錄。
const PHASE_OF = { pushed: 'pending', tested_green: 'selftest_green', tested_red: 'rejected', queued: 'queued', merged: 'merged', rejected: 'rejected', conflict: 'rejected', halted: 'halted', selftest_started: 'selftesting', batch_started: 'batching' };
// 需要人處理、要叫醒負責串的種類
const ATTENTION = new Set(['tested_red', 'rejected', 'conflict', 'superseded']);   // → 領卡的開發者
const ADMIN_KINDS = new Set(['halted', 'parent_blocked', 'queue_error']);        // → 管理者
const KIND_TEXT = { drop_rejected: '放棄沒成立：這張卡還有改動不在 main 也不在接手的卡裡', drop_discarded: '卡被放棄，且仍有改動不在 main（事後請看一眼）',  parent_blocked: '父卡被放棄或不存在，子卡被擋住',  waiting_done: '接手卡已進 main，等待中的卡已結案',  loop_culprit: '背景循環在 main 上紅了，已 bisect 出兇手並自動開急修卡',  loop_triage: 'main 紅燈待判斷：請總指揮讀 diff 指出嫌疑分支',  superseded: '送審後內容又改了，要重新送審', stuck: '卡住沒動靜', review_requested: '送審待審', review_rejected: '審查退回，請修改後重新送審', tested_red: '自測紅', rejected: '被退回', conflict: '跟 main 撞檔', halted: '排隊器暫停待人處理', queue_error: '排隊器出狀況' };

// 一批事件在記憶體裡依序處理（卡片狀態用預載的副本，邊處理邊更新），所有寫入收成語句，最後用 D1 batch 一次送出
// （每個事件的語句整組同進同出，不跨 batch 切開）：一批 50 筆只有幾次往返，一組失敗不會留下做一半的事件。
// D1 一句 SQL 最多 100 個參數：id 清單分段更新，回傳總共改了幾列
async function updateByIds(db, make, pre, ids) {
  let changes = 0;
  for (const c of chunk(ids, 80)) changes += (await db.prepare(make(c.map(() => '?').join(','))).bind(...pre, ...c).run()).meta.changes;
  return changes;
}
const chunk = (arr, n) => { const o = []; for (let i = 0; i < arr.length; i += n) o.push(arr.slice(i, i + n)); return o; };

export async function handleHook(req, env, ctx) {
  if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
  if (!bearerIs(req, env.HOOK_TOKEN)) return json({ error: 'unauthorized' }, 401);
  const body = await req.json().catch(() => null);
  if (!body || !Array.isArray(body.events)) return json({ error: 'body 要有 events 陣列' }, 400);
  const db = env.DB, source = String(body.source || 'queue');
  const admins = list(env.ADMIN_THREADS);
  const events = body.events.slice(0, 200), remaining = Math.max(0, body.events.length - events.length);
  const T = now();
  let applied = 0, unmatched = 0, dup = 0, stale = 0, logged = false;
  // ---- 預載：已見過的事件編號、事件牽涉的卡 ----
  const keyOf = e => `${source}:${String(e.id)}`, hasId = e => e.id != null && e.id !== '';
  const seen = new Set();
  for (const ks of chunk(events.filter(hasId).map(keyOf), 90)) {
    const r = await db.prepare(`SELECT key FROM hook_seen WHERE key IN (${ks.map(() => '?').join(',')})`).bind(...ks).all();
    for (const row of r.results) seen.add(row.key);
  }
  const byBranch = new Map(), byId = new Map();
  const load = rows => { for (const c of rows) { if (!byId.has(c.id)) byId.set(c.id, c); if (c.branch && !byBranch.has(c.branch)) byBranch.set(c.branch, byId.get(c.id)); } };
  for (const bs of chunk([...new Set(events.map(e => e.branch).filter(Boolean).map(String))], 90))
    load((await db.prepare(`SELECT * FROM cards WHERE branch IN (${bs.map(() => '?').join(',')})`).bind(...bs).all()).results);
  for (const bs of chunk([...new Set(events.filter(e => e.kind === 'loop_culprit' && e.culprit_branch).map(e => String(e.culprit_branch)))], 90))
    load((await db.prepare(`SELECT * FROM cards WHERE branch IN (${bs.map(() => '?').join(',')})`).bind(...bs).all()).results);
  for (const ids of chunk([...new Set(events.filter(e => e.kind === 'loop_culprit' && e.culprit_card).map(e => String(e.culprit_card)))], 90))
    load((await db.prepare(`SELECT * FROM cards WHERE id IN (${ids.map(() => '?').join(',')})`).bind(...ids).all()).results);
  for (const ids of chunk([...new Set(events.filter(e => e.kind === 'drop_checked').flatMap(e => [e.card].filter(Boolean)).map(String))], 90))
    load((await db.prepare(`SELECT * FROM cards WHERE id IN (${ids.map(() => '?').join(',')})`).bind(...ids).all()).results);
  // 放棄待查、branch 欄是空的卡：排隊器用 <前綴><卡號小寫> 當分支名查，事件的 branch 對得上這個推導名
  const droppingNoBranch = events.some(e => e.kind === 'drop_checked' && e.branch)
    ? (await db.prepare("SELECT * FROM cards WHERE dropping_at IS NOT NULL AND status!='dropped' AND branch IS NULL").all()).results : [];
  const derived = new Map(droppingNoBranch.map(c => [`${env.BRANCH_PREFIX ?? 'dev/'}${String(c.id).toLowerCase().replace(/[^a-z0-9._-]+/g, '-')}`, c]));
  for (const c of droppingNoBranch) if (!byId.has(c.id)) byId.set(c.id, c); else Object.assign(byId.get(c.id), c);
  // ---- 逐筆處理，寫入累積成語句 ----
  let cur = [];
  const q = (sql, ...args) => cur.push(db.prepare(sql).bind(...args));
  const patch = (card, cols) => { const ks = Object.keys(cols); q(`UPDATE cards SET ${ks.map(k => `${k}=?`).join(',')} WHERE id=?`, ...ks.map(k => cols[k]), card.id); Object.assign(card, cols); };
  // guard：[條件 SQL, ...參數]；有帶時只在條件成立才寫（同一個 batch 內前面的 UPDATE 已生效）
  const post = (owners, card, kind, branch, detail, guard) => owners.forEach(o => guard
    ? q(`INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) SELECT ?,?,?,?,?,? WHERE EXISTS(${guard[0]})`, o, card?.id ?? null, branch, kind, detail, T, ...guard.slice(1))
    : q('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)', o, card?.id ?? null, branch, kind, detail, T));
  const logEv = (id, note, guard) => guard
    ? q(`INSERT INTO events(card_id,at,actor,note) SELECT ?,?,?,? WHERE EXISTS(${guard[0]})`, id, T, source, note, ...guard.slice(1))
    : q('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)', id, T, source, note);
  const hl = (e, outcome) => { logged = true; q('INSERT INTO hook_log(at,body,outcome) VALUES(?,?,?)', T, JSON.stringify(e).slice(0, 600), outcome); };
  const groups = [], hotNew = new Set();
  for (const e of events) {
    cur = []; const a0 = applied;
    await (async () => {
      const kind = String(e.kind || ''), branch = e.branch ? String(e.branch) : null;
      if (hasId(e)) {
        if (seen.has(keyOf(e))) { dup++; return; }
        seen.add(keyOf(e)); q('INSERT INTO hook_seen(key) VALUES(?)', keyOf(e));   // 不帶 OR IGNORE：兩個併發相同請求撞 UNIQUE，後者整組回滾，去重保持原子
      }
      if (kind === 'workflow_status') {   // 排隊器回報：{mode, loop:{state,last_run_at,last_result,main_sha,failed_items,culprit,next_run_at,...}}；整份取代，不需要對應到卡
        if (!['gate_batch', 'direct_merge'].includes(e.mode)) { stale++; hl(e, 'workflow_status 的 mode 要是 gate_batch 或 direct_merge'); return; }
        const val = JSON.stringify({ mode: e.mode, loop: e.loop && typeof e.loop === 'object' ? e.loop : null }).slice(0, 4000);
        q("INSERT INTO meta(key,value,updated_at) VALUES('workflow',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", val, T);
        if (e.loop && (e.loop.state === 'green' || e.loop.last_result === 'green')) q("DELETE FROM meta WHERE key='main_red'");
        applied++; return;
      }
      if (kind === 'loop_perf') {   // 背景測試一輪的效能量測：{main_sha, perf:{kind, wall_s, cores, cpu_util, items, top:[{item,secs}]}}；留最近 200 輪，給優化參考
        const p = e.perf && typeof e.perf === 'object' ? e.perf : null;
        const num = v => (v == null || v === '' || !Number.isFinite(Number(v))) ? null : Number(v);
        if (!p || num(p.wall_s) == null) { stale++; hl(e, 'loop_perf 要帶 perf.wall_s（數字）'); return; }
        const top = (Array.isArray(p.top) ? p.top : []).slice(0, 30).map(x => ({ item: String((x && (x.item ?? x.item_id ?? x.name)) ?? x).slice(0, 120), secs: num(x && x.secs) ?? 0 }));
        q('INSERT INTO loop_perf(at,main_sha,kind,wall_s,cores,cpu_util,items,top) VALUES(?,?,?,?,?,?,?,?)', T, e.main_sha ? String(e.main_sha).slice(0, 40) : null, p.kind ? String(p.kind).slice(0, 40) : null, num(p.wall_s), num(p.cores), num(p.cpu_util), num(p.items), JSON.stringify(top));
        q('DELETE FROM loop_perf WHERE n NOT IN (SELECT n FROM loop_perf ORDER BY n DESC LIMIT 200)');
        applied++; return;
      }
      if (kind === 'loop_red' || kind === 'loop_green') {   // 背景循環在 main 上紅／恢復綠：只記錄 main 紅狀態（顯示用），不開卡；開卡在 loop_culprit
        if (kind === 'loop_green') { q("DELETE FROM meta WHERE key='main_red'"); q("DELETE FROM meta WHERE key='triage'"); }
        else q("INSERT INTO meta(key,value,updated_at) VALUES('main_red',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", JSON.stringify({ since: e.since ?? T, main_sha: e.main_sha ? String(e.main_sha).slice(0, 40) : null, failed_items: Array.isArray(e.failed_items) ? e.failed_items.slice(0, 20) : null, detail: e.detail ? String(e.detail).slice(0, 300) : null }).slice(0, 3000), T);
        applied++; return;
      }
      if (kind === 'loop_triage') {   // 背景循環紅了：先請人讀 diff 判斷嫌疑分支（比二分快）；存起來、通知管理者，回報走 POST /api/triage
        const cut = (v, n) => String(v ?? '').slice(0, n);
        const range = (Array.isArray(e.commits) ? e.commits : Array.isArray(e.range) ? e.range : []).slice(0, 100).map(r => ({ commit: cut(r && (r.sha ?? r.commit), 40), branch: cut(r && r.branch, 120), subject: cut(r && r.subject, 160) }));
        const failed = (Array.isArray(e.failed_items) ? e.failed_items : []).slice(0, 20).map(f => (f && typeof f === 'object') ? { item: cut(f.item ?? f.item_id ?? f.name, 120), summary: cut(f.summary, 800) } : { item: cut(f, 120) });   // 筆數與長度先限好，整份 JSON 不超過約 5 萬字（不切 JSON 字串，切斷讀回來會是壞值）
        const tri = { main_sha: cut(e.main_sha, 40), last_green_sha: cut(e.green_sha ?? e.last_green_sha, 40), deadline_at: e.deadline_at ?? null, range, failed_items: failed, detail: cut(e.detail, 500), status: 'pending', at: T };
        q("INSERT INTO meta(key,value,updated_at) VALUES('triage',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at", JSON.stringify(tri), T);
        const brs = [...new Set(range.map(r => r.branch).filter(Boolean))];
        post(admins.length ? admins : [''], null, 'loop_triage', null, `main 紅燈 ${tri.main_sha.slice(0, 8)}（上次綠 ${tri.last_green_sha.slice(0, 8)}）：區間 ${range.length} 筆提交，分支 ${brs.slice(0, 8).join('、')}${brs.length > 8 ? '…' : ''}；紅項 ${failed.map(f => f.item).slice(0, 5).join('、')}。請讀 diff 判斷嫌疑分支，再 POST /api/triage 回報（或回「看不出來」）。`.slice(0, 500));
        applied++; return;
      }
      if (kind === 'loop_culprit') {   // 背景循環 bisect 出兇手（事件帶兇手分支名）：自動開一張急修卡（priority 0、area hotfix），通知兇手卡的負責串與管理者；同一個 id 只開一次
        const hid = String(e.hotfix_id || `HOTFIX-${String(e.main_sha || e.culprit_commit || e.commit || e.id || T).slice(0, 8)}`).replace(/[^A-Za-z0-9._-]+/g, '-').slice(0, 60);
        const cul = (e.culprit_card && byId.get(String(e.culprit_card))) || (e.culprit_branch && byBranch.get(String(e.culprit_branch))) || (branch && byBranch.get(branch)) || null;
        q("UPDATE meta SET value=json_set(value,'$.status','resolved'),updated_at=? WHERE key='triage'", T);   // 兇手已確認：紅燈判斷結案（即使急修卡已存在也要結，desired 才不會一直帶 loop_suspect）
        if (hotNew.has(hid) || await db.prepare('SELECT 1 FROM cards WHERE id=?').bind(hid).first()) { stale++; hl(e, `急修卡 ${hid} 已存在`); return; }
        const items = Array.isArray(e.failed_items) ? e.failed_items.slice(0, 20).map(x => String((x && x.item) || (x && x.name) || x)).join('、') : '';
        const bodyText = `背景循環在 main ${String(e.main_sha || '').slice(0, 12)} 測出紅項${items ? '：' + items : ''}。兇手提交 ${String(e.culprit_commit || '不明').slice(0, 12)}${cul ? `（來自卡 ${cul.id}）` : ''}。${e.detail ? String(e.detail).slice(0, 500) : ''}`.slice(0, 1500);
        hotNew.add(hid);   // 同一批裡重複的急修卡 id：資料庫還查不到前一筆，用這個擋
        applied++;
        q("INSERT INTO cards(id,title,body,area,priority,parent,owner_thread,created_at,updated_at) VALUES(?,?,?,'hotfix',0,NULL,?,?,?)", hid, `急修：main 紅燈${cul ? '（' + cul.id + '）' : ''}`, bodyText, cul?.owner_thread ?? null, T, T);
        q('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)', hid, T, source, '建立（背景循環紅燈自動開卡）');
        post([...new Set([cul?.owner_thread || '', ...(admins.length ? admins : [])])], { id: hid }, 'loop_culprit', null, `急修卡 ${hid}：${bodyText}`.slice(0, 500));
        return;
      }
      let card = branch ? byBranch.get(branch) : null;
      if (!card && kind === 'drop_checked' && e.card) card = byId.get(String(e.card));
      if (!card && kind === 'drop_checked' && branch) { const d = derived.get(branch); if (d) card = byId.get(d.id); }
      const detail = e.detail ? String(e.detail).slice(0, 500) : '';
      if (!card) {
        unmatched++;
        if (kind === 'drop_checked') hl(e, '找不到對應的卡（branch／card 都對不上）');
        if (ADMIN_KINDS.has(kind)) post(admins.length ? admins : [''], null, kind, branch, detail);   // 排隊器自己的問題：通知管理者
        return;
      }
      if (kind === 'drop_checked') {   // 放棄待查的結果：只對「放棄待查」的卡有效，其他一律當過期丟掉
        if (!card.dropping_at || card.status === 'dropped') { stale++; hl(e, `過期：卡不在待查（${card.status}）`); return; }
        // 查驗結果缺必要欄位（unmerged／covered 不是布林）：當作排隊器沒查成，不能當成「沒有漏」放行
        if (typeof e.unmerged !== 'boolean' || typeof e.covered !== 'boolean') { stale++; hl(e, '格式不全：unmerged／covered 要是布林'); return; }
        // 比對是哪一次放棄的結果：事件帶回 dropping_at 就必須相同；沒帶但有 target 就必須是現在接手卡的分支
        if (e.dropping_at != null && Number(e.dropping_at) !== card.dropping_at) { stale++; hl(e, `過期：dropping_at 對不上（卡上 ${card.dropping_at}）`); return; }
        if (e.dropping_at == null && card.superseded_by && e.target != null) {
          let sc = byId.get(card.superseded_by);
          if (!sc) { sc = await db.prepare('SELECT id,branch FROM cards WHERE id=?').bind(card.superseded_by).first(); if (sc) byId.set(sc.id, sc); }
          // 接手卡的分支還不存在時排隊器回的 target 是 main（退回只看 main），也算對得上
          if (!sc || (String(e.target) !== 'main' && String(e.target) !== (sc.branch || `dev/${String(sc.id).toLowerCase()}`))) { stale++; hl(e, `過期：target 對不上（接手卡 ${card.superseded_by}）`); return; }
        }
        const unmerged = e.unmerged === true, covered = e.covered === true, mc = Number(e.missing_count ?? 0) || 0;
        const missing = Array.isArray(e.missing_files) ? e.missing_files.slice(0, 20).map(String).join('、') : '';
        applied++;
        hl(e, '收下');
        // 帶 superseded_by：沒有不在 main 的改動，或接手的卡全都涵蓋了才放棄；只帶 discard_reason：一律放棄（有漏的另發通知）
        const ok = !card.superseded_by ? true : (!unmerged || covered);
        if (ok) {
          // 預載到送出之間若有人取消放棄或改掉，條件不成立就整組不寫（UPDATE 帶條件；events／outbox 用同條件的 EXISTS）
          // 接手卡還沒進 main：不算放棄，進「等待父卡」，接手卡合併後才轉放棄
          let sup = card.superseded_by ? byId.get(card.superseded_by) : null;
          if (card.superseded_by && !sup) { sup = await db.prepare('SELECT * FROM cards WHERE id=?').bind(card.superseded_by).first(); if (sup) byId.set(sup.id, sup); }
          const waiting = !!(sup && sup.phase !== 'merged' && sup.status !== 'dropped');
          const fin = waiting ? 'waiting' : 'dropped';
          q("UPDATE cards SET status=?,dropping_at=?,drop_unmerged=?,drop_checked_at=?,updated_at=? WHERE id=? AND dropping_at=? AND status!='dropped'", fin, waiting ? null : card.dropping_at, unmerged ? 1 : 0, T, T, card.id, card.dropping_at);
          Object.assign(card, { status: fin, dropping_at: waiting ? null : card.dropping_at, drop_unmerged: unmerged ? 1 : 0, drop_checked_at: T, updated_at: T });
          logEv(card.id, waiting ? `放棄查驗通過，但接手卡 ${card.superseded_by} 還沒進 main：進等待父卡，接手卡合併後自動結案` : `放棄查驗通過（${unmerged ? `有改動不在 main，缺 ${mc} 個檔` : '沒有漏掉的改動'}）`, [`SELECT 1 FROM cards WHERE id=? AND status=? AND drop_checked_at=?`, card.id, fin, T]);
          const dg = [`SELECT 1 FROM cards WHERE id=? AND status=? AND drop_checked_at=?`, card.id, fin, T];
          if (fin === 'dropped') for (const k of (await db.prepare("SELECT * FROM cards WHERE superseded_by=? AND status='waiting'").bind(card.id).all()).results)   // 等的接手卡也被放棄了：要人決定
            post(admins.length ? admins : [k.owner_thread || ''], k, 'parent_blocked', k.branch, `接手卡 ${card.id} 被放棄，等待中的 ${k.id} 要改接別張卡`, dg);
          const kids = (await db.prepare("SELECT * FROM cards WHERE parent=? AND status='claimed'").bind(card.id).all()).results;   // 子卡被擋住，通知管理者
          for (const k of kids) post(admins.length ? admins : [k.owner_thread || ''], k, 'parent_blocked', k.branch, `父卡 ${card.id} 被放棄，請改接別張卡或直接接 main`, dg);
          if (unmerged && !card.superseded_by) post(admins.length ? admins : [''], card, 'drop_discarded', card.branch, `缺 ${mc} 個檔${missing ? `：${missing}` : ''}；放棄原因：${card.discard_reason}`, dg);
        } else {   // 放棄不成立：回到原階段，通知負責串與管理者
          const why = `這張卡還有改動不在 main，也不在 ${card.superseded_by} 裡${mc ? `（缺 ${mc} 個檔）` : ''}${missing ? `，缺的檔：${missing}` : ''}`;
          const owner = card.owner_thread || '';
          q('UPDATE cards SET dropping_at=NULL,superseded_by=NULL,discard_reason=NULL,drop_unmerged=1,drop_checked_at=?,updated_at=? WHERE id=? AND dropping_at=?', T, T, card.id, card.dropping_at);
          Object.assign(card, { dropping_at: null, superseded_by: null, discard_reason: null, drop_unmerged: 1, drop_checked_at: T, updated_at: T });
          const rg = ['SELECT 1 FROM cards WHERE id=? AND dropping_at IS NULL AND drop_checked_at=?', card.id, T];
          logEv(card.id, `放棄沒成立：${why}`, rg);
          post([...new Set([owner, ...(admins.length ? admins : [])])], card, 'drop_rejected', card.branch, why, rg);
        }
        return;
      }
      // 過期回報不算數：卡片沒在跑（沒放行、已放棄），或事件依據的放行版本／重測計數跟卡片現在的不符，一律丟掉（仍回 200）
      const claimedAndApproved = card.status === 'claimed' && card.approved_commit;
      const sameApproval = e.approved_sha == null || sameCommit(String(e.approved_sha), card.approved_commit);
      const sameRetest = e.retest == null || Number(e.retest) === card.retest;
      if (kind !== 'merged' && (!claimedAndApproved || !sameApproval || !sameRetest)) { stale++; return; }   // 已合併事實一律接受（main 已經有了）
      // superseded 說「放行版本的內容變了」，但事件帶的現版跟放行版本是同一個提交：不可能，丟掉（不能因為排隊器誤報就洗掉送審與放行）
      if (kind === 'superseded' && e.commit && card.approved_commit && sameCommit(String(e.commit), card.approved_commit)) { stale++; hl(e, 'superseded 的現版跟放行版本是同一個提交，不採信'); return; }
      applied++;
      if (kind === 'superseded') {   // 內容變了：送審、放行、舊結果作廢，回到開發中
        patch(card, { submitted_commit: null, submitted_at: null, rejected_reason: null, rejected_by: null, rejected_at: null, approved_commit: null, approved_by: null, approved_at: null, phase: null, phase_at: null, last_result: null, updated_at: T });
      }
      // 優先用事件自帶的上游狀態名（state，或排隊器狀態事件的 to），否則依 kind 對照
      const to = kind === 'superseded' ? null : e.state ? String(e.state) : (typeof e.to === 'string' && /^[a-z_]{3,20}$/.test(e.to) ? e.to : PHASE_OF[kind]);
      if (to) patch(card, { phase: to, phase_at: T, updated_at: T });
      if (!['approved', 'revoked', 'superseded', 'parent_blocked'].includes(kind)) {   // 放行是看板自己的欄位；父卡擋住只是註記，都不覆蓋「最新結果」
        let fi = Array.isArray(e.failed_items) && e.failed_items.length ? e.failed_items.slice(0, 50).map(f => (f && typeof f === 'object' && typeof f.summary === 'string' && f.summary.length > 1200) ? { ...f, summary: f.summary.slice(0, 1200) } : f) : null;
        if (!fi) { const prev = resultOf(card); if (prev && prev.failed_items && prev.failed_items.length && e.commit && prev.commit === e.commit) fi = prev.failed_items; }   // 同一版本後來的事件（如 rejected）沒帶紅項就沿用先前的
        const res = { kind, at: e.at ?? T, commit: e.commit ?? null, detail: detail || null, from: e.from ?? null, to: e.to ?? null, failed_items: fi };
        patch(card, { last_result: JSON.stringify(res) });
      }
      logEv(card.id, `${kind}${to ? ` → ${to}` : ''}${detail ? `：${detail}` : ''}`);
      if (kind === 'merged') {   // 這張卡是別人的接手卡：等它進 main 的「等待父卡」卡自動結案（轉放棄），通知負責串；接龍（A 等 B、B 等 C）一路結案到底
        const seen = new Set([card.id]), todo = [card.id];
        while (todo.length) {
          const via = todo.shift();
          for (const k of (await db.prepare("SELECT * FROM cards WHERE superseded_by=? AND status='waiting'").bind(via).all()).results) {
            if (seen.has(k.id)) continue;
            seen.add(k.id); todo.push(k.id);
            const g = [`SELECT 1 FROM cards WHERE id=? AND status='dropped' AND updated_at=?`, k.id, T];
            q("UPDATE cards SET status='dropped',updated_at=? WHERE id=? AND status='waiting'", T, k.id);
            logEv(k.id, `接手卡 ${via} 已結案（內容進了 main，起點 ${card.id}），等待結束：轉放棄（結案）`, g);
            post([...new Set([k.owner_thread || '', ...(admins.length ? admins : [])])], k, 'waiting_done', k.branch, `接手卡 ${via} 已結案（內容進了 main，起點 ${card.id}），這張卡已結案`, g);
          }
        }
      }
      // 通知：被排隊器退回、內容變了 → 領卡的開發者；暫停、父卡擋住 → 管理者
      if (ADMIN_KINDS.has(kind)) post(admins.length ? admins : [card.owner_thread || ''], card, kind, branch, detail);
      else if (ATTENTION.has(kind)) post([card.owner_thread || ''], card, kind, branch, detail);
    })();
    if (cur.length) groups.push({ s: cur, applied: applied > a0 });
  }
  if (logged) groups.push({ s: [db.prepare('DELETE FROM hook_log WHERE n NOT IN (SELECT n FROM hook_log ORDER BY n DESC LIMIT 100)')], applied: false });
  // ---- 一次送出：每個 batch 最多 80 句，事件的語句整組不拆開 ----
  // 整個 batch 失敗（例如併發的相同請求撞 hook_seen 的 UNIQUE）：改成每組單獨送；單組撞 UNIQUE 就記為重複跳過，其他錯誤照丟
  let batch = [];
  const sendOne = async g => { try { await db.batch(g.s); } catch (err) { if (!/UNIQUE|constraint/i.test(String(err && err.message || err))) throw err; dup++; if (g.applied) applied--; } };
  const flushBatch = async () => {
    if (!batch.length) return;
    const b = batch; batch = [];
    try { await db.batch(b.flatMap(g => g.s)); } catch { for (const g of b) await sendOne(g); }
  };
  let n = 0;
  for (const g of groups) { if (batch.length && n + g.s.length > 80) { await flushBatch(); n = 0; } batch.push(g); n += g.s.length; }
  await flushBatch();
  if (ctx && ctx.waitUntil) ctx.waitUntil(autoFlush(env));
  return json({ ok: true, applied, unmatched, duplicate: dup, stale, processed: events.length, remaining });
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
    await updateByIds(db, ph => `UPDATE outbox SET sent_at=? WHERE n IN (${ph})`, [t], ids);
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
  // 放棄待查太久沒結果（排隊器沒回、或回的格式不全）：通知管理者一次（新的一輪才會再通知）
  const dm = Number(env.DROP_STUCK_MIN ?? 30);
  const dr = (await env.DB.prepare("SELECT * FROM cards WHERE dropping_at IS NOT NULL AND status!='dropped' AND dropping_at<? AND (stuck_notified_at IS NULL OR stuck_notified_at<dropping_at)").bind(t - dm * 60).all()).results;
  for (const c of dr) {
    const detail = `放棄待查超過 ${dm} 分鐘還沒有查驗結果（排隊器沒回、或回的格式不全，請看 hook_log）`;
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
  loop_triage: 'main 紅了：讀通知裡區間的提交與紅項原文，判斷最可能的分支（看 git diff，不要先跑測試），用 POST /api/triage {actor, main_sha, verdict:"suspects"|"unknown", suspects:[{branch, commit?, reason?}]} 回報；看不出來就 verdict unknown，排隊器會自己二分。',
  loop_culprit: 'main 在背景循環測出紅燈，兇手已 bisect 出來：請負責串優先領急修卡（priority 0）修好，不要等人催。',
  review_requested: '請看 git diff 審查（detail 若帶「上次退回原因」，先確認那一點真的改掉）；通過就在看板對這張卡 approve（帶審過的 commit），要改就 reject 並寫原因。',
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
  // 同一個收件串、同一張卡的通知只留最新一則（舊的已被新的取代，直接標成已送）：壓低發信量。沒有卡號的通知不合併。
  const latest = new Map(), stale = [];
  for (const r of rows) {
    if (!r.card_id) continue;
    const k = `${r.owner_thread || fallback}\u0000${r.card_id}`, prev = latest.get(k);
    if (!prev || r.n > prev.n) { if (prev) stale.push(prev.n); latest.set(k, r); } else stale.push(r.n);
  }
  if (stale.length) await updateByIds(db, ph => `UPDATE outbox SET sent_at=? WHERE sent_at IS NULL AND claim_id=? AND n IN (${ph})`, [t, token], stale);
  const dropped = new Set(stale);
  const groups = new Map();
  for (const r of rows) { if (dropped.has(r.n)) continue; const to = r.owner_thread || fallback; (groups.get(to) || groups.set(to, []).get(to)).push(r); }
  // 發信端有限額：每個收件串兩則之間至少隔 min_gap_s（同一串的通知攢到下一輪一起發）、一次最多 max_messages 個收件串；
  // 沒輪到的不是丟掉，只是把領取標記放掉，下一次 claim 再給（claimed_at 清空，不用等 5 分鐘）。
  const minGap = Math.max(0, Number(body.min_gap_s) || 0), maxMsg = Number.isInteger(body.max_messages) && body.max_messages > 0 ? body.max_messages : Infinity;
  const lastSent = new Map();
  if (minGap > 0) for (const r of (await db.prepare('SELECT owner_thread, max(sent_at) AS t FROM outbox WHERE sent_at IS NOT NULL GROUP BY owner_thread').all()).results) lastSent.set(r.owner_thread || fallback, r.t);
  const held = [];
  let given = 0;
  for (const [to, items] of [...groups].sort((a, b) => Math.min(...a[1].map(i => i.at)) - Math.min(...b[1].map(i => i.at)))) {
    if ((lastSent.get(to) != null && t - lastSent.get(to) < minGap) || given >= maxMsg) { groups.delete(to); held.push(...items.map(i => i.n)); } else given++;
  }
  if (held.length) await updateByIds(db, ph => `UPDATE outbox SET claimed_at=NULL,claim_id=NULL WHERE sent_at IS NULL AND claim_id=? AND n IN (${ph})`, [token], held);
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

// 發信失敗（例如被限額）：把領走的信放回去，hold_s 秒內不再給（預設 0＝下一次 claim 就再給）
export async function outboxRelease(req, env) {
  if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
  const body = await req.json().catch(() => ({}));
  const actor = String(body.actor ?? '').trim();
  if (!actor || actor.toLowerCase() === 'unknown') return json({ error: 'actor 必填' }, 400);
  const ids = (Array.isArray(body.ids) ? body.ids : []).map(Number).filter(Number.isInteger).slice(0, 500);
  if (!ids.length) return json({ error: 'ids 要是非空的數字陣列（claim 回傳的 ids）' }, 400);
  const hold = Math.min(CLAIM_TTL, Math.max(0, Number(body.hold_s) || 0));
  const claimedAt = hold > 0 ? now() - CLAIM_TTL + hold : null;
  const released = await updateByIds(env.DB, ph => `UPDATE outbox SET claimed_at=?,claim_id=NULL WHERE sent_at IS NULL AND n IN (${ph})`, [claimedAt], ids);
  return json({ released });
}

export async function outboxAck(req, env) {
  if (req.method !== 'POST') return json({ error: 'POST only' }, 405);
  const body = await req.json().catch(() => ({}));
  const actor = String(body.actor ?? '').trim();
  if (!actor || actor.toLowerCase() === 'unknown') return json({ error: 'actor 必填' }, 400);
  const ids = (Array.isArray(body.ids) ? body.ids : []).map(Number).filter(Number.isInteger).slice(0, 500);
  if (!ids.length) return json({ error: 'ids 要是非空的數字陣列（claim 回傳的 ids）' }, 400);
  const acked = await updateByIds(env.DB, ph => `UPDATE outbox SET sent_at=? WHERE sent_at IS NULL AND n IN (${ph})`, [now()], ids);
  return json({ acked });
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
  const kids = (await env.DB.prepare("SELECT * FROM cards WHERE (parent=? AND status='claimed') OR (superseded_by=? AND status='waiting')").bind(id, id).all()).results;
  for (const k of kids) for (const o of (admins.length ? admins : [k.owner_thread || ''])) await env.DB.prepare('INSERT INTO outbox(owner_thread,card_id,branch,kind,detail,at) VALUES(?,?,?,?,?,?)').bind(o, k.id, k.branch, 'parent_blocked', `父卡 ${id} 被放棄，請改接別張卡或直接接 main`, now()).run();
}
