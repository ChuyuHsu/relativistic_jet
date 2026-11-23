// 任務卡服務：Cloudflare Worker + D1。env.DB 是 D1，env.TOKEN 是寫入權杖。
import { bearerIs } from './auth.js';
import { handleHook, notifyKids, autoFlush, outboxClaim, outboxAck, outboxRelease, outboxPending, resultOf, stuckCheck } from './queue.js';
import { stageOf, sameCommit, STAGES } from './stage.js';
// 卡片自己只管前段與結局：open 空、claimed 已認領、dropped 放棄；放行是卡片上的欄位（approved_*）。
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
  const dropped = ['dropping', 'dropped'].includes(stage) ? { superseded_by: c.superseded_by ?? null, discard_reason: c.discard_reason ?? null, unmerged: c.drop_unmerged == null ? null : !!c.drop_unmerged, checked_at: c.drop_checked_at ?? null } : null;
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
async function withParent(db, cards) {
  const need = cards.filter(c => c.parent);
  if (!need.length) return cards;
  const rows = (await db.prepare('SELECT * FROM cards').all()).results;
  const by = new Map(rows.map(r => [r.id, r]));
  return cards.map(c => {
    if (!c.parent) return c;
    const p = by.get(c.parent), ps = p ? stageOf(p) : 'missing';
    return { ...c, parent_stage: ps, waiting_parent: ps !== 'merged', parent_blocked: ps === 'dropped' || ps === 'missing' };
  });
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

async function log(db, id, actor, note) {
  await db.prepare('INSERT INTO events(card_id,at,actor,note) VALUES(?,?,?,?)').bind(id, now(), actor, note).run();
}

// 給排隊器定時拉：所有「已認領、有分支、還沒合入」的卡 = 排隊器該管的分支（已放棄、釋放的不在內）。出錯一律回非 200。
async function desired(req, env) {
  if (req.method !== 'GET') return json({ error: 'GET only' }, 405);
  if (!bearerIs(req, env.HOOK_TOKEN, env.READ_TOKEN, env.TOKEN)) return json({ error: 'unauthorized' }, 401);
  try {
    // 一次把卡讀進記憶體再算（原本每張卡多查一兩次 D1，卡多了會超過 10 秒）
    const all = (await env.DB.prepare('SELECT * FROM cards ORDER BY priority,created_at').all()).results;
    const by = new Map(all.map(c => [c.id, c]));
    const out = [];
    for (const c of all) {
      if (!(c.status === 'claimed' && c.branch && c.phase !== 'merged' && !c.dropping_at)) continue;
      const e = { branch: c.branch };
      const p = parentInfo(env, c, by);
      if (p) Object.assign(e, p);
      if (c.approved_commit) e.approved_sha = c.approved_commit;
      e.retest = c.retest;
      e.priority = c.priority;   // 數字小的先（跟看板排序一致）；排隊器拿來排自測與批測順序
      out.push(e);
    }
    if (new URL(req.url).searchParams.get('with') === 'drop_checks') {   // 放棄待查：排隊器要查這些分支有沒有改動漏掉
      const drop_checks = all.filter(c => c.dropping_at && c.status !== 'dropped').sort((a, b) => a.dropping_at - b.dropping_at).map(c => {
        const sc = c.superseded_by ? by.get(c.superseded_by) : null;
        return { card: c.id, branch: c.branch || branchFor(env, c.id), superseded_by_branch: sc ? (sc.branch || branchFor(env, sc.id)) : null, dropping_at: c.dropping_at };   // dropping_at 請原樣帶回事件，用來認是哪一次放棄
      });
      return json({ cards: out, drop_checks });
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
  if (path === '/api/outbox/pending') return outboxPending(req, env);
  if (!(write ? bearerIs(req, env.TOKEN) : bearerIs(req, env.TOKEN, env.READ_TOKEN))) return json({ error: 'unauthorized' }, 401);
  if (path === '/api/outbox/claim' || path === '/api/outbox/ack' || path === '/api/outbox/release') return (path.endsWith('claim') ? outboxClaim : path.endsWith('ack') ? outboxAck : outboxRelease)(req.clone(), env);
  let m;
  if (req.method === 'GET' && path === '/api/cards') {
    const status = url.searchParams.get('status');
    const rows = status
      ? await db.prepare('SELECT * FROM cards WHERE status=? ORDER BY priority,created_at').bind(status).all()
      : await db.prepare('SELECT * FROM cards ORDER BY priority,created_at').all();
    let out = (await withParent(db, rows.results.map(view)));
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
      const r = await db.prepare(`UPDATE cards SET ${set},updated_at=? WHERE id=? AND status=? AND IFNULL(submitted_commit,'')=? AND IFNULL(approved_commit,'')=? AND retest=? AND IFNULL(rejected_reason,'')=?`)
        .bind(...vals, now(), c.id, c.status, c.submitted_commit ?? '', c.approved_commit ?? '', c.retest, c.rejected_reason ?? '').run();
      return r.meta.changes === 1;
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
      await notify(db, list(env.REVIEW_THREADS), c, 'review_requested', String(body.commit).slice(0, 12));
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
      if (!await upd(`approved_commit=?,approved_by=?,approved_at=?,${QUEUE_CLEAR}`, String(body.commit), actor, now())) return lost();
      return done(`放行 ${String(body.commit).slice(0, 12)}`);
    }
    if (act === 'revoke') {
      if (stage === 'reviewing') return json(view(c));
      if (!['selftesting', 'batching'].includes(stage)) return no();
      if (!await upd(`${APPROVAL_CLEAR},${QUEUE_CLEAR}`)) return lost();
      return done('撤回放行');
    }
    // retest：只有被排隊器退回（含暫停）的卡；放行保留、重測計數加一、清排隊器結果，回到審查通過自測中
    if (stage !== 'rejected' || c.rejected_reason) return no(c.rejected_reason ? '被審查退回的卡不能重測，要改好再送審' : undefined);
    if (!await upd(`retest=retest+1,${QUEUE_CLEAR}`)) return lost();
    return done(`重測（第 ${c.retest + 1} 次）`);
  }
  if (req.method === 'POST' && path === '/api/cards') {
    if (!body.id || !body.title) return json({ error: 'id 與 title 必填' }, 400);
    { const bad = await badParent(db, String(body.id), body.parent); if (bad) return json({ error: bad }, 409); }
    const t = now();
    try {
      await db.prepare('INSERT INTO cards(id,title,body,area,priority,parent,owner_thread,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)')
        .bind(String(body.id), String(body.title), String(body.body || ''), String(body.area || ''), Number(body.priority ?? 100), body.parent || null, body.owner_thread ?? null, t, t).run();
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
      r.meta.changes ? added++ : skipped++;   // 分支名有唯一索引：同名分支會被略過
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
      const res = await db.prepare("UPDATE cards SET status='claimed',owner=?,branch=COALESCE(branch,?),owner_thread=?,updated_at=? WHERE id=? AND status='open' AND dropping_at IS NULL")
        .bind(String(body.owner), branch, body.owner_thread ? String(body.owner_thread) : null, now(), id).run();
      if (res.meta.changes === 1) {
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
    if ((c.status === 'dropped' && body.status !== 'dropped') || c.phase === 'merged') return json({ error: `現在是「${stageOf(c)}」，已結束的卡不能再改`, stage: stageOf(c) }, 409);
    const resend = body.status === 'dropped' && Object.keys(body).every(k => ['status', 'actor', 'superseded_by', 'discard_reason', 'note'].includes(k));
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
          const sup = body.superseded_by ? String(body.superseded_by) : '', why = String(body.discard_reason ?? '').trim();
          if (!sup && !why) return json({ error: '放棄要帶 superseded_by（由哪張未放棄的卡接手）或 discard_reason（真的不要了的原因）', stage }, 400);
          if (sup) {
            const sc = sup === id ? null : await db.prepare('SELECT status FROM cards WHERE id=?').bind(sup).first();
            if (!sc || sc.status === 'dropped') return json({ error: 'superseded_by 必須是另一張存在且未放棄的卡' }, 409);
          }
          sets.push('dropping_at=?', 'superseded_by=?', 'discard_reason=?', 'drop_unmerged=NULL', 'drop_checked_at=NULL'); vals.push(Math.max(now(), (c.dropping_at || 0) + 1), sup || null, why || null);   // 新的一輪：dropping_at 一定比上一輪大，上一輪晚到的結果對不上
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
  scheduled: (_ev, env, ctx) => ctx.waitUntil(stuckCheck(env).then(() => autoFlush(env))),
};
