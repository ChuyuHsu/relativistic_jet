"""命令列：與 HTTP 同功能。

  讀：status branches branch merged todo metrics
  寫：approve revoke priority set-parent run-test pause resume resolve   （全部經 HTTP，由主行程執行）
  維運：serve check-config init-keys backup list-backups restore deploy rollback supervise

連線資訊從設定檔來（--config，或環境變數 JET_CONFIG，預設 ./jet.json）：位址用 api.listen，
鑰匙用 api.read_key_file／write_key_file。也可以用 --url 與 --read-key-file／--write-key-file 蓋過。
加 --json 輸出原始 JSON。
"""
import argparse
import json
import logging
import os
import sys
import time
from typing import Any, List, Optional

from . import config, guard, maintenance, ops, serve
from .client import Client, ClientError

STATE_LABEL = {
    "pending": "待自測", "selftesting": "自測中", "selftest_green": "自測綠", "queued": "排隊",
    "batching": "批測中", "merged": "已合入", "rejected": "退回", "halted": "暫停",
}


def _ts(t: Optional[float]) -> str:
    if not t:
        return "-"
    return time.strftime("%m-%d %H:%M:%S", time.localtime(float(t)))


def _read_file(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip() or None
    except OSError:
        return None


def _cfg_path(args) -> str:
    return args.config or os.environ.get("JET_CONFIG") or "jet.json"


def _client(args, need_write: bool = False) -> Client:
    url = args.url
    rk = _read_file(args.read_key_file)
    wk = _read_file(args.write_key_file)
    if url is None or (rk is None and wk is None):
        try:
            cfg = config.load(_cfg_path(args))
        except (config.ConfigError, OSError) as e:
            raise ClientError(f"讀不到設定檔 {_cfg_path(args)}：{e}（可用 --config 或 --url 加鑰匙檔）")
        if url is None:
            host, _, port = cfg.listen.rpartition(":")
            url = f"http://{host if host not in ('', '0.0.0.0', '::') else '127.0.0.1'}:{port}"
        rk = rk or _read_file(cfg.read_key_file)
        wk = wk or _read_file(cfg.write_key_file)
    if need_write and not wk:
        raise ClientError("找不到寫入鑰匙（api.write_key_file 或 --write-key-file）")
    return Client(url, rk, wk)


def _out(args, obj: Any, text: str) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2) if args.json else text)


def _table(rows: List[List[str]], head: List[str]) -> str:
    rows = [head] + rows
    widths = [max(len(str(r[i])) for r in rows) for i in range(len(head))]
    return "\n".join("  ".join(str(c).ljust(w) for c, w in zip(r, widths)).rstrip() for r in rows)


# ---------------------------------------------------------------- 讀
def cmd_status(args):
    s = _client(args).get("/status")
    c = s["counts"]
    lines = [f"狀態：{'已暫停' if s['paused'] else '運作中'}　排隊 {s['queue_len']}　待辦 {s['open_todo']}",
             "各狀態：" + "　".join(f"{STATE_LABEL.get(k, k)} {v}" for k, v in c.items() if v)]
    for r in s["running"]:
        lines.append(f"  跑著：#{r['id']} {r['kind']}/{r['layer']} {r.get('branch') or ''} 自 {_ts(r['started_at'])}")
    if s["last_merge"]:
        lm = s["last_merge"]
        lines.append(f"最近合入：{_ts(lm['finished_at'])}（{lm['merged_count']} 條）")
    if s.get("restored_at"):
        lines.append(f"（資料庫曾在 {_ts(float(s['restored_at']))} 從備份還原）")
    _out(args, s, "\n".join(lines))


def cmd_branches(args):
    path = "/branches" + (f"?state={args.state}" if args.state else "")
    b = _client(args).get(path)
    rows = [[x["name"], STATE_LABEL.get(x["state"], x["state"]), "是" if x["approved"] else "-",
             x["parent"] or "-", str(x["prio_override"]), _ts(x["updated_at"])] for x in b]
    _out(args, b, _table(rows, ["分支", "狀態", "放行", "父分支", "優先", "更新"]) if rows else "（沒有分支）")


def cmd_branch(args):
    from urllib.parse import quote
    b = _client(args).get("/branches/" + quote(args.name, safe="/"))
    lines = [f"{b['name']}　{STATE_LABEL.get(b['state'], b['state'])}　放行：{'是' if b['approved'] else '否'}"
             f"　父：{b['parent'] or '-'}　優先：{b['prio_override']}"]
    if b.get("reject_reason"):
        lines.append(f"退回原因：{b['reject_reason']}")
    for r in b["runs"][:5]:
        lines.append(f"  跑 #{r['id']} {r['kind']}/{r['layer']} {r['status']} {_ts(r['started_at'])}")
        for it in r["failed_items"][:10]:
            lines.append(f"      紅：{it['item_id']}　{(it.get('summary') or '')[:100]}")
    _out(args, b, "\n".join(lines))


def cmd_merged(args):
    m = _client(args).get(f"/merged?limit={args.limit}")
    _out(args, m, _table([[x["name"], (x["merged_sha"] or "")[:10], _ts(x["updated_at"])] for x in m],
                         ["分支", "提交", "時間"]) if m else "（還沒有）")


def cmd_todo(args):
    t = _client(args).get("/todo")
    lines = [f"待辦事件 {len(t['events'])}　等人處理的分支 {len(t['waiting'])}"]
    lines += [f"  事件 #{e['id']} {e['kind']} {e.get('branch') or '系統'}　{(e.get('detail') or '')[:100]}" for e in t["events"]]
    lines += [f"  {w['name']}：{STATE_LABEL.get(w['state'], w['state'])}　{(w.get('reject_reason') or '')[:100]}" for w in t["waiting"]]
    _out(args, t, "\n".join(lines))


def cmd_metrics(args):
    m = _client(args).get("/metrics")
    lines = [f"24 小時合入 {m['merged_24h']}　批 {m['batches_24h']}（作廢 {m['aborted_24h']}）　退回 {m['rejected_24h']}",
             f"自測平均 {m['selftest_avg_s']} 秒　批測平均 {m['batch_avg_s']} 秒　排隊平均等 {m['queue_wait_avg_s']} 秒"]
    _out(args, m, "\n".join(lines))


# ---------------------------------------------------------------- 寫
def _post(args, path: str, body: dict, text):
    r = _client(args, True).post(path, body)
    _out(args, r, text(r) if callable(text) else text)


def cmd_approve(args): _post(args, "/approve", {"branch": args.branch, "by": args.by}, "已放行 " + args.branch)
def cmd_revoke(args): _post(args, "/revoke", {"branch": args.branch}, "已撤回放行 " + args.branch)
def cmd_priority(args): _post(args, "/priority", {"branch": args.branch, "value": args.value}, f"{args.branch} 優先序 {args.value}")
def cmd_set_parent(args): _post(args, "/set-parent", {"branch": args.branch, "parent": args.parent}, f"{args.branch} 父分支 {args.parent or '無'}")
def cmd_resolve(args): _post(args, "/resolve", {"id": args.id}, f"待辦 #{args.id} 已處理")
def cmd_pause(args): _post(args, "/pause", {}, "已暫停（不再派新工作；跑著的跑完）")


def cmd_resume(args):
    _post(args, "/resume", {"branch": args.branch} if args.branch else {}, "已恢復" + (f" {args.branch}" if args.branch else ""))


def cmd_run_test(args):
    body = {"branch": args.branch, "layer": args.layer}
    if args.only:
        body["only"] = args.only
    _post(args, "/adhoc-run", body, lambda r: f"已排入插隊測試 #{r['run_id']}")


# ---------------------------------------------------------------- 維運
def cmd_serve(args):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    srv = serve.Server(_cfg_path(args))
    try:
        srv.run_forever()
    except KeyboardInterrupt:
        srv.stop()


def cmd_check_config(args):
    try:
        config.load(args.path)
    except (config.ConfigError, OSError) as e:
        print(f"設定檔有問題：{e}", file=sys.stderr)
        return 1
    print("設定檔正確")
    return 0


def cmd_init_keys(args):
    cfg = config.load(_cfg_path(args))
    if not cfg.read_key_file or not cfg.write_key_file:
        print("設定檔 api 區段要先寫 read_key_file 與 write_key_file 的路徑", file=sys.stderr)
        return 1
    serve.init_keys(cfg.read_key_file, cfg.write_key_file, args.force)
    print("已產生讀、寫鑰匙（權限 600）。鑰匙不要放進版本庫。")
    return 0


def cmd_backup(args):
    cfg = config.load(_cfg_path(args))
    con = ops.open_readonly(maintenance.db_path(cfg.state_dir))   # 唯讀快照，不是寫入者
    try:
        print(maintenance.make_backup(con, cfg.state_dir))
    finally:
        con.close()
    return 0


def cmd_list_backups(args):
    cfg = config.load(_cfg_path(args))
    for p in maintenance.list_backups(cfg.state_dir):
        print(p)
    return 0


def cmd_restore(args):
    cfg = config.load(_cfg_path(args))
    try:
        res = maintenance.restore(cfg.state_dir, args.source)
    except maintenance.MaintenanceError as e:
        print(f"還原失敗：{e}", file=sys.stderr)
        return 1
    _out(args, res, f"已從 {res['restored_from']} 還原（撤回補回 {res['revocations_replayed']} 筆、"
                    f"分支重設 {res['branches_reset']} 條）。壞掉的舊資料庫留在 {res['old_db']}。\n"
                    "備份之後才放行的分支要重新確認放行。")
    return 0


def cmd_deploy(args):
    res = guard.deploy(args.root, args.release)
    print(f"目前 {res['current']}，上一版 {res['previous']}")


def cmd_rollback(args):
    res = guard.rollback(args.root)
    print(f"目前 {res['current']}，上一版 {res['previous']}")


def cmd_supervise(args):
    cfg = config.load(_cfg_path(args))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sup = guard.Supervisor(args.root, cfg.state_dir, guard.default_cmd(_cfg_path(args)),
                           notify_url=cfg.notify_url)
    guard.install_signal_handlers(sup)
    return sup.run()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="jet", description="合併佇列加測試調度器")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="設定檔（預設環境變數 JET_CONFIG 或 ./jet.json）")
    common.add_argument("--url", help="排隊器網址（預設用設定檔的 api.listen）")
    common.add_argument("--read-key-file")
    common.add_argument("--write-key-file")
    common.add_argument("--json", action="store_true", help="輸出原始 JSON")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add(name, fn, help_, parents=(common,)):
        sp = sub.add_parser(name, parents=list(parents), help=help_)
        sp.set_defaults(fn=fn)
        return sp

    add("status", cmd_status, "現況")
    sp = add("branches", cmd_branches, "分支清單"); sp.add_argument("--state")
    sp = add("branch", cmd_branch, "單一分支的狀態與紅項"); sp.add_argument("name")
    sp = add("merged", cmd_merged, "已合入"); sp.add_argument("--limit", type=int, default=20)
    add("todo", cmd_todo, "待辦")
    add("metrics", cmd_metrics, "量測")
    sp = add("approve", cmd_approve, "放行"); sp.add_argument("branch"); sp.add_argument("--by", default="cli")
    sp = add("revoke", cmd_revoke, "撤回放行"); sp.add_argument("branch")
    sp = add("priority", cmd_priority, "調優先序（大的先）"); sp.add_argument("branch"); sp.add_argument("value", type=int)
    sp = add("set-parent", cmd_set_parent, "登記父分支（不給就清掉）"); sp.add_argument("branch"); sp.add_argument("parent", nargs="?")
    sp = add("run-test", cmd_run_test, "插隊跑一次指定測試")
    sp.add_argument("branch"); sp.add_argument("--layer", default="selftest", choices=ops.LAYERS); sp.add_argument("--only")
    add("pause", cmd_pause, "暫停派工")
    sp = add("resume", cmd_resume, "恢復派工；給 --branch 就是恢復那條暫停的分支"); sp.add_argument("--branch")
    sp = add("resolve", cmd_resolve, "把待辦事件標成已處理"); sp.add_argument("id", type=int)

    add("serve", cmd_serve, "啟動排隊器主行程")
    sp = sub.add_parser("check-config", help="檢查設定檔格式"); sp.add_argument("path"); sp.set_defaults(fn=cmd_check_config)
    sp = add("init-keys", cmd_init_keys, "產生讀、寫鑰匙檔"); sp.add_argument("--force", action="store_true")
    add("backup", cmd_backup, "立刻做一份備份（唯讀快照）")
    add("list-backups", cmd_list_backups, "列出備份")
    sp = add("restore", cmd_restore, "從備份還原（主行程要先停）"); sp.add_argument("--from", dest="source")
    sp = add("deploy", cmd_deploy, "換版：releases/<版本> 變成 current"); sp.add_argument("--root", required=True); sp.add_argument("release")
    sp = add("rollback", cmd_rollback, "退回上一版"); sp.add_argument("--root", required=True)
    sp = add("supervise", cmd_supervise, "守著主行程：掛了重啟，連續當掉三次自動退版"); sp.add_argument("--root", required=True)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.fn(args) or 0)
    except ClientError as e:
        print(f"錯誤：{e}", file=sys.stderr)
        return 1
    except (config.ConfigError, guard.GuardError, FileExistsError, OSError) as e:
        print(f"錯誤：{e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
