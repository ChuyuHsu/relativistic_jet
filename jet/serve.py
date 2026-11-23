"""排隊器主行程（DESIGN 原則 2：狀態只有主行程能寫）。

這個檔把介面與維運的零件接起來：
  每一輪：執行 HTTP／命令列送來的寫入命令 → 設定檔熱重載（先自檢）→ 調度核心 tick
         → 通知 → 每小時備份。
調度核心（scheduler）由 `scheduler_factory(cfg, con, wq)` 提供；沒提供時只跑介面與維運
（讀得到、寫得進、會備份、會通知，但不會自己排隊測試）。
"""
import logging
import os
import threading
import time
from typing import Any, Callable, Optional

from . import api, config, db, expose, guard, maintenance, notify, ops

log = logging.getLogger("jet")
TICK_S = 0.5


def init_keys(read_key_file: str, write_key_file: str, force: bool = False) -> None:
    """產生兩把隨機鑰匙（檔案權限 600）。已存在就不蓋掉，除非 force。"""
    import secrets
    for p in (read_key_file, write_key_file):
        if os.path.exists(p) and not force:
            raise FileExistsError(f"{p} 已存在（要覆蓋請加 --force）")
    for p in (read_key_file, write_key_file):
        os.makedirs(os.path.dirname(os.path.abspath(p)), exist_ok=True)
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(secrets.token_urlsafe(32) + "\n")


class Server:
    """可在測試裡直接建立、手動 `step()`，也可以 `run_forever()`。"""

    def __init__(self, config_path: str,
                 scheduler_factory: Optional[Callable[[config.Config, Any, ops.WriteQueue], Any]] = None,
                 now: Callable[[], float] = time.time):
        self.config_path = config_path
        self.now = now
        self.cfg = config.load(config_path)
        self.state_dir = self.cfg.state_dir
        os.makedirs(self.state_dir, exist_ok=True)
        self.lock = maintenance.StateLock(self.state_dir)
        if not self.lock.acquire():
            raise RuntimeError("另一個排隊器已經在用這個 state_dir")
        try:
            self.con = db.connect(maintenance.db_path(self.state_dir))
            guard.ingest_events(self.con, self.state_dir)
            rk = api.read_key_file(self.cfg.read_key_file, "讀取")
            wk = api.read_key_file(self.cfg.write_key_file, "寫入")
            self.wq = ops.WriteQueue()
            self.api = api.ApiServer(maintenance.db_path(self.state_dir), self.wq, rk, wk, self.cfg.listen,
                                      hide=[self.cfg.state_dir, self.cfg.work_dir, os.path.abspath(self.cfg.state_dir),
                                            os.path.abspath(self.cfg.work_dir)])
            self.notifier = notify.Notifier(self.cfg.notify_url, self.cfg.notify_batch_s)
            self.backups = maintenance.BackupSchedule(self.state_dir)
            self.watcher = maintenance.ConfigWatcher(config_path, self.cfg)
            self.exposer = self._make_exposer(self.cfg)
            self.scheduler = scheduler_factory(self.cfg, self.con, self.wq) if scheduler_factory else None
        except BaseException:
            self.lock.release()
            raise
        self._stop = threading.Event()

    # ------------------------------------------------------------
    def _pre_commit(self, action: str, args: dict, now: float) -> None:
        # 撤回要先落到只增不改的紀錄檔（fsync）才提交；寫失敗就整個撤回失敗，避免還原後撤回過的放行復活
        if action == "revoke":
            maintenance.log_revocation(self.state_dir, str(args.get("branch")), now)

    @staticmethod
    def _make_exposer(cfg):
        e = cfg.expose
        if not e:
            return None
        return expose.Exposer(e["tunnel_command"], e["url_pattern"], e["register_url"],
                              e["register_token_file"], e["reregister_seconds"])

    def step(self) -> None:
        now = self.now()
        if self.exposer is not None:
            for msg in self.exposer.drain_events():   # 資料庫只有主行程寫
                maintenance.record_queue_error(self.con, msg, now)
        self.wq.drain(self.con, now, pre_commit=self._pre_commit)
        cfg, err = self.watcher.poll()
        if err:
            log.warning("%s", err)
            maintenance.record_queue_error(self.con, err, now)
        elif cfg is not self.cfg:
            self.cfg = cfg
            self.notifier = notify.Notifier(cfg.notify_url, cfg.notify_batch_s)
            if hasattr(self.scheduler, "update_config"):
                self.scheduler.update_config(cfg)
        if self.scheduler is not None:
            self.scheduler.tick()
        self.notifier.tick(self.con, now)
        if self.backups.tick(self.con, now) is None and self.backups.last_error:
            log.warning("備份失敗：%s", self.backups.last_error)

    def run_forever(self) -> None:
        self.api.start()
        if self.exposer is not None:
            self.exposer.start()
        host, port = self.api.address
        log.info("介面開在 %s:%d%s", host, port, "" if self.scheduler else "（調度核心未接上，只跑介面與維運）")
        try:
            while not self._stop.is_set():
                try:
                    self.step()
                except Exception as e:   # 單一輪出錯不能弄掛主行程；記下來、通知，下一輪再來
                    log.exception("主迴圈這一輪出錯")
                    try:
                        msg = f"{type(e).__name__}: {e}"[:300]
                        if msg != getattr(self, "_last_loop_err", None):     # 同樣的錯不洗版
                            self._last_loop_err = msg
                            maintenance.record_queue_error(self.con, "主迴圈出錯：" + msg, self.now())
                        import json as _j
                        self.con.execute(
                            "INSERT INTO meta(key,value) VALUES('obs_last_error',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            (_j.dumps({"at": self.now(), "where": "主迴圈", "message": msg}, ensure_ascii=False),))
                    except Exception:
                        pass
                self._stop.wait(TICK_S)
        finally:
            self.close()

    def stop(self) -> None:
        self._stop.set()

    def close(self) -> None:
        try:
            if self.exposer is not None:
                self.exposer.stop()
            self.api.stop()
        finally:
            try:
                self.con.close()
            finally:
                self.lock.release()
