"""工作啟動器：先把自己綁在指定的核上，再用 exec 換成真正的測試指令（DESIGN 第 6 節）。

為什麼要有它：排隊器主行程裡會有別的執行緒（HTTP 等）。多執行緒的行程在 fork 之後、exec 之前
跑 Python 程式（subprocess 的 preexec_fn）不安全，可能卡死。所以排隊器開子行程時不放任何 Python
在 fork 與 exec 之間，改成 exec 這支小程式，由它自己在新行程裡綁核再 exec 指令。
行程編號不變（exec 不換 pid），所以行程群組、逾時殺群、回收都照舊。

用法（只給執行器用）：python3 -I jet/_launch.py [--exit-file <路徑>] <核清單，逗號分隔> -- <指令> [參數…]
有 --exit-file 時不 exec：自己留著當行程群組的領頭，把指令當子行程跑，等它結束後把
{"v":1,"rc":退出碼,"signal":訊號或null,"at":時間} 以「寫暫存檔再改名」寫進該路徑，然後用同樣的退出碼結束。
這樣排隊器（主行程）重啟、換版時，測試照跑，新的主行程靠這個檔案收結果，不用等也不用殺（DESIGN 換版接管）。
被 SIGKILL 殺掉的領頭來不及寫檔；那種情況由排隊器自己決定（逾時、取消、infra）。
退出碼：125＝核清單壞或綁核失敗；126＝指令不能執行；127＝找不到指令（都不是測試紅，執行器當 infra）。
只用 os 與 sys，不 import jet 套件（用 -I 執行，不吃目前目錄的任何模組）。"""
import errno
import json
import os
import signal
import sys
import time


def _write_exit(path, rc, sig):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"v": 1, "rc": rc, "signal": sig, "at": time.time()}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _supervise(cmd, exit_file):
    """子行程跑指令；領頭（自己）等它結束後寫 exit.json。收到終止訊號先轉給子行程，仍等它結束再寫。"""
    pid = os.fork()
    if pid == 0:
        try:
            os.execvp(cmd[0], cmd)
        except OSError as e:
            print("_launch：不能執行 %s：%s" % (cmd[0], e), file=sys.stderr)
            os._exit(127 if e.errno == errno.ENOENT else 126)
    def fwd(signum, _frame):
        try:
            os.kill(pid, signum)
        except OSError:
            pass
    for s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(s, fwd)
    while True:
        try:
            _, status = os.waitpid(pid, 0)
            break
        except InterruptedError:
            continue
        except ChildProcessError:
            status = 255 << 8
            break
    if os.WIFSIGNALED(status):
        sig, rc = os.WTERMSIG(status), 128 + os.WTERMSIG(status)
    else:
        sig, rc = None, os.WEXITSTATUS(status)
    try:
        _write_exit(exit_file, rc, sig)
    except OSError as e:
        print("_launch：寫不出 exit.json：%s" % e, file=sys.stderr)
    return rc


def main(argv):
    exit_file = None
    if len(argv) >= 2 and argv[0] == "--exit-file":
        exit_file, argv = argv[1], argv[2:]
    if len(argv) < 3 or argv[1] != "--":
        print("_launch：用法 _launch.py <核清單> -- <指令> [參數…]", file=sys.stderr)
        return 125
    try:
        cores = {int(x) for x in argv[0].split(",") if x.strip()}
        if not cores:
            raise ValueError("核清單是空的")
        os.sched_setaffinity(0, cores)
    except (ValueError, OSError) as e:
        print("_launch：綁核失敗：%s" % e, file=sys.stderr)
        return 125
    cmd = argv[2:]
    if exit_file:
        return _supervise(cmd, exit_file)
    try:
        os.execvp(cmd[0], cmd)
    except OSError as e:
        print("_launch：不能執行 %s：%s" % (cmd[0], e), file=sys.stderr)
        return 127 if e.errno == errno.ENOENT else 126
    return 0   # 到不了：exec 成功就不會回來


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
