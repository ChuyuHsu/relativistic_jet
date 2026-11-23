"""工作啟動器：先把自己綁在指定的核上，再用 exec 換成真正的測試指令（DESIGN 第 6 節）。

為什麼要有它：排隊器主行程裡會有別的執行緒（HTTP 等）。多執行緒的行程在 fork 之後、exec 之前
跑 Python 程式（subprocess 的 preexec_fn）不安全，可能卡死。所以排隊器開子行程時不放任何 Python
在 fork 與 exec 之間，改成 exec 這支小程式，由它自己在新行程裡綁核再 exec 指令。
行程編號不變（exec 不換 pid），所以行程群組、逾時殺群、回收都照舊。

用法（只給執行器用）：python3 -I jet/_launch.py <核清單，逗號分隔> -- <指令> [參數…]
退出碼：125＝核清單壞或綁核失敗；126＝指令不能執行；127＝找不到指令（都不是測試紅，執行器當 infra）。
只用 os 與 sys，不 import jet 套件（用 -I 執行，不吃目前目錄的任何模組）。"""
import errno
import os
import sys


def main(argv):
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
    try:
        os.execvp(cmd[0], cmd)
    except OSError as e:
        print("_launch：不能執行 %s：%s" % (cmd[0], e), file=sys.stderr)
        return 127 if e.errno == errno.ENOENT else 126
    return 0   # 到不了：exec 成功就不會回來


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
