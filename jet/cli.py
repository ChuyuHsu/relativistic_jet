"""命令列：與 HTTP 同功能。子命令：serve status branch approve revoke priority
run-test pause resume check-config backup restore。"""
import argparse
import sys

from . import config


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="jet")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check-config", help="檢查設定檔格式")
    c.add_argument("path")
    args = p.parse_args(argv)
    if args.cmd == "check-config":
        try:
            config.load(args.path)
        except (config.ConfigError, OSError) as e:
            print(f"設定檔有問題：{e}", file=sys.stderr)
            return 1
        print("設定檔正確")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
