"""Git 操作介面（DESIGN 原則 1、4）。對被測專案只准兩種寫入：
fast_forward_main、delete_branch。其餘都在自己的 work_dir 副本裡做。
骨架：只定介面，實作由 Git 串負責。"""
from dataclasses import dataclass
from typing import ContextManager, Dict, List, Optional, Protocol, Tuple


class MainMoved(Exception):
    """fast_forward_main 時遠端 main 已不是 expect_main。"""


@dataclass
class StackResult:
    kept: List[int]        # 疊進去的項目在輸入裡的位置（跟前面撞檔的被略過）
    prefixes: List[str]    # prefixes[i]＝疊完 kept[0..i] 之後的提交 sha


class Repo(Protocol):
    def fetch(self) -> None: ...
    def list_branches(self) -> Dict[str, str]:
        """符合 branch_patterns 的遠端分支 → head sha。"""
    def main_sha(self) -> str: ...
    def content_id(self, branch_sha: str, base_sha: str) -> str:
        """分支相對 base 的內容指紋（patch-id）；重疊到新 main 不變。
        堆疊分支的 base 是父分支的 head，父合入後重算（相對 main）應該相同。"""
    def changed_files(self, branch_sha: str, base_sha: str) -> List[str]: ...
    def conflicts_with(self, branch_sha: str, base_sha: str) -> bool:
        """這條分支疊到 base 上會不會撞檔。"""
    def stack(self, base_sha: str, items: List[Tuple[str, str]]) -> StackResult:
        """items＝[(分支名, head sha)]。每條壓成一個提交、依序疊在 base 上。
        跟前面已疊進去的撞檔的那條略過（不在 kept 裡），其餘照疊。
        同樣輸入必須產生同樣的 sha（提交者、時間固定），方便重疊與比對。"""
    def worktree(self, sha: str) -> ContextManager[str]:
        """在 work_dir 檢出 sha 的唯讀副本，離開時清掉。"""
    def fast_forward_main(self, sha: str, expect_main: str) -> None:
        """推 main：遠端 main 必須仍是 expect_main，否則丟 MainMoved。絕不 force。"""
    def delete_branch(self, name: str, expect_sha: str) -> None:
        """刪已合入的分支；sha 對不上就不刪。"""
