"""Git 操作介面（DESIGN 原則 1、4）。對被測專案只准兩種寫入：
fast_forward_main、delete_branch。其餘都在自己的 work_dir 副本裡做。
骨架：只定介面，實作由 Git 串負責。"""
from typing import Dict, List, Optional, Protocol


class Repo(Protocol):
    def fetch(self) -> None: ...
    def list_branches(self) -> Dict[str, str]:
        """符合 branch_patterns 的遠端分支 → head sha。"""
    def main_sha(self) -> str: ...
    def content_id(self, branch_sha: str, base_sha: str) -> str:
        """分支相對 base 的內容指紋（patch-id）；重疊到新 main 不變。"""
    def changed_files(self, branch_sha: str, base_sha: str) -> List[str]: ...
    def conflicts_with(self, branch_sha: str, base_sha: str) -> bool:
        """這條分支疊到 base 上會不會撞檔。"""
    def stack(self, base_sha: str, branch_shas: List[str]) -> List[str]:
        """每條分支壓成一個提交、依序疊在 base 上；回傳每個前綴的提交 sha。
        撞檔的那條回 None（留下一批）。"""
    def worktree(self, sha: str) -> "ContextManager[str]":
        """在 work_dir 檢出 sha 的唯讀副本，離開時清掉。"""
    def fast_forward_main(self, sha: str, expect_main: str) -> None:
        """推 main：遠端 main 必須仍是 expect_main，否則丟錯。絕不 force。"""
    def delete_branch(self, name: str, expect_sha: str) -> None:
        """刪已合入的分支；sha 對不上就不刪。"""
