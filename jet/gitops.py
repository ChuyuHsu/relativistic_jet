"""Git 操作（DESIGN 原則 1、4）。對被測專案只准兩種寫入：
fast_forward_main、delete_branch。其餘都在自己的 work_dir 副本（一個 bare 鏡像）裡做。

需要 git ≥ 2.38（用 `merge-tree --write-tree` 做不碰工作目錄的三方合併）。只用標準庫。

堆疊分支：content_id／changed_files／conflicts_with 的 base 由呼叫端給（堆疊分支給父分支 head），
指紋與改動檔只看 merge-base(分支, base)..分支。stack 自己偵測：已疊進去的分支裡，head 是這條分支祖先的，
就以它（最新的那個）當共同祖先，這樣子分支只套自己的那一段，不會跟已壓合的父分支撞自己。
"""
import contextlib
import fnmatch
import os
import re
import shutil
import subprocess
import uuid
from dataclasses import dataclass
from typing import ContextManager, Dict, Iterator, List, Optional, Protocol, Sequence, Tuple

MIN_GIT = (2, 38)
EMPTY_ID = "empty"  # 沒有任何改動的分支的內容指紋

# 壓合提交用固定身分與時間，同樣的輸入一定得到同樣的 sha
_IDENT = {
    "GIT_AUTHOR_NAME": "jet", "GIT_AUTHOR_EMAIL": "jet@localhost",
    "GIT_COMMITTER_NAME": "jet", "GIT_COMMITTER_EMAIL": "jet@localhost",
}


class GitError(Exception):
    pass


class MainMoved(GitError):
    """fast_forward_main 時遠端 main 已不是 expect_main。"""


class StaleBranch(GitError):
    """遠端分支已不是預期的 sha。"""


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
    def drop_check(self, branch: str, superseded_by: Optional[str] = None) -> Dict[str, object]:
        """放棄防呆：branch 的改動是否已全部在目標（superseded_by 的頭，沒有就 main）裡。"""
    def stack(self, base_sha: str, items: List[Tuple[str, str]]) -> StackResult:
        """items＝[(分支名, head sha)]。每條壓成一個提交、依序疊在 base 上。
        跟前面已疊進去的撞檔的那條略過（不在 kept 裡），其餘照疊。
        同樣輸入必須產生同樣的 sha（提交者、時間固定），方便重疊與比對。"""
    def worktree(self, sha: str) -> ContextManager[str]:
        """在 work_dir 檢出 sha 的唯讀副本，離開時清掉。"""
    def fast_forward_main(self, sha: str, expect_main: str) -> None:
        """推 main：遠端 main 必須仍是 expect_main，否則丟 MainMoved。絕不 force。"""
    def is_ancestor(self, a: str, b: str) -> bool:
        """a 是不是 b 的祖先（含相同）。復原時用來看批測的終點有沒有已經進了 main。"""
    def delete_branch(self, name: str, expect_sha: str) -> None:
        """刪已合入的分支；sha 對不上就不刪。"""


class _Worktree:
    """worktree 的離開動作。不用 generator：排隊器換版交接時直接退出，generator 被回收會觸發 finally 把
    還在跑的測試的目錄刪掉；這個物件沒有終結動作，退出時什麼都不做。"""

    def __init__(self, repo: "GitRepo", path: str):
        self.repo, self.path = repo, path

    def __enter__(self) -> str:
        return self.path

    def __exit__(self, *a) -> None:
        self.repo._remove_worktree(self.path)


class GitRepo:
    def __init__(self, remote: str, work_dir: str, main: str = "main",
                 branch_patterns: Sequence[str] = ("*",), timeout_s: float = 900,
                 fetch_timeout_s: float = 3600):
        self.timeout_s = timeout_s
        self.fetch_timeout_s = fetch_timeout_s
        self.remote = remote
        self.main = main
        self.patterns = list(branch_patterns) or ["*"]
        self.work_dir = os.path.abspath(work_dir)
        self.mirror = os.path.join(self.work_dir, "mirror.git")
        self.wt_root = os.path.join(self.work_dir, "wt")
        self.squashed_tips: frozenset = frozenset()    # 已合入（squash）的分支當時的尖端，由排隊器每輪更新
        self._check_version()
        os.makedirs(self.work_dir, exist_ok=True)
        if not os.path.isdir(self.mirror):
            self._git("init", "--bare", "-q", self.mirror, cwd=self.work_dir)
        self._git("remote", "remove", "origin", check=False)
        self._git("remote", "add", "origin", remote)

    def commits_between(self, base: str, head: str) -> List[str]:
        """base 之後到 head 的提交（舊→新，只走第一親代）；讀不到回空。"""
        try:
            return self._git("rev-list", "--first-parent", "--reverse", f"{base}..{head}").stdout.split()
        except GitError:
            return []

    def commit_ids(self, sha: str, base: str) -> frozenset:
        """base 之外、sha 以內（不含合併提交）每個提交「只看加減的行」的指紋集合（git patch-id --stable，零行上下文）。
        重疊／解衝突時上下文會變、加減的行不變，所以指紋不變；內容真的改了、合併或拆提交就會不同。讀不到回空集合。"""
        try:
            log = self._git("log", "-p", "-U0", "--no-color", "--no-ext-diff", "--no-merges", f"{base}..{sha}").stdout
            if not log.strip():
                return frozenset()
            out = self._git("patch-id", "--stable", input=log, cwd=self.work_dir).stdout
            return frozenset(line.split()[0] for line in out.splitlines() if line.split())
        except GitError:
            return frozenset()

    def commit_files(self, sha: str) -> List[str]:
        """這個提交（相對第一親代）改了哪些檔；讀不到回空（呼叫端要把空當「不知道」）。"""
        try:
            return self._git("diff", "--name-only", f"{sha}^", sha).stdout.split("\n")[:-1] or \
                self._git("show", "--name-only", "--format=", sha).stdout.split()
        except GitError:
            return []

    def files_between(self, a: str, b: str) -> List[str]:
        """a 到 b 之間改了哪些檔；讀不到回空。"""
        try:
            return [x for x in self._git("diff", "--name-only", a, b).stdout.split("\n") if x]
        except GitError:
            return []

    def commit_subject(self, sha: str) -> str:
        try:
            return self._git("log", "-1", "--format=%s", sha).stdout.strip()[:200]
        except GitError:
            return ""

    def commit_messages(self, base: str, head: str) -> str:
        """base..head 之間所有提交的完整訊息（讀不到回空字串）。"""
        try:
            return self._git("log", "--format=%B%x00", f"{base}..{head}").stdout
        except GitError:
            return ""

    def nongate_ids(self, sha: str, reg_dir: str) -> List[str]:
        """登記目錄（例如 tests/shops）裡 "gate": false 的檔名（不含 .json）；讀不到就回空（不擋也不放寬）。"""
        import json as _json
        out: List[str] = []
        try:
            names = self._git("ls-tree", "--name-only", sha, reg_dir.rstrip("/") + "/").stdout.split()
        except GitError:
            return out
        for n in names:
            if not n.endswith(".json"):
                continue
            try:
                d = _json.loads(self._git("show", f"{sha}:{n}").stdout)
            except (GitError, ValueError):
                continue
            if isinstance(d, dict) and d.get("gate") is False:
                out.append(os.path.basename(n)[:-5])
        return out

    # ---- 基礎 ----
    def _git(self, *args: str, cwd: Optional[str] = None, check: bool = True,
             env: Optional[dict] = None, input: Optional[str] = None,
             timeout: Optional[float] = None) -> subprocess.CompletedProcess:
        e = dict(os.environ)
        e.update({"GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C", "GIT_CONFIG_NOSYSTEM": "1"})
        if env:
            e.update(env)
        full = ["git"] + list(args)
        if cwd is None:
            full = ["git", "--git-dir", self.mirror] + list(args)
        # 自己一個行程群組＋逾時：卡住的 git（例如 ssh 等不到回應）整群殺掉，不會把主迴圈卡死
        t = self.timeout_s if timeout is None else timeout
        proc = subprocess.Popen(full, cwd=cwd, env=e, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            out, err = proc.communicate(input, timeout=t)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, 9)
            except OSError:
                pass
            proc.communicate()
            raise GitError(f"git {' '.join(args[:3])} 逾時（{t:.0f} 秒）")
        r = subprocess.CompletedProcess(full, proc.returncode, out, err)
        if check and r.returncode != 0:
            raise GitError(f"git {' '.join(args[:3])} 失敗（{r.returncode}）：{r.stderr.strip()[:400]}")
        return r

    def _out(self, *args: str, **kw) -> str:
        return self._git(*args, **kw).stdout.strip()

    @staticmethod
    def _check_version() -> None:
        try:
            out = subprocess.run(["git", "--version"], capture_output=True, text=True).stdout
        except OSError as e:
            raise GitError(f"找不到 git：{e}")
        m = re.search(r"(\d+)\.(\d+)", out)
        if not m or (int(m.group(1)), int(m.group(2))) < MIN_GIT:
            raise GitError(f"git 版本太舊（需要 ≥ {MIN_GIT[0]}.{MIN_GIT[1]}）：{out.strip()}")

    @staticmethod
    def _git_at_least(major: int, minor: int) -> bool:
        try:
            out = subprocess.run(["git", "--version"], capture_output=True, text=True).stdout
        except OSError:
            return False
        m = re.search(r"(\d+)\.(\d+)", out)
        return bool(m) and (int(m.group(1)), int(m.group(2))) >= (major, minor)

    def _attr(self, commit: str) -> List[str]:
        """合併時的 .gitattributes 取自這個提交的樹（bare 鏡像預設不讀樹裡的 .gitattributes，union 之類的 merge 驅動不會生效，
        同一檔兩邊各附加幾列就被判撞檔）。需要 git ≥ 2.40；更舊的版本維持原行為。"""
        if getattr(self, "_attr_ok", None) is None:
            self._attr_ok = self._git_at_least(2, 40)
        return ["--attr-source=" + commit] if self._attr_ok else []

    def _has(self, sha: str) -> bool:
        return self._git("cat-file", "-e", sha + "^{commit}", check=False).returncode == 0

    def _merge_base(self, a: str, b: str) -> str:
        r = self._git("merge-base", a, b, check=False)
        if r.returncode != 0 or not r.stdout.strip():
            raise GitError("兩個提交沒有共同祖先")
        return r.stdout.strip().split()[0]

    def _merge_base_for(self, branch_sha: str, base_sha: str) -> str:
        """分支跟 base 的共同祖先；疊卡的父卡被 squash 進 main 時，要改用「父卡的舊尖端」（squashed_tips）。
        squash 後 main 上只有一個內容相同的新提交，普通 merge-base 會退回父卡開出來之前，三方合併就把父卡的改動
        當成兩邊各改一次（子卡又動到同一處就撞檔）。做法：分支上、不在 base 裡的提交（新→舊）中，第一個是已合入分支的
        舊尖端的，就當共同祖先；沒有就照舊用 merge-base。"""
        tips = self.squashed_tips
        if tips:
            r = self._git("rev-list", "--topo-order", branch_sha, f"^{base_sha}", check=False)
            if r.returncode == 0:
                for c in r.stdout.split():
                    if c in tips:
                        return c
        return self._merge_base(branch_sha, base_sha)

    def _is_ancestor(self, a: str, b: str) -> bool:
        return self._git("merge-base", "--is-ancestor", a, b, check=False).returncode == 0

    def is_ancestor(self, a: str, b: str) -> bool:
        """a 是不是 b 的祖先（含相同）。找不到提交回 False。"""
        return self._has(a) and self._has(b) and self._is_ancestor(a, b)

    # ---- 讀 ----
    def fetch(self) -> None:
        self._git("fetch", "--prune", "--no-tags", "-q", "origin",
                  "+refs/heads/*:refs/remotes/origin/*", timeout=self.fetch_timeout_s)

    def mirror_info(self) -> Dict[str, object]:
        """鏡像現況（給診斷用）：存在嗎、有幾個遠端分支、物件庫大小。"""
        info: Dict[str, object] = {"path": self.mirror, "exists": os.path.isdir(self.mirror)}
        if info["exists"]:
            r = self._git("for-each-ref", "--format=x", "refs/remotes/origin", check=False)
            info["refs"] = len(r.stdout.split())
            c = self._git("count-objects", "-v", check=False).stdout
            m = re.search(r"size-pack: (\d+)", c)
            info["pack_kb"] = int(m.group(1)) if m else None
        return info

    def list_branches(self) -> Dict[str, str]:
        out = self._out("for-each-ref", "--format=%(refname:strip=3) %(objectname)", "refs/remotes/origin")
        res: Dict[str, str] = {}
        for line in out.splitlines():
            name, _, sha = line.partition(" ")
            if not name or name in ("HEAD", self.main):
                continue
            if any(fnmatch.fnmatchcase(name, p) for p in self.patterns):
                res[name] = sha
        return res

    def main_sha(self) -> str:
        r = self._git("rev-parse", "--verify", "-q", f"refs/remotes/origin/{self.main}", check=False)
        if r.returncode != 0:
            raise GitError(f"遠端沒有 {self.main}")
        return r.stdout.strip()

    def content_id(self, branch_sha: str, base_sha: str) -> str:
        """patch-id：只看加減的行，不看行號與上下文，所以重疊到新 main 不變。
        先把分支套到 base 上（三方合併），再取「base → 結果」的差：這樣父分支壓合進 main 之後，
        子分支相對 main 重算，父分支那段已在 main 裡、不會算進去，指紋與當初相對父分支時相同。
        套不上去（撞檔）時退回直接看 merge-base..分支 的差；這和不撞時的算法不同，同一份內容可能算出
        不同指紋，結果是放行失效、要重新確認——偏安全的方向。"""
        mb = self._merge_base_for(branch_sha, base_sha)
        tree = self._merge_tree(base_sha, branch_sha, mb)
        if tree is not None:
            diff = self._git("diff", "--no-color", "--no-ext-diff", base_sha, tree).stdout
        else:
            diff = self._git("diff", "--no-color", "--no-ext-diff", mb, branch_sha).stdout
        if not diff.strip():
            return EMPTY_ID
        r = self._git("patch-id", "--stable", input=diff, cwd=self.work_dir)
        pid = r.stdout.split()
        return pid[0] if pid else EMPTY_ID

    def changed_files(self, branch_sha: str, base_sha: str) -> List[str]:
        mb = self._merge_base(branch_sha, base_sha)
        out = self._git("diff", "--name-only", "-z", mb, branch_sha).stdout
        return sorted(f for f in out.split("\0") if f)

    # ---- 合併（不碰工作目錄） ----
    def _merge_tree(self, tip: str, branch_sha: str, mb: str) -> Optional[str]:
        """以 mb 為共同祖先，把 branch 的改動套到 tip 上；回傳結果 tree，撞檔回 None。"""
        r = self._git(*self._attr(tip), "merge-tree", "--write-tree", f"--merge-base={mb}", tip, branch_sha, check=False)
        if r.returncode == 0:
            return r.stdout.split()[0]
        if r.returncode == 1:
            return None
        raise GitError(f"merge-tree 失敗（{r.returncode}）：{r.stderr.strip()[:400]}")

    def conflicts_with(self, branch_sha: str, base_sha: str) -> bool:
        mb = self._merge_base_for(branch_sha, base_sha)
        if self._merge_tree(base_sha, branch_sha, mb) is not None:
            return False
        return self._retry_plain_base(base_sha, branch_sha, mb) is None

    def _retry_plain_base(self, tip: str, branch_sha: str, used_mb: str) -> Optional[str]:
        """squashed_tips 挑出的共同祖先只對「疊卡的父被 squash」才對；分支歷史裡剛好有已合入分支的舊尖端
        （例如退版後又疊回來的卡，歷史含退版提交）時，拿它當祖先會把 main 之後的改動算成雙邊修改而誤判撞檔
        （某張疊卡連退四次）。這種情況再用普通 merge-base 算一次，乾淨就不算撞檔。"""
        plain = self._merge_base(branch_sha, tip)
        if plain == used_mb:
            return None
        return self._merge_tree(tip, branch_sha, plain)

    def _ref_sha(self, branch: str) -> Optional[str]:
        r = self._git("rev-parse", "--verify", "-q", f"refs/remotes/origin/{branch}^{{commit}}", check=False)
        return r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else None

    def _probe(self, target: str, branch_sha: str):
        """把 branch 合進 target 試試看（不寫任何 ref）。回傳 (全部已包含, 差異檔清單)。
        已包含＝無衝突、而且合出來的 tree 跟 target 的 tree 完全相同（也就是合了什麼都不變）。"""
        ttree = self._out("rev-parse", f"{target}^{{tree}}")
        r = self._git(*self._attr(target), "merge-tree", "--write-tree", "--name-only", "--no-messages", target, branch_sha, check=False)
        if r.returncode not in (0, 1):                     # 例如沒有共同祖先：直接比兩邊的檔案
            out = self._git("diff", "--name-only", "-z", target, branch_sha).stdout
            return False, sorted(f for f in out.split("\0") if f)
        lines = r.stdout.split("\n")
        tree, conflicts = lines[0].strip(), [x for x in lines[1:] if x.strip()]
        if r.returncode == 0 and tree == ttree:
            return True, []
        out = self._git("diff", "--name-only", "-z", ttree, tree).stdout
        files = {f for f in out.split("\0") if f} | set(conflicts)
        return False, sorted(files)

    HISTORY_DEPTH = 200      # 逐檔比內容時，往 main 歷史回查同一檔的版本數上限

    def _blob(self, rev: str, path: str) -> Optional[str]:
        """ rev 這個提交裡 path 的內容指紋；沒有這個檔回 None。"""
        r = self._git("rev-parse", "--verify", "-q", f"{rev}:{path}", check=False)
        return r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else None

    def _still_missing(self, files: List[str], branch_sha: str, targets: Sequence[str], main_sha: str) -> List[str]:
        """逐檔比內容：分支上的這個檔，符合下列任一條就算「沒漏」，其餘才算缺的。
        ①內容等於任一目標（main 現在的版本、接手卡分支的版本）；②等於 merge-base..main 之間該檔的某一版（最多回查 HISTORY_DEPTH 版，
        代表分支開出後 main 已經走過這一版）。分支沒有這個檔（刪掉）時，只有目標也沒有才算沒漏。"""
        try:
            base = self._merge_base(branch_sha, main_sha)
        except GitError:
            base = None                                    # 沒有共同祖先：不回查歷史，除了目標相同的都當缺
        left = []
        for f in files:
            try:
                mine = self._blob(branch_sha, f)
                if any(self._blob(t, f) == mine for t in targets):
                    continue
                if mine is not None and base is not None:
                    # 只認分支開出來「之後」main 才出現的版本；還原成開分支之前的舊版不算已走過
                    hist = self._git("log", f"-n{self.HISTORY_DEPTH}", "--format=%H", f"{base}..{main_sha}", "--", f, check=False).stdout
                    if any(self._blob(c, f) == mine for c in hist.split()):
                        continue
            except GitError:
                pass                                       # 查不出來就當缺：寧可誤擋不可誤放
            left.append(f)
        return left

    def drop_check(self, branch: str, superseded_by: Optional[str] = None, max_files: int = 20) -> Dict[str, object]:
        """放棄防呆：這條分支的改動，是不是已經全部在目標（取代它的分支頭；沒有或已不存在就是 main）裡。
        先看「合進去之後有沒有變」；有變的檔再逐檔比內容（見 _still_missing），內容 main 已有或已走過的不算缺。
        寧可誤擋不可誤放。"""
        main_sha = self.main_sha()
        bsha = self._ref_sha(branch)
        tsha = self._ref_sha(superseded_by) if superseded_by else None
        target = superseded_by if tsha else self.main
        res: Dict[str, object] = {"branch": branch, "target": target, "branch_sha": bsha,
                                  "target_sha": tsha or main_sha, "main_sha": main_sha}
        if bsha is None:                                   # 分支已不存在：沒有東西會遺失
            res.update(unmerged=False, covered=True, missing_count=0, missing_files=[])
            return res
        in_main, main_files = self._probe(main_sha, bsha)
        if not in_main:
            main_files = self._still_missing(main_files, bsha, [main_sha], main_sha)
            in_main = not main_files
        res["unmerged"] = not in_main
        if tsha is None:
            covered, files = in_main, main_files
        else:
            covered, files = self._probe(tsha, bsha)
            if not covered:
                files = self._still_missing(files, bsha, [tsha, main_sha], main_sha)
                covered = not files
        res.update(covered=covered, missing_count=len(files), missing_files=files[:max_files])
        return res

    def stack(self, base_sha: str, items: List[Tuple[str, str]]) -> StackResult:
        """前提（呼叫端負責）：堆疊分支的父分支要排在子之前、也在 items 裡，或已合入 base。
        父不在 items 裡又還沒合入時，子會連父的改動一起壓進來。"""
        tip = base_sha
        kept: List[int] = []
        prefixes: List[str] = []
        kept_heads: List[str] = []
        for i, (_name, b) in enumerate(items):
            # 已疊進去而且是這條祖先的分支（堆疊的父）：以最新的那個當共同祖先
            mb = None
            for c in (k for k in kept_heads if self._is_ancestor(k, b)):
                if mb is None or self._is_ancestor(mb, c):
                    mb = c
            if mb is None:
                mb = self._merge_base_for(b, tip)
            tree = self._merge_tree(tip, b, mb)
            if tree is None:
                tree = self._retry_plain_base(tip, b, mb)     # 用「已合入分支舊尖端」當共同祖先算出撞檔時，再用普通 merge-base 算一次
            if tree is None:
                continue
            subject = self._out("log", "-1", "--format=%s", b) or "squash"
            when = self._out("log", "-1", "--format=%cI", b)
            env = dict(_IDENT, GIT_AUTHOR_DATE=when, GIT_COMMITTER_DATE=when)
            msg = f"{subject}\n\n壓合自 {b[:12]}\n"
            tip = self._out("commit-tree", tree, "-p", tip, "-m", msg, env=env)
            kept.append(i)
            prefixes.append(tip)
            kept_heads.append(b)
        return StackResult(kept, prefixes)

    # ---- worktree ----
    def worktree(self, sha: str) -> "_Worktree":
        if not self._has(sha):
            raise GitError(f"副本裡沒有這個提交：{sha[:12]}")
        os.makedirs(self.wt_root, exist_ok=True)
        path = os.path.join(self.wt_root, uuid.uuid4().hex[:12])
        self._git("worktree", "add", "--detach", "-q", path, sha)
        return _Worktree(self, path)

    def adopt_worktree(self, path: str) -> "_Worktree":
        """接管上一代留下、測試還在用的 worktree（之後離開時照常清掉）。"""
        return _Worktree(self, path)

    def worktree_exists(self, path: str) -> bool:
        p = os.path.abspath(path)
        return p.startswith(self.wt_root + os.sep) and os.path.isdir(p)

    def _remove_worktree(self, path: str) -> None:
        self._git("worktree", "remove", "--force", path, check=False)
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        self._git("worktree", "prune", check=False)

    def cleanup_worktrees(self, keep: Sequence[str] = ()) -> int:
        """啟動時清掉上次當掉留下的 worktree，回傳清掉幾個。keep＝被接管的工作還在用的目錄，不動。"""
        n = 0
        keep_set = {os.path.abspath(k) for k in keep}
        if os.path.isdir(self.wt_root):
            for d in os.listdir(self.wt_root):
                if os.path.abspath(os.path.join(self.wt_root, d)) in keep_set:
                    continue
                shutil.rmtree(os.path.join(self.wt_root, d), ignore_errors=True)
                n += 1
        self._git("worktree", "prune", check=False)
        return n

    # ---- 唯二的寫入 ----
    def _remote_sha(self, branch: str) -> Optional[str]:
        out = self._out("ls-remote", "origin", f"refs/heads/{branch}")
        for line in out.splitlines():
            sha, _, ref = line.partition("\t")
            if ref == f"refs/heads/{branch}":
                return sha
        return None

    def fast_forward_main(self, sha: str, expect_main: str) -> None:
        if not self._has(sha):
            raise GitError(f"副本裡沒有這個提交：{sha[:12]}")
        cur = self._remote_sha(self.main)
        if cur != expect_main:
            raise MainMoved(f"遠端 {self.main} 已經是 {str(cur)[:12]}，不是預期的 {expect_main[:12]}")
        if self._git("merge-base", "--is-ancestor", expect_main, sha, check=False).returncode != 0:
            raise GitError("這個提交不是預期 main 的後代，不是快轉")
        # 不帶任何 force：就算中間被別人動過，非快轉會被遠端拒絕
        r = self._git("push", "-q", "origin", f"{sha}:refs/heads/{self.main}", check=False)
        if r.returncode != 0:
            now = self._remote_sha(self.main)
            if now != expect_main:
                raise MainMoved(f"推送時 {self.main} 已被動過：{r.stderr.strip()[:200]}")
            raise GitError(f"推 main 失敗：{r.stderr.strip()[:300]}")
        self._git("update-ref", f"refs/remotes/origin/{self.main}", sha)

    def delete_branch(self, name: str, expect_sha: str) -> None:
        if name == self.main:
            raise GitError("不准刪 main")
        r = self._git("push", "-q", f"--force-with-lease=refs/heads/{name}:{expect_sha}",
                      "origin", f":refs/heads/{name}", check=False)
        if r.returncode != 0:
            cur = self._remote_sha(name)
            if cur is None:
                return  # 已經不在了
            if cur != expect_sha:
                raise StaleBranch(f"分支 {name} 已有新提交（{cur[:12]}），沒刪")
            raise GitError(f"刪分支失敗：{r.stderr.strip()[:300]}")
        self._git("update-ref", "-d", f"refs/remotes/origin/{name}", check=False)

