"""假的 Repo／Executor：不碰 git、不開行程，用來測調度邏輯。

世界模型：每條分支有「會讓哪些測試項目紅」；疊起來的提交 sha＝"main:名稱+名稱…"，
紅項＝main 本來紅的＋所有成員的紅項。"""
import contextlib
from jet.executor import JobResult
from jet.gitops import MainMoved, StackResult


class FakeRepo:
    def __init__(self):
        self.main = "m0"
        self.main_red = set()
        self.branches = {}      # name → {"sha","reds","files","conflict_main","conflict_with"}
        self.deleted = []
        self.main_files = {}    # main sha → 該次更動的檔案（給純文件判斷）
        self.merged = {}
        self.ff_log = []

    def add(self, name, reds=(), gate_reds=(), sha=None, files=("src/a.py",), conflict_main=False,
            conflict_with=()):
        self.branches[name] = dict(sha=sha or f"{name}-1", reds=set(reds), gate_reds=set(gate_reds),
                                   files=list(files),
                                   conflict_main=conflict_main, conflict_with=set(conflict_with))

    def push_new(self, name, reds=(), gate_reds=(), **kw):
        old = self.branches[name]["sha"]
        n = int(old.rsplit("-", 1)[1]) + 1
        kw.setdefault("files", self.branches[name]["files"])
        self.add(name, reds, gate_reds, sha=f"{name}-{n}", **kw)

    # --- Repo 介面 ---
    def fetch(self): pass
    def list_branches(self): return {n: b["sha"] for n, b in self.branches.items()}
    def main_sha(self): return self.main

    def _by_sha(self, sha):
        for n, b in self.branches.items():
            if b["sha"] == sha:
                return n, b
        raise KeyError(sha)

    def content_id(self, sha, base):
        n, b = self._by_sha(sha)
        return f"{n}:{sorted(b['reds'])}:{sorted(b['gate_reds'])}:{b['files']}"

    def changed_files(self, sha, base):
        if sha in self.main_files or sha == self.main:
            return list(self.main_files.get(sha, []))
        return self._by_sha(sha)[1]["files"]

    def conflicts_with(self, sha, base):
        return self._by_sha(sha)[1]["conflict_main"]

    def stack(self, base, items):
        kept, names = [], []
        for i, (n, sha) in enumerate(items):
            b = self.branches[n]
            if any(m in b["conflict_with"] or n in self.branches[m]["conflict_with"] for m in names):
                continue
            kept.append(i); names.append(n)
        return StackResult(kept, [f"{base}>" + "+".join(names[:k + 1]) for k in range(len(names))])

    @contextlib.contextmanager
    def worktree(self, sha):
        yield sha

    def stack_names_of(self, sha):
        return sha.rsplit('>', 1)[1].split('+') if '>' in sha else []

    def is_ancestor(self, a, b):
        return b == a or b.startswith(a + ">")

    def fast_forward_main(self, sha, expect_main):
        if self.main != expect_main:
            raise MainMoved()
        self.main = sha
        self.ff_log.append(sha)

    def delete_branch(self, name, expect_sha):
        self.deleted.append(name)
        self.merged[name] = self.branches.pop(name)

    # --- 世界：某提交的紅項（layer＝selftest 時不含 gate_reds）---
    def reds_of(self, sha, layer="gate"):
        if ">" not in sha:
            return set(self.main_red)
        base, names = sha.rsplit(">", 1)
        reds = self.reds_of(base, layer)
        for n in names.split("+"):
            r = self.branches[n] if n in self.branches else self.merged[n]
            reds |= r["reds"]
            if layer == "gate":
                reds |= r["gate_reds"]
        return reds


class FakeExecutor:
    """start 立刻決定結果，poll 第一次就回（可設 delay 輪數）。"""
    def __init__(self, repo, delay=0, flaky=None, flaky_p=0.0, rng=None):
        self.repo, self.delay = repo, delay
        self.flaky, self.flaky_p, self.rng = flaky, flaky_p, rng
        self.started, self.cancelled = [], []
        self.live_cores = []
        self.force = {}  # commit → 強制狀態（"infra"/"timeout"）

    def start(self, job):
        # 核絕不共用
        for c in job.cores:
            assert c not in self.live_cores, f"核 {c} 被兩個工作共用"
        self.live_cores.extend(job.cores)
        h = type("H", (), {})()
        h.job, h.left, h.pgid = job, self.delay, 1000 + len(self.started)
        self.started.append(job)
        return h

    def poll(self, h):
        if h.left > 0:
            h.left -= 1
            return None
        for c in h.job.cores:
            self.live_cores.remove(c)
        sha = h.job.workdir
        st = self.force.get(sha)
        if st:
            return JobResult(st, None)
        reds = self.repo.reds_of(sha, "gate" if h.job.layer != "selftest" else "selftest")
        if self.flaky and self.rng and self.rng.random() < self.flaky_p:
            reds = set(reds) | {self.flaky}                                # 抖動項：跟程式碼無關，隨機紅
        return JobResult("fail" if reds else "pass", 1 if reds else 0, sorted(reds))

    def cancel(self, h):
        self.cancelled.append(h)
        for c in h.job.cores:
            if c in self.live_cores:
                self.live_cores.remove(c)
