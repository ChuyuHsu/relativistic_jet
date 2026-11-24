"""gitops 測試：全部用臨時 git repo 與合成內容，不碰任何真實專案。"""
import os
import shutil
import subprocess
import tempfile
import unittest

from jet.gitops import GitError, GitRepo, StaleBranch, MainMoved

ENV = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@x", GIT_COMMITTER_NAME="t",
           GIT_COMMITTER_EMAIL="t@x", GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0")


def sh(cwd, *args):
    r = subprocess.run(["git"] + list(args), cwd=cwd, env=ENV, capture_output=True, text=True)
    assert r.returncode == 0, (args, r.stderr)
    return r.stdout.strip()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="jet_t_")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.remote = os.path.join(self.tmp, "remote.git")
        sh(self.tmp, "init", "--bare", "-q", "-b", "main", self.remote)
        self.dev = os.path.join(self.tmp, "dev")
        sh(self.tmp, "clone", "-q", self.remote, self.dev)
        sh(self.dev, "checkout", "-q", "-b", "main")
        lines = "".join(f"line {i}\n" for i in range(1, 31))
        self.write("a.txt", lines)
        self.write("b.txt", "bbb\n")
        self.write("c.txt", "ccc\n")
        self.commit("初始")
        sh(self.dev, "push", "-q", "origin", "main")
        self.repo = GitRepo(self.remote, os.path.join(self.tmp, "work"))
        self.repo.fetch()

    def write(self, name, text):
        p = os.path.join(self.dev, name)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            f.write(text)

    def commit(self, msg):
        sh(self.dev, "add", "-A")
        sh(self.dev, "commit", "-q", "-m", msg)
        return sh(self.dev, "rev-parse", "HEAD")

    def remote_heads(self):
        out = sh(self.dev, "ls-remote", "--heads", "origin")
        return {l.split("refs/heads/")[1] for l in out.splitlines()}

    def push(self, name):
        sh(self.dev, "push", "-q", "-f", "origin", name)
        self.repo.fetch()
        return self.repo.list_branches()[name]

    def make(self, name, files, start="main", msg=None):
        """從 start 開一條分支、寫檔、提交、推上去，回傳 sha。"""
        sh(self.dev, "fetch", "-q", "origin")
        sh(self.dev, "checkout", "-q", "-B", name, f"origin/{start}")
        for k, v in files.items():
            self.write(k, v)
        self.commit(msg or f"改 {name}")
        return self.push(name)


class ListAndMain(Base):
    def test_list_and_main(self):
        s1 = self.make("f1", {"b.txt": "b1\n"})
        self.assertEqual(self.repo.list_branches(), {"f1": s1})
        self.assertEqual(self.repo.main_sha(), sh(self.dev, "rev-parse", "origin/main"))

    def test_patterns(self):
        self.make("topic/x", {"b.txt": "x\n"})
        self.make("other", {"c.txt": "y\n"})
        r = GitRepo(self.remote, os.path.join(self.tmp, "w2"), branch_patterns=["topic/*"])
        r.fetch()
        self.assertEqual(list(r.list_branches()), ["topic/x"])

    def test_pruned_branch_disappears(self):
        self.make("gone", {"b.txt": "x\n"})
        sh(self.dev, "push", "-q", "origin", ":gone")
        self.repo.fetch()
        self.assertNotIn("gone", self.repo.list_branches())


class ContentId(Base):
    def test_same_after_rebase_onto_new_main(self):
        s = self.make("f", {"b.txt": "changed\n"})
        old_main = self.repo.main_sha()
        cid = self.repo.content_id(s, old_main)
        # main 往前走（改別的檔、同檔別的位置）
        sh(self.dev, "checkout", "-q", "main")
        self.write("c.txt", "ccc2\n")
        self.commit("main 前進")
        sh(self.dev, "push", "-q", "origin", "main")
        sh(self.dev, "checkout", "-q", "f")
        sh(self.dev, "rebase", "-q", "origin/main")
        s2 = self.push("f")
        self.assertNotEqual(s, s2)
        self.assertEqual(self.repo.content_id(s2, self.repo.main_sha()), cid)

    def test_differs_when_content_differs(self):
        a = self.make("a", {"b.txt": "one\n"})
        b = self.make("b", {"b.txt": "two\n"})
        m = self.repo.main_sha()
        self.assertNotEqual(self.repo.content_id(a, m), self.repo.content_id(b, m))

    def test_multi_commit_equals_squashed(self):
        sh(self.dev, "checkout", "-q", "-B", "m", "origin/main")
        self.write("b.txt", "step1\n")
        self.commit("一")
        self.write("b.txt", "final\n")
        self.commit("二")
        m1 = self.push("m")
        s2 = self.make("s", {"b.txt": "final\n"})
        m = self.repo.main_sha()
        self.assertEqual(self.repo.content_id(m1, m), self.repo.content_id(s2, m))

    def test_empty_branch(self):
        sh(self.dev, "checkout", "-q", "-B", "e", "origin/main")
        s = self.push("e")
        self.assertEqual(self.repo.content_id(s, self.repo.main_sha()), "empty")
        self.assertEqual(self.repo.changed_files(s, self.repo.main_sha()), [])

    def test_changed_files(self):
        s = self.make("f", {"b.txt": "x\n", "dir/new.txt": "n\n"})
        self.assertEqual(self.repo.changed_files(s, self.repo.main_sha()), ["b.txt", "dir/new.txt"])


class Conflicts(Base):
    def test_no_conflict_different_files(self):
        s = self.make("f", {"b.txt": "x\n"})
        sh(self.dev, "checkout", "-q", "main")
        self.write("c.txt", "other\n")
        self.commit("m")
        sh(self.dev, "push", "-q", "origin", "main")
        self.repo.fetch()
        self.assertFalse(self.repo.conflicts_with(s, self.repo.main_sha()))

    def test_no_conflict_same_file_far_apart(self):
        lines = "".join(f"line {i}\n" for i in range(1, 31))
        s = self.make("f", {"a.txt": lines.replace("line 2\n", "TWO\n")})
        sh(self.dev, "checkout", "-q", "main")
        self.write("a.txt", lines.replace("line 28\n", "TWENTY-EIGHT\n"))
        self.commit("m")
        sh(self.dev, "push", "-q", "origin", "main")
        self.repo.fetch()
        self.assertFalse(self.repo.conflicts_with(s, self.repo.main_sha()))

    def test_conflict_same_lines(self):
        s = self.make("f", {"b.txt": "mine\n"})
        sh(self.dev, "checkout", "-q", "main")
        self.write("b.txt", "theirs\n")
        self.commit("m")
        sh(self.dev, "push", "-q", "origin", "main")
        self.repo.fetch()
        self.assertTrue(self.repo.conflicts_with(s, self.repo.main_sha()))


class ConflictsUnionAttr(Base):
    def test_union_attribute_in_tree_is_honored_in_bare_mirror(self):
        """樹裡 .gitattributes 的 merge=union 在 bare 鏡像也要生效：兩邊各在尾端附加不同列不算撞檔。"""
        if not GitRepo._git_at_least(2, 40):
            self.skipTest("git < 2.40 沒有 --attr-source")
        self.write(".gitattributes", "b.txt merge=union\n")
        self.commit("加 union 屬性")
        sh(self.dev, "push", "-q", "origin", "main")
        self.repo.fetch()
        s = self.make("f", {"b.txt": "bbb\nmine\n"})
        sh(self.dev, "checkout", "-q", "main")
        self.write("b.txt", "bbb\ntheirs\n")
        self.commit("m")
        sh(self.dev, "push", "-q", "origin", "main")
        self.repo.fetch()
        self.assertFalse(self.repo.conflicts_with(s, self.repo.main_sha()))
        res = self.repo.stack(self.repo.main_sha(), [("f", s)])
        self.assertEqual(res.kept, [0])


def items(*pairs):
    return [(n, s) for n, s in pairs]


class Stack(Base):
    def test_stack_squashes_each_branch_in_order(self):
        sh(self.dev, "checkout", "-q", "-B", "multi", "origin/main")
        self.write("b.txt", "b-1\n")
        self.commit("一")
        self.write("b.txt", "b-2\n")
        self.commit("二")
        multi = self.push("multi")
        other = self.make("other", {"c.txt": "c-1\n"})
        m = self.repo.main_sha()
        res = self.repo.stack(m, items(("multi", multi), ("other", other)))
        self.assertEqual(res.kept, [0, 1])
        r = res.prefixes
        self.assertEqual(sh(self.repo.mirror, "rev-list", "--count", f"{m}..{r[1]}"), "2")
        self.assertEqual(sh(self.repo.mirror, "rev-parse", r[0] + "^"), m)
        self.assertEqual(sh(self.repo.mirror, "rev-parse", r[1] + "^"), r[0])
        self.assertEqual(sh(self.repo.mirror, "show", f"{r[1]}:b.txt"), "b-2")
        self.assertEqual(sh(self.repo.mirror, "show", f"{r[1]}:c.txt"), "c-1")

    def test_stack_is_deterministic(self):
        a = self.make("a", {"b.txt": "x\n"})
        m = self.repo.main_sha()
        self.assertEqual(self.repo.stack(m, [("a", a)]), self.repo.stack(m, [("a", a)]))

    def test_conflicting_branch_is_skipped_and_rest_continue(self):
        a = self.make("a", {"b.txt": "from-a\n"})
        b = self.make("b", {"b.txt": "from-b\n"})
        c = self.make("c", {"c.txt": "from-c\n"})
        m = self.repo.main_sha()
        res = self.repo.stack(m, items(("a", a), ("b", b), ("c", c)))
        self.assertEqual(res.kept, [0, 2])
        self.assertEqual(len(res.prefixes), 2)
        self.assertEqual(sh(self.repo.mirror, "rev-parse", res.prefixes[1] + "^"), res.prefixes[0])
        self.assertEqual(sh(self.repo.mirror, "show", f"{res.prefixes[1]}:b.txt"), "from-a")

    def test_stacked_child_uses_parent_as_base(self):
        parent = self.make("p", {"b.txt": "parent\n"})
        child = self.make("ch", {"c.txt": "child\n"}, start="p")
        m = self.repo.main_sha()
        # 以父分支 head 當 base：孩子自己的那一段只有 c.txt
        self.assertEqual(self.repo.changed_files(child, parent), ["c.txt"])
        self.assertEqual(self.repo.changed_files(child, m), ["b.txt", "c.txt"])
        res = self.repo.stack(m, items(("p", parent), ("ch", child)))
        self.assertEqual(res.kept, [0, 1])
        self.assertEqual(sh(self.repo.mirror, "show", f"{res.prefixes[1]}:b.txt"), "parent")
        self.assertEqual(sh(self.repo.mirror, "show", f"{res.prefixes[1]}:c.txt"), "child")

    def test_stacked_child_editing_same_lines_as_parent(self):
        # 孩子改了父分支改過的同一行：只套自己那一段，不能跟已壓合的父撞
        parent = self.make("p", {"b.txt": "parent\n"})
        child = self.make("ch", {"b.txt": "child-final\n"}, start="p")
        m = self.repo.main_sha()
        res = self.repo.stack(m, items(("p", parent), ("ch", child)))
        self.assertEqual(res.kept, [0, 1])
        self.assertEqual(sh(self.repo.mirror, "show", f"{res.prefixes[1]}:b.txt"), "child-final")

    def test_three_level_chain(self):
        p = self.make("p", {"b.txt": "1\n"})
        c = self.make("c", {"b.txt": "2\n"}, start="p")
        g = self.make("g", {"b.txt": "3\n"}, start="c")
        res = self.repo.stack(self.repo.main_sha(), items(("p", p), ("c", c), ("g", g)))
        self.assertEqual(res.kept, [0, 1, 2])
        self.assertEqual(sh(self.repo.mirror, "show", f"{res.prefixes[2]}:b.txt"), "3")

    def test_stacked_child_skipped_when_parent_conflicts(self):
        a = self.make("a", {"b.txt": "from-a\n"})
        p = self.make("p", {"b.txt": "parent\n"})
        child = self.make("ch", {"c.txt": "child\n"}, start="p")
        res = self.repo.stack(self.repo.main_sha(), items(("a", a), ("p", p), ("ch", child)))
        self.assertEqual(res.kept, [0])   # 父撞了被略過，孩子帶著父的改動也撞

    def test_content_id_of_child_stable_after_parent_merged(self):
        parent = self.make("p", {"b.txt": "parent\n"})
        child = self.make("ch", {"c.txt": "child\n"}, start="p")
        before = self.repo.content_id(child, parent)
        m = self.repo.main_sha()
        top = self.repo.stack(m, [("p", parent)]).prefixes[0]
        self.repo.fast_forward_main(top, m)
        # 父合入後相對 main 重算，內容指紋相同
        self.assertEqual(self.repo.content_id(child, self.repo.main_sha()), before)

    def test_empty_items(self):
        res = self.repo.stack(self.repo.main_sha(), [])
        self.assertEqual((res.kept, res.prefixes), ([], []))


class Worktree(Base):
    def test_checkout_and_cleanup(self):
        s = self.make("f", {"b.txt": "wt\n"})
        with self.repo.worktree(s) as d:
            self.assertTrue(os.path.isdir(d))
            with open(os.path.join(d, "b.txt")) as f:
                self.assertEqual(f.read(), "wt\n")
        self.assertFalse(os.path.exists(d))
        self.assertEqual(sh(self.repo.mirror, "worktree", "list").count("\n"), 0)

    def test_cleanup_on_exception(self):
        s = self.make("f", {"b.txt": "wt\n"})
        with self.assertRaises(RuntimeError):
            with self.repo.worktree(s) as d:
                raise RuntimeError("x")
        self.assertFalse(os.path.exists(d))

    def test_stacked_commit_checks_out(self):
        s = self.make("f", {"b.txt": "wt\n"})
        top = self.repo.stack(self.repo.main_sha(), [("f", s)]).prefixes[0]
        with self.repo.worktree(top) as d:
            self.assertTrue(os.path.exists(os.path.join(d, "b.txt")))

    def test_unknown_sha(self):
        with self.assertRaises(GitError):
            with self.repo.worktree("0" * 40):
                pass

    def test_cleanup_leftovers(self):
        s = self.make("f", {"b.txt": "wt\n"})
        cm = self.repo.worktree(s)
        d = cm.__enter__()
        self.assertEqual(self.repo.cleanup_worktrees(), 1)
        self.assertFalse(os.path.exists(d))


class Writes(Base):
    def test_fast_forward_main(self):
        s = self.make("f", {"b.txt": "ff\n"})
        m = self.repo.main_sha()
        top = self.repo.stack(m, [("f", s)]).prefixes[0]
        self.repo.fast_forward_main(top, m)
        self.assertEqual(sh(self.dev, "ls-remote", self.remote, "refs/heads/main").split()[0], top)
        self.assertEqual(self.repo.main_sha(), top)

    def test_ff_refuses_if_main_moved(self):
        s = self.make("f", {"b.txt": "ff\n"})
        m = self.repo.main_sha()
        top = self.repo.stack(m, [("f", s)]).prefixes[0]
        sh(self.dev, "checkout", "-q", "main")
        self.write("c.txt", "moved\n")
        self.commit("別人推了")
        sh(self.dev, "push", "-q", "origin", "main")
        moved = sh(self.dev, "rev-parse", "HEAD")
        with self.assertRaises(MainMoved):
            self.repo.fast_forward_main(top, m)
        self.assertEqual(sh(self.dev, "ls-remote", self.remote, "refs/heads/main").split()[0], moved)

    def test_ff_refuses_non_descendant(self):
        s = self.make("f", {"b.txt": "ff\n"})   # 基於舊 main
        m = self.repo.main_sha()
        sh(self.dev, "checkout", "-q", "main")
        self.write("c.txt", "moved\n")
        self.commit("前進")
        sh(self.dev, "push", "-q", "origin", "main")
        self.repo.fetch()
        m2 = self.repo.main_sha()
        with self.assertRaises(GitError):
            self.repo.fast_forward_main(s, m2)   # s 不是 m2 的後代
        self.assertEqual(self.repo.main_sha(), m2)
        del m

    def test_delete_branch(self):
        s = self.make("done", {"b.txt": "d\n"})
        self.repo.delete_branch("done", s)
        self.assertNotIn("done", self.remote_heads())
        self.repo.fetch()
        self.assertNotIn("done", self.repo.list_branches())

    def test_delete_refuses_when_branch_advanced(self):
        s = self.make("done", {"b.txt": "d\n"})
        self.write("b.txt", "newer\n")
        self.commit("作者又推了")
        sh(self.dev, "push", "-q", "origin", "done")
        with self.assertRaises(StaleBranch):
            self.repo.delete_branch("done", s)
        self.assertIn("done", self.remote_heads())

    def test_delete_missing_branch_is_ok(self):
        self.repo.delete_branch("never", "0" * 40)

    def test_never_delete_main(self):
        with self.assertRaises(GitError):
            self.repo.delete_branch("main", self.repo.main_sha())
        self.assertIn("main", self.remote_heads())

    def test_only_two_kinds_of_writes_reach_remote(self):
        # 做完讀取、合併、壓合、worktree 之後，遠端的分支集合與 main 都沒變
        s = self.make("f", {"b.txt": "w\n"})
        before = sh(self.dev, "ls-remote", self.remote)
        m = self.repo.main_sha()
        self.repo.list_branches()
        self.repo.content_id(s, m)
        self.repo.conflicts_with(s, m)
        top = self.repo.stack(m, [("f", s)]).prefixes[0]
        with self.repo.worktree(top):
            pass
        self.assertEqual(sh(self.dev, "ls-remote", self.remote), before)


if __name__ == "__main__":
    unittest.main()


class DropCheckTest(Base):
    """放棄防呆：改動合進目標後什麼都不變，才算已包含。"""

    def branch(self, name, edits, base="main", delete=()):
        sh(self.dev, "checkout", "-q", "-B", name, f"origin/{base}" if base else "main")
        for f, t in edits.items():
            self.write(f, t)
        for f in delete:
            sh(self.dev, "rm", "-q", f)
        self.commit(name)
        sh(self.dev, "push", "-q", "-f", "origin", name)

    def to_main(self, edits, delete=()):
        sh(self.dev, "checkout", "-q", "main")
        sh(self.dev, "pull", "-q", "origin", "main")
        for f, t in edits.items():
            self.write(f, t)
        for f in delete:
            sh(self.dev, "rm", "-q", f)
        self.commit("main")
        sh(self.dev, "push", "-q", "origin", "main")

    def chk(self, b, sup=None):
        self.repo.fetch()
        return self.repo.drop_check(b, sup)

    def test_missing_branch(self):
        r = self.chk("nope")
        self.assertTrue(r["covered"])
        self.assertFalse(r["unmerged"])

    def test_untouched_branch_unmerged(self):
        self.branch("x", {"b.txt": "new\n"})
        r = self.chk("x")
        self.assertTrue(r["unmerged"])
        self.assertFalse(r["covered"])
        self.assertEqual(r["missing_files"], ["b.txt"])
        self.assertEqual(r["target"], "main")

    def test_squash_into_main(self):
        self.branch("x", {"b.txt": "new\n", "d.txt": "d\n"})
        self.to_main({"b.txt": "new\n", "d.txt": "d\n"})      # 內容一樣、但不同提交（等於 squash）
        r = self.chk("x")
        self.assertFalse(r["unmerged"])
        self.assertTrue(r["covered"])
        self.assertEqual(r["missing_count"], 0)

    def test_squash_after_cherry_misjudge(self):
        """兩個提交 squash 進 main：git cherry 會說都還沒合，這邊必須判已包含。"""
        self.branch("x", {"b.txt": "one\n"})
        self.write("c.txt", "two\n")
        self.commit("second")
        sh(self.dev, "push", "-q", "-f", "origin", "x")
        self.to_main({"b.txt": "one\n", "c.txt": "two\n"})
        self.assertFalse(self.chk("x")["unmerged"])

    def test_rebased_same_content(self):
        self.branch("x", {"b.txt": "new\n"})
        self.to_main({"c.txt": "other\n"})
        self.branch("x2", {"b.txt": "new\n"})                  # 重接到新 main 的同內容複本
        r = self.chk("x", "x2")
        self.assertTrue(r["covered"])
        self.assertEqual(r["target"], "x2")
        self.assertTrue(r["unmerged"])                          # 對 main 還沒進

    def test_superseder_changed_same_hunk(self):
        self.branch("x", {"a.txt": "".join(f"line {i}\n" for i in range(1, 31)).replace("line 5\n", "X5\n")})
        self.branch("y", {"a.txt": "".join(f"line {i}\n" for i in range(1, 31)).replace("line 5\n", "Y5\n")})
        r = self.chk("x", "y")
        self.assertFalse(r["covered"])
        self.assertIn("a.txt", r["missing_files"])

    def test_superseder_lacks_file(self):
        self.branch("x", {"b.txt": "new\n", "e.txt": "e\n"})
        self.branch("y", {"b.txt": "new\n"})
        r = self.chk("x", "y")
        self.assertFalse(r["covered"])
        self.assertEqual(r["missing_files"], ["e.txt"])

    def test_delete_vs_modify(self):
        self.branch("x", {"b.txt": "changed\n"})
        self.branch("y", {}, delete=["b.txt"])
        r = self.chk("x", "y")
        self.assertFalse(r["covered"])
        self.assertIn("b.txt", r["missing_files"])

    def test_both_delete_is_covered(self):
        self.branch("x", {}, delete=["b.txt"])
        self.branch("y", {}, delete=["b.txt"])
        self.assertTrue(self.chk("x", "y")["covered"])

    def test_rename(self):
        self.branch("x", {}, delete=["b.txt"])
        self.write("b2.txt", "bbb\n")
        self.commit("rename")
        sh(self.dev, "push", "-q", "-f", "origin", "x")
        self.branch("y", {}, delete=["b.txt"])
        self.write("b2.txt", "bbb\n")
        self.commit("rename")
        sh(self.dev, "push", "-q", "-f", "origin", "y")
        self.assertTrue(self.chk("x", "y")["covered"])
        self.branch("z", {"b2.txt": "bbb\n"})                    # 沒刪舊檔的取代卡：不算包含
        self.assertFalse(self.chk("x", "z")["covered"])

    def test_binary(self):
        def put(b, data):
            sh(self.dev, "checkout", "-q", "-B", b, "origin/main")
            with open(os.path.join(self.dev, "img.bin"), "wb") as f:
                f.write(data)
            self.commit(b)
            sh(self.dev, "push", "-q", "-f", "origin", b)
        put("x", b"\x00\x01\x02" * 50)
        put("y", b"\x00\x01\x02" * 50)
        put("z", b"\x00\x01\x03" * 50)
        self.assertTrue(self.chk("x", "y")["covered"])
        r = self.chk("x", "z")
        self.assertFalse(r["covered"])
        self.assertEqual(r["missing_files"], ["img.bin"])

    def test_superseder_gone_falls_back_to_main(self):
        self.branch("x", {"b.txt": "new\n"})
        self.to_main({"b.txt": "new\n"})
        r = self.chk("x", "deleted-branch")
        self.assertTrue(r["covered"])
        self.assertEqual(r["target"], "main")

    def test_missing_files_capped(self):
        self.branch("x", {f"f{i}.txt": "1\n" for i in range(30)})
        r = self.chk("x")
        self.assertEqual(r["missing_count"], 30)
        self.assertEqual(len(r["missing_files"]), 20)


class NongateIdsTest(unittest.TestCase):
    def test_reads_gate_false(self):
        import json, os, subprocess, tempfile
        d = tempfile.mkdtemp()
        subprocess.run(["git", "init", "-q", d], check=True)
        os.makedirs(os.path.join(d, "tests/shops"))
        for n, g in (("a", True), ("b", False)):
            json.dump({"gate": g}, open(os.path.join(d, f"tests/shops/{n}.json"), "w"))
        env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
        subprocess.run(["git", "-C", d, "add", "-A"], check=True)
        subprocess.run(["git", "-C", d, "commit", "-qm", "x"], check=True, env=env)
        sha = subprocess.run(["git", "-C", d, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
        g = GitRepo.__new__(GitRepo)
        g.mirror = os.path.join(d, ".git"); g.timeout_s = 20
        self.assertEqual(g.nongate_ids(sha, "tests/shops"), ["b"])


class DropCheckContentTest(DropCheckTest):
    """逐檔比內容（JET-03）：squash 後內容相同、分支帶舊版、真的有沒進的改動。"""

    def test_branch_carries_older_main_version(self):
        self.branch("x", {"b.txt": "v1\n"})
        self.to_main({"b.txt": "v1\n"})
        self.to_main({"b.txt": "v2\n"})                          # main 已經走過 v1 又改成 v2
        r = self.chk("x")
        self.assertTrue(r["covered"])
        self.assertFalse(r["unmerged"])
        self.assertEqual(r["missing_files"], [])

    def test_real_missing_change_still_reported(self):
        self.branch("x", {"b.txt": "mine\n", "n.txt": "only mine\n"})
        self.to_main({"b.txt": "v1\n"})
        self.to_main({"b.txt": "mine\n"})                        # b.txt 已走過；n.txt 沒進
        r = self.chk("x")
        self.assertTrue(r["unmerged"])
        self.assertEqual(r["missing_files"], ["n.txt"])
        self.assertEqual(r["missing_count"], 1)

    def test_superseder_has_same_content(self):
        self.branch("x", {"b.txt": "new\n"})
        self.branch("y", {"b.txt": "new\n"})
        self.to_main({"b.txt": "other\n"})
        r = self.chk("x", "y")
        self.assertTrue(r["covered"])

    def test_revert_to_pre_branch_version_is_missing(self):
        self.to_main({"b.txt": "old\n"})
        self.to_main({"b.txt": "current\n"})
        self.branch("x", {"b.txt": "old\n"})                    # 分支故意把檔還原成開分支之前的舊版
        self.to_main({"c.txt": "other\n"})                       # main 沒有還原
        r = self.chk("x")
        self.assertFalse(r["covered"])
        self.assertTrue(r["unmerged"])
        self.assertEqual(r["missing_files"], ["b.txt"])
