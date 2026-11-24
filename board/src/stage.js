// 看板對外的階段線（由現有欄位算出，不另存）：
// new 剛開卡 → developing 開發中 → reviewing 審查中 → selftesting 審查通過自測中 → batching 自測完畢批測中 → merged 已合併；
// rejected 退回（審查退回，或排隊器 rejected／halted）；waiting_parent 等待父卡（放棄時指定的接手卡還沒進 main，等它進 main 才算結案）；dropping 放棄待查（已申請放棄、等排隊器查有沒有改動漏掉）；dropped 放棄。
export const STAGES = ['new', 'developing', 'reviewing', 'selftesting', 'batching', 'merged', 'rejected', 'waiting_parent', 'dropping', 'dropped'];

export function stageOf(c) {
  if (c.status === 'dropped') return 'dropped';
  if (c.status === 'waiting') return 'waiting_parent';   // 等待父卡（接手卡）進 main：還沒結案，接手卡合併後才轉放棄
  if (c.phase === 'merged' || c.status === 'merged') return 'merged';
  if (c.dropping_at) return 'dropping';
  if (c.status === 'open') return 'new';
  if (c.rejected_reason) return 'rejected';
  if (!c.submitted_commit) return 'developing';
  if (!c.approved_commit) return 'reviewing';
  if (['rejected', 'halted'].includes(c.phase)) return 'rejected';
  if (['selftest_green', 'queued', 'batching'].includes(c.phase)) return 'batching';
  return 'selftesting';
}

// 放行的提交必須等於送審的提交（短 hash 當前綴比對，至少 7 碼）
export const sameCommit = (a, b) => !!a && !!b && a.length >= 7 && b.length >= 7 && (a.startsWith(b) || b.startsWith(a));
