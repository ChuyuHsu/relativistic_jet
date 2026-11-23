// 看板對外的階段線（由現有欄位算出，不另存）：
// new 剛開卡 → developing 開發中 → reviewing 審查中 → selftesting 審查通過自測中 → batching 自測完畢批測中 → merged 已合併；
// rejected 退回（審查退回，或排隊器 rejected／halted）；dropped 放棄。
export const STAGES = ['new', 'developing', 'reviewing', 'selftesting', 'batching', 'merged', 'rejected', 'dropped'];

export function stageOf(c) {
  if (c.status === 'dropped') return 'dropped';
  if (c.status === 'open') return 'new';
  if (c.phase === 'merged') return 'merged';
  if (c.rejected_reason || c.phase === 'rejected' || c.phase === 'halted') return 'rejected';
  if (c.approved_commit) return ['selftest_green', 'queued', 'batching'].includes(c.phase) ? 'batching' : 'selftesting';
  if (c.submitted_commit) return 'reviewing';
  return 'developing';
}

// 放行的提交必須等於送審的提交（短 hash 當前綴比對，至少 7 碼）
export const sameCommit = (a, b) => !!a && !!b && a.length >= 7 && b.length >= 7 && (a.startsWith(b) || b.startsWith(a));
