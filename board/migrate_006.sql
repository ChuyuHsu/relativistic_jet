-- 分支名唯一（排隊器遇到重複分支會整份清單拒收）。若已有重複，先手動處理再執行。
DROP INDEX IF EXISTS cards_branch;
CREATE UNIQUE INDEX IF NOT EXISTS cards_branch ON cards(branch) WHERE branch IS NOT NULL;
