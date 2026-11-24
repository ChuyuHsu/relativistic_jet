import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';

export function fakeD1() {
  const s = new DatabaseSync(':memory:');
  s.exec(readFileSync(new URL('../schema.sql', import.meta.url), 'utf8'));
  // 照 D1 的行為：changes 含觸發器改的列（sqlite3_total_changes 的差），用 node:sqlite 原生的 changes 會漏掉這個坑
  const total = s.prepare('SELECT total_changes() AS n');
  const runCounted = st => { const b = total.get().n; const r = st.run(); return { ...r, changes: total.get().n - b }; };
  return {
    async batch(list) {   // D1 batch：同一交易，全成功或全回滾
      s.exec('BEGIN');
      try { const out = list.map(o => ({ meta: { changes: o._run().changes } })); s.exec('COMMIT'); return out; } catch (e) { s.exec('ROLLBACK'); throw e; }
    },
    prepare(sql) {
      let args = [];
      const st = s.prepare(sql);
      const o = {
        bind(...a) { args = a; return o; },
        _run() { return runCounted({ run: () => st.run(...args) }); },
        async run() { const r = runCounted({ run: () => st.run(...args) }); return { meta: { changes: r.changes } }; },
        async first() { return st.get(...args) ?? null; },
        async all() { return { results: st.all(...args) }; },
      };
      return o;
    },
  };
}
