import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync, readdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;
const migs = readdirSync(root).filter(f => /^migrate_\d+\.sql$/.test(f)).sort();
const latest = Math.max(...migs.map(f => Number(f.match(/\d+/)[0])));
const run = (dbfile, args = [], env = {}) => {
  const log = join(dbfile + '.log'); writeFileSync(log, '');
  const r = spawnSync('bash', ['deploy.sh', ...args], { cwd: root, encoding: 'utf8', env: { ...process.env, WRANGLER: `node ${root}test/fake_wrangler.mjs`, FAKE_DB: dbfile, FAKE_LOG: log, ...env } });
  let calls = ''; try { calls = readFileSync(log, 'utf8'); } catch { /* 沒有 */ }
  return { ...r, calls };
};
const tmp = () => join(mkdtempSync(join(tmpdir(), 'dep-')), 'db.sqlite');
const version = f => { const d = new DatabaseSync(f); const r = d.prepare("SELECT value FROM meta WHERE key='schema_version'").get(); d.close(); return r && r.value; };

test('schema.sql 裡的 schema_version 等於最大的 migrate 編號', () => {
  assert.match(readFileSync(root + 'schema.sql', 'utf8'), new RegExp(`VALUES\\('schema_version','${latest}',0\\)`));
});

test('空資料庫：跑 schema.sql、版本記最新、部署並印 Version ID', () => {
  const f = tmp(); const r = run(f);
  assert.equal(r.status, 0, r.stderr + r.stdout);
  assert.equal(version(f), String(latest));
  assert.match(r.stdout, /Version ID: 12345678-aaaa/);
  assert.ok(r.calls.includes('file schema.sql') && r.calls.trim().endsWith('deploy'));
  assert.ok(!r.calls.includes('migrate_'));
});

test('舊資料庫沒有 schema_version：沒給 --baseline 就拒絕（不動資料庫、不部署）', () => {
  const f = tmp(); const d = new DatabaseSync(f); d.exec('CREATE TABLE cards(id TEXT); CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER)'); d.close();
  const r = run(f); assert.equal(r.status, 3); assert.ok(!r.calls.includes('deploy')); assert.equal(version(f), undefined);
});

test('--baseline N：只補跑比 N 新的 migration（依序），每支後記版本，最後部署；再跑一次什麼都不補', () => {
  // 用真的 schema.sql 的前身不好重現；這裡用全新資料庫跑完 schema 後把版本改回 17 模擬「少了 018、019」
  const f = tmp(); run(f);
  const d = new DatabaseSync(f); d.exec("UPDATE meta SET value='17' WHERE key='schema_version'; DROP TABLE thread_redirect; DROP TABLE nightly"); d.close();
  const r = run(f); assert.equal(r.status, 0, r.stderr + r.stdout);
  const files = r.calls.split('\n').filter(x => x.startsWith('file ')).map(x => x.slice(5));
  assert.deepEqual(files, migs.filter(m => Number(m.match(/\d+/)[0]) > 17));
  assert.equal(version(f), String(latest));
  const d2 = new DatabaseSync(f); assert.ok(d2.prepare("SELECT 1 FROM sqlite_master WHERE name='nightly'").get()); d2.close();
  const again = run(f); assert.ok(!again.calls.includes('file migrate_'));
});

test('--dry-run 不動資料庫也不部署；部署失敗退出碼非 0', () => {
  const f = tmp(); run(f);
  const d = new DatabaseSync(f); d.exec("UPDATE meta SET value='18' WHERE key='schema_version'"); d.close();
  const r = run(f, ['--dry-run']); assert.equal(r.status, 0); assert.match(r.stdout, /migrate_019/); assert.ok(!r.calls.includes('deploy') && !r.calls.includes('file migrate_')); assert.equal(version(f), '18');
  const bad = run(f, [], { FAKE_DEPLOY_FAIL: '1' }); assert.equal(bad.status, 1);
});
