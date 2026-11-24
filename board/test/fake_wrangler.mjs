// 測試用的假 wrangler：d1 execute 對本機 sqlite 檔（環境變數 FAKE_DB）跑；deploy 印出 Version ID。
import { DatabaseSync } from 'node:sqlite';
import { readFileSync, appendFileSync } from 'node:fs';
const a = process.argv.slice(2), log = process.env.FAKE_LOG;
const say = s => log && appendFileSync(log, s + '\n');
if (a[0] === 'deploy') { say('deploy'); console.log('Uploaded task-board\nCurrent Version ID: 12345678-aaaa-bbbb-cccc-1234567890ab'); process.exit(process.env.FAKE_DEPLOY_FAIL ? 1 : 0); }
const db = new DatabaseSync(process.env.FAKE_DB);
const cmd = a.find((x, i) => a[i - 1] === '--command'), file = (a.find(x => x.startsWith('--file=')) || '').slice(7);
if (file) { say('file ' + file); db.exec(readFileSync(file, 'utf8')); console.log('ok'); }
else if (cmd) {
  if (/^\s*select/i.test(cmd)) {
    if (process.env.FAKE_QUERY_FAIL) { say('queryfail'); console.error('auth expired'); process.exit(1); }
    if (process.env.FAKE_QUERY_BAD) { console.log('<html>oops</html>'); process.exit(0); } let rows = []; try { rows = db.prepare(cmd).all(); } catch { rows = []; } console.log(JSON.stringify([{ results: rows, success: true }])); }
  else { say('cmd ' + cmd.slice(0, 40)); db.exec(cmd); console.log('ok'); }
}
