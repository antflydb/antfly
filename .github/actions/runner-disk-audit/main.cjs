'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawn} = require('node:child_process');

const root = fs.mkdtempSync(path.join(process.env.RUNNER_TEMP || os.tmpdir(), 'disk-audit-'));
const stopFile = path.join(root, 'stop');
const reportFile = path.join(root, 'report.json');
const paths = ['/mnt/cache', process.env.GITHUB_WORKSPACE, process.env.RUNNER_TEMP].filter(Boolean);
const child = spawn(process.execPath, [path.join(__dirname, 'monitor.cjs'), stopFile, reportFile, '5000', ...paths], {
  detached: true,
  stdio: 'ignore',
});
child.unref();
fs.appendFileSync(process.env.GITHUB_STATE, `root=${root}\n`);
console.log(`Sampling filesystem use for ${paths.join(', ')} (PID ${child.pid})`);
