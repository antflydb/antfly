'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawn} = require('node:child_process');

test('sampler retains a filesystem peak until the post hook stops it', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'disk-audit-test-'));
  const stop = path.join(root, 'stop');
  const report = path.join(root, 'report.json');
  try {
    const child = spawn(process.execPath, [path.join(__dirname, 'monitor.cjs'), stop, report, '20', root]);
    await new Promise(resolve => setTimeout(resolve, 100));
    fs.writeFileSync(stop, '');
    const status = await new Promise(resolve => child.on('exit', resolve));
    assert.equal(status, 0);
    const value = JSON.parse(fs.readFileSync(report, 'utf8'));
    assert.ok(value.filesystems[root].peakUsed >= value.filesystems[root].start.used);
    assert.ok(value.filesystems[root].minimumAvailable <= value.filesystems[root].start.available);
    assert.ok(value.finishedAt);
  } finally {
    fs.rmSync(root, {recursive: true, force: true});
  }
});
