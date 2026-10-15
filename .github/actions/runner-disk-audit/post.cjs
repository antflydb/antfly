'use strict';

const fs = require('node:fs');
const path = require('node:path');

const root = process.env.STATE_root;
if (!root) process.exit(0);
const reportFile = path.join(root, 'report.json');
fs.writeFileSync(path.join(root, 'stop'), '');
const pause = new Int32Array(new SharedArrayBuffer(4));
for (let attempt = 0; attempt < 70; attempt++) {
  if (fs.existsSync(reportFile) && JSON.parse(fs.readFileSync(reportFile, 'utf8')).finishedAt) break;
  Atomics.wait(pause, 0, 0, 100);
}
if (!fs.existsSync(reportFile)) {
  console.log('Disk sampler did not finish; no peak measurement available');
  process.exit(0);
}
const report = JSON.parse(fs.readFileSync(reportFile, 'utf8'));
const lines = ['### ARC runner disk use', '', '| Path | Capacity GiB | Start GiB | Peak GiB | Minimum free GiB |', '| --- | ---: | ---: | ---: | ---: |'];
const gib = value => (value / 1024 ** 3).toFixed(2);
for (const [name, record] of Object.entries(report.filesystems)) {
  lines.push(`| ${name} | ${gib(record.start.capacity)} | ${gib(record.start.used)} | ${gib(record.peakUsed)} | ${gib(record.minimumAvailable)} |`);
}
if (report.memory) {
  const record = report.memory;
  const latest = record.finish || record.start;
  lines.push('', '### ARC runner memory use', '',
    `Observed cgroup peak: ${gib(record.peakCurrent)} GiB; visible limit: ${typeof latest.limit === 'number' ? gib(latest.limit) + ' GiB' : latest.limit ?? 'unavailable'}; minimum node MemAvailable: ${record.minimumNodeAvailable === null ? 'unavailable' : gib(record.minimumNodeAvailable) + ' GiB'}.`, '',
    'Memory counters (bytes) and pressure events:', '', '```json', JSON.stringify(record, null, 2), '```');
}
console.log(lines.join('\n'));
if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, lines.join('\n') + '\n');
