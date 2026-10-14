'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawn, spawnSync} = require('node:child_process');
const {sampleMemory} = require('./monitor.cjs');

test('memory diagnostics preserve an unlimited visible limit, kernel peak and pressure counters', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'memory-audit-test-'));
  try {
    fs.writeFileSync(path.join(root, 'memory.current'), '4096\n');
    fs.writeFileSync(path.join(root, 'memory.peak'), '16384\n');
    fs.writeFileSync(path.join(root, 'memory.max'), 'max\n');
    fs.writeFileSync(path.join(root, 'memory.events'), 'max 7\noom 2\noom_kill 1\n');
    fs.writeFileSync(path.join(root, 'memory.stat'), 'anon 1024\nfile 3072\nactive_file 2048\ninactive_file 1024\nunrelated 123\n');
    const meminfo = path.join(root, 'meminfo');
    fs.writeFileSync(meminfo, 'MemAvailable: 256 kB\n');
    assert.deepEqual(sampleMemory(root, meminfo), {
      current: 4096, peak: 16384, limit: 'max',
      events: {max: 7, oom: 2, oom_kill: 1},
      stat: {anon: 1024, file: 3072, active_file: 2048, inactive_file: 1024},
      nodeAvailable: 256 * 1024,
    });
    fs.writeFileSync(path.join(root, 'memory.max'), '8192\n');
    assert.equal(sampleMemory(root, meminfo).limit, 8192);
    fs.unlinkSync(path.join(root, 'memory.peak'));
    assert.equal(sampleMemory(root, '/missing-meminfo').peak, null);
    assert.equal(sampleMemory(root, '/missing-meminfo').nodeAvailable, null);
    fs.unlinkSync(path.join(root, 'memory.current'));
    assert.equal(sampleMemory(root, meminfo), null);
  } finally {
    fs.rmSync(root, {recursive: true, force: true});
  }
});

test('sampler checkpoints before shutdown and post hook waits for the final report', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'disk-checkpoint-test-'));
  const stop = path.join(root, 'stop');
  const report = path.join(root, 'report.json');
  const child = spawn(process.execPath, [path.join(__dirname, 'monitor.cjs'), stop, report, '20', root]);
  try {
    const deadline = Date.now() + 5000;
    while (!fs.existsSync(report) && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
    assert.equal(JSON.parse(fs.readFileSync(report, 'utf8')).finishedAt, null);
    const post = spawn(process.execPath, [path.join(__dirname, 'post.cjs')], {env: {...process.env, STATE_root: root}, stdio: ['ignore', 'pipe', 'pipe']});
    let output = '';
    post.stdout.on('data', bytes => {output += bytes;});
    post.stderr.on('data', bytes => {output += bytes;});
    assert.equal(await new Promise(resolve => post.on('exit', resolve)), 0, output);
    assert.ok(JSON.parse(fs.readFileSync(report, 'utf8')).finishedAt);
    assert.match(output, /ARC runner disk use/);
  } finally {
    child.kill();
    fs.rmSync(root, {recursive: true, force: true});
  }
});

test('post summary distinguishes an unlimited cgroup from measured node headroom', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'memory-summary-test-'));
  try {
    const memory = {current: 1024 ** 3, peak: 2 * 1024 ** 3, limit: 'max', events: {oom_kill: 1}, stat: {anon: 1024 ** 3}, nodeAvailable: 0};
    fs.writeFileSync(path.join(root, 'report.json'), JSON.stringify({finishedAt: new Date().toISOString(), filesystems: {}, memory: {start: memory, finish: memory, peakCurrent: memory.current, minimumNodeAvailable: 0}}));
    const result = spawnSync(process.execPath, [path.join(__dirname, 'post.cjs')], {env: {...process.env, STATE_root: root}, encoding: 'utf8'});
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /visible limit: max; minimum node MemAvailable: 0.00 GiB/);
    assert.match(result.stdout, /"oom_kill": 1/);
    assert.match(result.stdout, /"peak": 2147483648/);
  } finally {
    fs.rmSync(root, {recursive: true, force: true});
  }
});

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
