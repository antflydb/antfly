'use strict';

const fs = require('node:fs');

function sample(path) {
  const stat = fs.statfsSync(path);
  return {
    capacity: stat.blocks * stat.bsize,
    used: (stat.blocks - stat.bfree) * stat.bsize,
    available: stat.bavail * stat.bsize,
  };
}

function monitor(paths, stopFile, reportFile, intervalMs = 5000) {
  const filesystems = {};
  for (const path of paths) {
    if (!fs.existsSync(path)) continue;
    const start = sample(path);
    filesystems[path] = {start, peakUsed: start.used, minimumAvailable: start.available};
  }
  const startedAt = new Date().toISOString();
  const timer = setInterval(() => {
    for (const [path, record] of Object.entries(filesystems)) {
      try {
        const current = sample(path);
        record.peakUsed = Math.max(record.peakUsed, current.used);
        record.minimumAvailable = Math.min(record.minimumAvailable, current.available);
        record.finish = current;
      } catch (error) {
        record.error = String(error);
      }
    }
    if (!fs.existsSync(stopFile)) return;
    clearInterval(timer);
    fs.writeFileSync(reportFile, JSON.stringify({startedAt, finishedAt: new Date().toISOString(), filesystems}, null, 2));
  }, intervalMs);
  return timer;
}

if (require.main === module) {
  const [stopFile, reportFile, interval, ...paths] = process.argv.slice(2);
  monitor(paths, stopFile, reportFile, Number(interval));
}

module.exports = {monitor, sample};
