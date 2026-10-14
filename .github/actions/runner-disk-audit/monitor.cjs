'use strict';

const fs = require('node:fs');
const path = require('node:path');

function readCounters(file, unit = 1) {
  try {
    return Object.fromEntries(fs.readFileSync(file, 'utf8').trim().split('\n').flatMap(line => {
      const [key, value] = line.trim().split(/\s+/);
      return /^\d+$/.test(value) ? [[key.replace(/:$/, ''), Number(value) * unit]] : [];
    }));
  } catch {
    return {};
  }
}

function sampleMemory(cgroup = '/sys/fs/cgroup', meminfo = '/proc/meminfo') {
  try {
    const value = fs.readFileSync(path.join(cgroup, 'memory.current'), 'utf8').trim();
    if (!/^\d+$/.test(value)) return null;
    const current = Number(value);
    const readValue = name => {
      try {
        const value = fs.readFileSync(path.join(cgroup, name), 'utf8').trim();
        return /^\d+$/.test(value) ? Number(value) : value;
      } catch {
        return null;
      }
    };
    const counters = readCounters(path.join(cgroup, 'memory.stat'));
    const node = readCounters(meminfo, 1024);
    return {
      current,
      peak: readValue('memory.peak'),
      limit: readValue('memory.max'),
      events: readCounters(path.join(cgroup, 'memory.events')),
      stat: Object.fromEntries(['anon', 'file', 'active_file', 'inactive_file', 'kernel', 'slab', 'sock'].filter(key => key in counters).map(key => [key, counters[key]])),
      nodeAvailable: node.MemAvailable ?? null,
    };
  } catch {
    // Diagnostics must not fail jobs on macOS or restricted cgroup mounts.
    return null;
  }
}

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
  const initialMemory = sampleMemory();
  const memory = initialMemory ? {start: initialMemory, peakCurrent: initialMemory.current, peakSample: initialMemory, minimumNodeAvailable: initialMemory.nodeAvailable} : null;
  const publish = finishedAt => {
    const temporary = reportFile + '.next';
    fs.writeFileSync(temporary, JSON.stringify({startedAt, sampledAt: new Date().toISOString(), finishedAt, filesystems, memory}, null, 2));
    fs.renameSync(temporary, reportFile);
  };
  publish(null);
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
    const currentMemory = sampleMemory();
    if (memory && currentMemory) {
      if (currentMemory.current > memory.peakCurrent) {
        memory.peakCurrent = currentMemory.current;
        memory.peakSample = currentMemory;
      }
      if (currentMemory.nodeAvailable !== null) {
        memory.minimumNodeAvailable = memory.minimumNodeAvailable === null ? currentMemory.nodeAvailable : Math.min(memory.minimumNodeAvailable, currentMemory.nodeAvailable);
      }
      memory.finish = currentMemory;
    }
    if (!fs.existsSync(stopFile)) {
      publish(null);
      return;
    }
    clearInterval(timer);
    publish(new Date().toISOString());
  }, intervalMs);
  return timer;
}

if (require.main === module) {
  const [stopFile, reportFile, interval, ...paths] = process.argv.slice(2);
  monitor(paths, stopFile, reportFile, Number(interval));
}

module.exports = {monitor, sample, sampleMemory};
