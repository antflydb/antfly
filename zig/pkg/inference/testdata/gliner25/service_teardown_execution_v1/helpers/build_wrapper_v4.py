#!/usr/bin/env python3
"""Local receipt wrapper; uses the already pinned process-tree supervisor."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
from types import ModuleType

ROOT = Path('/Users/timkaye/Documents/af/antfly')
SUPERVISOR = ROOT / 'zig/pkg/inference/testdata/gliner25/published_inactive_classifier_cpu_v1/helpers/v2/supervision.py'
SUPERVISOR_SHA = 'a937237975be2ed879f62afd285494ccb51a7c1d7cc1dff7160621fb26b87332'

def pin(path):
    before = path.stat()
    h = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            h.update(chunk)
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise RuntimeError('input changed while hashing: ' + str(path))
    return dict(size_bytes=before.st_size, sha256=h.hexdigest())

def publish(path, value):
    with path.open('x') as out:
        json.dump(value, out, indent=2, sort_keys=True, allow_nan=False)
        out.write('\n')

def inventory():
    names = subprocess.check_output(['rg', '--files', 'zig'], cwd=ROOT, text=True).splitlines()
    suffixes = {'.zig', '.zon', '.metal', '.m', '.mm', '.c', '.h'}
    return {name: pin(ROOT / name) for name in sorted(names) if Path(name).suffix in suffixes}

def stop(number, frame):
    for requested in (signal.SIGTERM, signal.SIGINT):
        signal.signal(requested, signal.SIG_IGN)
    raise KeyboardInterrupt('supervisor received signal ' + str(number))

def main():
    output = Path(sys.argv[1])
    command = sys.argv[2:]
    if not output.is_absolute() or output.exists() or not command:
        raise RuntimeError('fresh absolute output and explicit command required')
    raw = SUPERVISOR.read_bytes()
    if hashlib.sha256(raw).hexdigest() != SUPERVISOR_SHA:
        raise RuntimeError('pinned supervisor changed')
    if shutil.disk_usage(ROOT).free < 1024**3:
        raise RuntimeError('less than 1 GiB available before serial build')
    output.mkdir(mode=0o700)
    supervisor = ModuleType('regional_validation_supervision')
    exec(compile(raw, str(SUPERVISOR), 'exec'), supervisor.__dict__)
    sources = inventory()
    publish(output / 'source_inventory.json', sources)
    inv = pin(output / 'source_inventory.json')
    os.chdir(ROOT / 'zig')
    os.environ['ZIG_GLOBAL_CACHE_DIR'] = '/private/tmp/antfly-gliner25-zig-cache'
    start = dict(scope='local_regional_training_and_service_checks/v1', qualification=False,
                 command=command, cwd=os.getcwd(), wrapper=pin(Path(__file__)), supervisor=pin(SUPERVISOR),
                 source_selection='rg --files zig; suffixes .zig .zon .metal .m .mm .c .h; this is not a complete dependency closure',
                 source_inventory=inv, source_count=len(sources),
                 environment={name: os.environ[name] for name in ('ZIG_GLOBAL_CACHE_DIR', 'ANTFLY_GLINER25_SMALL_MODEL_DIR') if name in os.environ})
    publish(output / 'start.json', start)
    class ObservedTree(supervisor.ProcessTree):
        def __init__(self, *args):
            super().__init__(*args)
            self.test_executables = []
            self.optional_identity_errors = []
            self.disk_guard = True
        def register(self, process, relation):
            super().register(process, relation)
            try:
                argv = process.cmdline()
                if argv and Path(argv[0]).name in ('test', 'train-gliner25', 'antfly-inference'):
                    cwd = process.cwd()
                    executable = (Path(cwd) / argv[0]).resolve()
                    if not executable.is_relative_to(ROOT / 'zig'):
                        return
                    entry = dict(pid=process.pid, create_time=process.create_time(), argv=argv, cwd=cwd,
                                 executable=dict(pin(executable), path=str(executable)))
                    if not any(x['pid'] == entry['pid'] and x['create_time'] == entry['create_time'] for x in self.test_executables):
                        self.test_executables.append(entry)
                        publish(output / ('observed-test-' + str(len(self.test_executables)) + '.json'), entry)
            except self.psutil.NoSuchProcess:
                pass
            except (self.psutil.AccessDenied, OSError) as error:
                # Optional argv/executable capture can fail with AccessDenied
                # or sysctl(KERN_PROCARGS2) OSError for transient processes. RSS, creation
                # identity and cleanup inspection remain strict in super().
                if len(self.optional_identity_errors) < 64:
                    self.optional_identity_errors.append(str(error))
        def sample(self):
            result = super().sample()
            if self.disk_guard and shutil.disk_usage(ROOT).free < 128 * 1024**2:
                raise RuntimeError('less than 128 MiB available during serial build')
            return result
        def cleanup_sample(self):
            self.disk_guard = False
            return super().cleanup_sample()
        def receipt(self):
            return dict(super().receipt(), observed_test_executables=self.test_executables,
                        optional_executable_identity_errors=self.optional_identity_errors)
    for number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(number, stop)
    result = supervisor.run(command, output / 'stdout.log', output / 'stderr.log',
                            timeout_seconds=1800, rss_limit_bytes=6 * 1024**3,
                            output_limit_bytes=8 * 1024**2, grace_seconds=5,
                            kill_seconds=5, worker_grace_seconds=2, tick=0.05,
                            tree_factory=ObservedTree)
    changed = [name for name, expected in sources.items() if not (ROOT / name).exists() or pin(ROOT / name) != expected]
    result.update(source_inventory_unchanged=not changed, changed_sources=changed,
                  stdout=pin(output / 'stdout.log'), stderr=pin(output / 'stderr.log'))
    publish(output / 'process.json', dict(start, process=result))
    print(json.dumps(dict(returncode=result['returncode'], failure=result['failure'], cleanup=result['cleanup'],
                          source_inventory_unchanged=not changed, peak_rss=result['peak_child_tree_rss_bytes'],
                          elapsed_seconds=result['elapsed_seconds'], receipt=str(output / 'process.json'))), flush=True)
    return 0 if result['failure'] is None and result['returncode'] == 0 and result['cleanup']['complete'] and not changed else 1

if __name__ == '__main__':
    raise SystemExit(main())
