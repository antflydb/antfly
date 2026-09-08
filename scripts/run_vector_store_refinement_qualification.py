"""Build and qualify source-store refinements, retaining all results in the worktree."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

from vector_store_experiment_settings import NEXT, configure

worktree = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--root', type=Path, default=worktree / '.benchmark-results' / ('vector-refinements-qualified-' + time.strftime('%Y%m%d-%H%M%S')))
parser.add_argument('--binary', type=Path, help='Use an existing executable instead of building.')
parser.add_argument('--vdbbench-root', type=Path)
parser.add_argument('--vdbbench-python', type=Path)
parser.add_argument('--jobs', type=int, default=2)
parser.add_argument('--pairs', type=int, default=2)
parser.add_argument('--port', type=int, default=18118)
parser.add_argument('--suite', choices=['next', 'first'], default='next')
parser.add_argument('--include-1m', action='store_true')
args = parser.parse_args()
if args.pairs < 2:
    parser.error('at least two pairs are required for alternating order')
args.root = args.root.absolute()
# The frozen shell harness obtains git context from its parent directory.
if not args.root.is_relative_to(worktree):
    parser.error('--root must be inside the worktree')
args.root.mkdir(parents=True, exist_ok=False)
common_git_dir = Path(subprocess.check_output(['git', '-C', str(worktree), 'rev-parse', '--path-format=absolute', '--git-common-dir'], text=True).strip())
vdbbench_root = (args.vdbbench_root or common_git_dir.parent.parent / 'VectorDBBench').absolute()
python = str((args.vdbbench_python or vdbbench_root / '.venv/bin/python').absolute())
prefix = args.root / 'release'
binary = prefix / 'bin/antfly'
if args.binary:
    binary.parent.mkdir(parents=True)
    shutil.copy2(args.binary.resolve(strict=True), binary)
receipt = args.root / 'phases.json'
phases = []

def run(name, command, cwd=worktree, environment=None):
    entry = {'phase': name, 'command': [str(x) for x in command], 'started_at': time.time()}
    phases.append(entry)
    receipt.write_text(json.dumps(phases, indent=2) + '\n')
    print('started', name, flush=True)
    with (args.root / (name + '.log')).open('w') as log:
        result = subprocess.run(command, cwd=cwd, env=environment, stdout=log, stderr=subprocess.STDOUT)
    entry.update(exit_code=result.returncode, finished_at=time.time())
    receipt.write_text(json.dumps(phases, indent=2) + '\n')
    print('finished', name, result.returncode, flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)

if not args.binary:
    run('build', ['zig', 'build', 'antfly', '-Doptimize=ReleaseFast', '--prefix', str(prefix), '-j' + str(args.jobs)], worktree / 'zig')
digest = hashlib.sha256()
with binary.open('rb') as stream:
    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
        digest.update(chunk)
(args.root / 'binary.json').write_text(json.dumps({'path': str(binary), 'sha256': digest.hexdigest()}, indent=2) + '\n')
receipt.write_text(json.dumps(phases, indent=2) + '\n')

# Freeze measurement code so concurrent edits in the shared worktree cannot
# change the treatment or invalidate a later arm. The nested location retains
# the git context used by the shell harness; all runtime paths are explicit.
snapshot = args.root / 'harness/scripts'
snapshot.mkdir(parents=True, exist_ok=False)
for source in (worktree / 'scripts').iterdir():
    if source.is_file() and source.suffix in {'.py', '.sh'}:
        shutil.copy2(source, snapshot / source.name)

environment = os.environ.copy()
environment.update(ANTFLY_BIN=str(binary),
    ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES='65536',
    ANTFLY_SOURCE_VECTOR_TARGET_SEGMENT_BYTES='8388608',
    ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES='8388608',
    ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS='64')
if args.suite == 'next':
    environment = configure(environment, 'next_combined', True)
run('api', [str(worktree / 'zig/e2e/antfly/.venv/bin/pytest'), '-q', 'e2e/antfly/test_vector_store.py'], worktree / 'zig', environment)
for feature in (NEXT if args.suite == 'next' else ['gc', 'combined']):
    run(feature, [python, str(snapshot / 'run_vector_store_enrichment_ab.py'),
        str(args.root / feature), '--binary', str(binary),
        '--refinement', feature, '--pairs', str(args.pairs), '--port', str(args.port)])
run('scale', [python, str(snapshot / 'run_vector_store_ab.py'),
    str(args.root / 'scale'), '--binary', str(binary),
    '--refinement', 'next_combined' if args.suite == 'next' else 'combined', '--pairs', str(args.pairs),
    *(['--include-1m'] if args.include_1m else []),
    '--port', str(args.port), '--health-port', str(args.port + 1),
    '--vdbbench-root', str(vdbbench_root),
    '--vdbbench-python', python])
run('summarize', [python, str(snapshot / 'summarize_vector_store_refinement_scale.py'), str(args.root / 'scale')])
