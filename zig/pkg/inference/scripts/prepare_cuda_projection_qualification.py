# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Prepare an original-vs-current launch qualification in a disposable worktree.

  python3 zig/pkg/inference/scripts/prepare_cuda_projection_qualification.py \
    --baseline <revision> --output zig/pkg/inference/src/ops/cuda/kernels.zig
  zig build --build-file zig/pkg/inference/build.zig test-bin -Dcuda=true \
    -Doptimize=ReleaseFast '-Dtest-filter=CUDA projection'

Run the resulting test binary normally for wire/oracle/graph checks. Set
ANTFLY_CUDA_PROJECTION_BENCH=1 and filter to "CUDA projection original adapter
qualification" for the opt-in AA/AB/BA host-submission assay. Preserve every
result; an unchanged failed assay must not be automatically retried.
Set ANTFLY_CUDA_PROJECTION_GPU_TIME=1 together with the benchmark flag to
measure CUDA-event stream duration instead of host submission. Event pairs are
created once; the recorded interval includes any host-enqueue starvation gaps.
ANTFLY_CUDA_PROJECTION_GRAPH_TIME=1 instead captures each 32-call arm once and
measures graph replay with CUDA events, separating within-batch enqueue gaps.
Each original/candidate captured batch is checked against the device oracle.

The output contains test-only copies of baseline adapters. Shared dependencies
and referenced scalar constants must match, preventing accidental comparison
against a baseline partly rewritten by the candidate. Never commit the output.
"""
import argparse
from pathlib import Path
import re
import subprocess

SOURCE = 'zig/pkg/inference/src/ops/cuda/kernels.zig'
HARNESS = 'zig/pkg/inference/src/ops/cuda/testing/projection_launch.zig'
MARKER = 'test "CUDA projection original adapter qualification"'


def function(text, name):
    match = re.search(r'(?m)^([ \t]*)(?:pub )?(?:inline )?fn ' + re.escape(name) + r'\(', text)
    if not match:
        raise ValueError('Missing function: ' + name)
    end = text.index('\n' + match[1] + '}', match.end()) + len(match[1]) + 2
    return text[match.start():end]


def prepare(root, baseline):
    before = subprocess.check_output(['git', 'show', baseline + ':' + SOURCE], cwd=root, text=True)
    current = (root / SOURCE).read_text()
    if MARKER in current:
        raise ValueError('Qualification already prepared; start from a clean disposable worktree.')
    dependencies = ('projectionArguments', 'quantQkvArguments', 'quantArgmaxArguments',
                    'checkedTensorElements', 'toU32', 'checkBytes', 'checkRawBytes',
                    'launchBlocks', 'launchRaw')
    for name in dependencies:
        if function(before, name) != function(current, name):
            raise ValueError('Baseline dependency changed: ' + name)
    shared_paths = ('zig/pkg/inference/src/ops/cuda/launch_abi.zig',
                    'zig/pkg/inference/src/ops/cuda/buffer.zig',
                    'zig/pkg/inference/src/ops/cuda/context.zig',
                    'zig/pkg/inference/src/ops/cuda/driver.zig',
                    'zig/pkg/inference/src/ops/cuda/kernels',
                    'zig/pkg/inference/src/ops/cuda/artifacts')
    if subprocess.run(['git', 'diff', '--quiet', baseline, '--', *shared_paths], cwd=root).returncode:
        raise ValueError('Baseline native ABI, driver, buffer, device source or images changed.')
    names = re.findall(r'\.adapter = "(\w+)"', (root / HARNESS).read_text())
    if not names or len(names) != len(set(names)):
        raise ValueError('Invalid native-contract adapter list.')
    copies = []
    for name in names:
        old, new = function(before, name), function(current, name)
        if old.split(') driver_mod.Error!void {')[0] != new.split(') driver_mod.Error!void {')[0]:
            raise ValueError('Public signature changed: ' + name)
        for identifier in set(re.findall(r'\b\w+\b', old)):
            pattern = r'(?m)^const ' + re.escape(identifier) + r'\b[^\n]+;'
            old_constant = re.search(pattern, before)
            if old_constant:
                new_constant = re.search(pattern, current)
                if not new_constant or old_constant[0] != new_constant[0]:
                    raise ValueError('Baseline constant changed: ' + identifier)
        copies.append(old.replace('projectionArguments(', 'KernelModule.projectionArguments('))
    return current + '\n' + MARKER + ''' {
    const Before = struct {
''' + '\n'.join(copies) + '''
    };
    try @import("testing/projection_launch.zig").run(KernelModule, Before);
    if (comptime @import("builtin").os.tag == .linux and build_options.enable_cuda) {
        try @import("testing/projection_launch.zig").device(KernelModule, Before);
        try @import("testing/projection_launch.zig").benchmark(KernelModule, Before);
    }
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[4]
    prepared = prepare(root, args.baseline)
    args.output.write_text(prepared)
    print('Prepared original adapter qualification:', args.output)


if __name__ == '__main__':
    main()
