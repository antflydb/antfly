# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Account for net source changes, separating tests, generated code and images.

Metal shader bytes count the maintained Objective-C source representation.
The decoded Metal source can have different line/byte counts due to escapes.
"""
from pathlib import Path
import argparse
import subprocess
import re
import json
import hashlib


def managed(text):
    authored = []
    generated = []
    inside = False
    for line in text.splitlines(True):
        if re.match('\\s*// quant-kernel-codegen:begin', line):
            inside = True
        (generated if inside else authored).append(line)
        if re.match('\\s*// quant-kernel-codegen:end', line):
            inside = False
    return (''.join(authored), ''.join(generated))


skip=re.compile(r'(?m:^[ \t]*\\\\[^\n]*)|//[^\n]*|/\*|"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'')
header=re.compile(r'(?m)^[ \t]*test\s*(?:"(?:[^"\\]|\\.)*"\s*)?\{')


def split_zig_tests(text):
    masked = list(text)
    cursor = 0
    while match := skip.search(text, cursor):
        end = match.end()
        if match[0] == '/*':
            depth = 1
            while depth:
                opening = text.find('/*', end)
                closing = text.find('*/', end)
                if closing < 0:
                    raise ValueError('unclosed Zig comment')
                if 0 <= opening < closing:
                    depth += 1
                    end = opening + 2
                else:
                    depth -= 1
                    end = closing + 2
        for index in range(match.start(), end):
            if masked[index] != '\n':
                masked[index] = ' '
        cursor = end
    masked = ''.join(masked)
    spans = []
    for m in header.finditer(text):
        start = m.end() - 1
        if masked[start] != '{':
            continue
        depth = 0
        for end in range(start, len(masked)):
            depth += (masked[end] == '{') - (masked[end] == '}')
            if depth == 0:
                break
        else:
            raise ValueError('unclosed test')
        spans.append((m.start(), end + 2 if text[end + 1:end + 2] == '\n' else end + 1))
    pure = []
    only = []
    start = 0
    for (a, b) in spans:
        pure.append(text[start:a])
        only.append(text[a:b])
        start = b
    pure.append(text[start:])
    return (''.join(pure), ''.join(only), len(spans))


def shader(text):
    start = text.index('static NSString *termite_metal_shader_source(void) {')
    end = text.index('\n}', start) + 2
    spans = [(start, end)]
    # Native source-fragment owners expand inside the shader, while their C
    # definitions live outside the NSString expression. Charge the complete
    # maintained owner and its contract comments to device source exactly once.
    lines = text.splitlines(True)
    offsets = [0]
    for line in lines:
        offsets.append(offsets[-1] + len(line))
    for index, line in enumerate(lines):
        if not line.startswith('#define TERMITE_MSL_'):
            continue
        first = index
        while first > 0 and lines[first - 1].startswith('//'):
            first -= 1
        last = index + 1
        while lines[last - 1].rstrip().endswith('\\'):
            if last == len(lines):
                raise ValueError('unclosed Metal source owner')
            last += 1
        spans.append((offsets[first], offsets[last]))
    device, host, cursor = [], [], 0
    for first, last in sorted(spans):
        if first < cursor:
            raise ValueError('overlapping Metal source owners')
        host.append(text[cursor:first])
        device.append(text[first:last])
        cursor = last
    host.append(text[cursor:])
    return (''.join(device), ''.join(host))


def account(root, baseline):
    root = Path(root).resolve()
    baseline = subprocess.check_output(['git', 'rev-parse', '--verify', baseline + '^{commit}'], cwd=root, text=True).strip()
    paths = set(subprocess.check_output(['git', 'diff', '--name-only', baseline], cwd=root, text=True).splitlines())
    paths.update(subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'], cwd=root, text=True).splitlines())

    def old(path):
        return subprocess.run(['git', 'show', baseline + ':' + path], cwd=root, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout
    groups = {}
    records = []

    def add(group, a, b):
        values = {'lines_removed': len(a.splitlines()) - len(b.splitlines()), 'bytes_removed': len(a.encode()) - len(b.encode())}
        total = groups.setdefault(group, {'lines_removed': 0, 'bytes_removed': 0})
        for key in total:
            total[key] += values[key]
        return dict(group=group, **values)
    for name in sorted(paths):
        path = root / name
        before = old(name)
        after = path.read_bytes() if path.exists() else b''
        row = {'path': name, 'baseline_sha256': hashlib.sha256(before).hexdigest(), 'candidate_sha256': hashlib.sha256(after).hexdigest(), 'groups': []}
        if path.suffix in {'.fatbin', '.cubin', '.ptx'}:
            row['image_bytes_removed'] = len(before) - len(after)
            records.append(row)
            continue
        a = before.decode()
        b = after.decode()
        if name.startswith('docs/'):
            row['groups'].append(add('documentation', a, b))
        elif '/scripts/' in name or '/bench/' in name or '/tools/' in name or (name == 'zig/pkg/inference/build.zig'):
            row['groups'].append(add('tests_and_benchmark_tooling', a, b))
        elif '/generated/' in name:
            row['groups'].append(add('generated', a, b))
        else:
            (a, ag) = managed(a)
            (b, bg) = managed(b)
            row['groups'].append(add('generated', ag, bg))
            if path.suffix == '.zig':
                (a, at, ac) = split_zig_tests(a)
                (b, bt, bc) = split_zig_tests(b)
                row['test_blocks'] = {'baseline': ac, 'candidate': bc}
                row['groups'].append(add('inline_tests', at, bt))
            if name.startswith('zig/lib/linalg/'):
                group = 'authored_cpu_linalg_alignment'
            elif name.endswith('/backends/metal_kernels.m'):
                (sa, a) = shader(a)
                (sb, b) = shader(b)
                row['groups'].append(add('authored_metal_shader', sa, sb))
                group = 'authored_metal_host_bindings'
            elif path.suffix in {'.cu', '.cuh'}:
                group = 'authored_cuda_device'
            elif name.endswith(('/cuda/kernels.zig', '/cuda/launch_abi.zig')):
                group = 'authored_cuda_launch'
            elif name.endswith(('/native_compute.zig', '/cuda/cuda_compute.zig')):
                group = 'authored_cpu_and_result_ownership'
            elif '/graph/quant_kernel_' in name:
                group = 'authored_generator_owners'
            else:
                group = 'authored_shared_contracts_and_build_identity'
            row['groups'].append(add(group, a, b))
        records.append(row)
    shipping = {k: v for (k, v) in groups.items() if k.startswith('authored_')}
    production = {key: sum((x[key] for x in shipping.values())) for key in ['lines_removed', 'bytes_removed']}
    receipt = {
        'baseline': baseline,
        'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'note': 'Net source accounting includes all replacement owners and metadata; '
                'Zig test blocks are separated lexically. '
                'All scripts/bench changes are tooling. Image bytes are reported separately.',
        'groups': groups,
        'authored_production': production,
        'images_bytes_removed': sum(x.get('image_bytes_removed', 0) for x in records),
        'files': records,
    }
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    receipt = account(args.root, args.revision)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({'groups': receipt['groups'], 'authored_production': receipt['authored_production']}, indent=2))

if __name__ == '__main__':
    main()
