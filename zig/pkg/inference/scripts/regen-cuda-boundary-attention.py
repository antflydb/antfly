#!/usr/bin/env python3
"""Build the pinned boundary-attention module without a Torch runtime dependency.

Requires CUDA 13.2 and build-only PyTorch/CUTLASS/curand include trees. The
checked-in dependency lock verifies every transitive header before compilation.
Use --refresh-lock only when intentionally updating dependencies and rerun CUDA
forward/backward parity, memory, resume and sustained-training qualification.
"""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import tempfile

ARTIFACTS = Path(__file__).resolve().parents[1] / 'src/ops/cuda/artifacts'
LOCK = ARTIFACTS / 'gliner25_boundary_attention.lock.json'
OPTIONS = ['-std=c++17', '-O3', '-DNDEBUG', '--expt-relaxed-constexpr', '--expt-extended-lambda']
DIRECTIONS = ('forward', 'backward')
COMPILER = 'release 13.2, V13.2.78'


def digest(data):
    return hashlib.sha256(data).hexdigest()



@contextmanager
def build_workspace():
    # NVCC includes the physical translation-unit path in private symbols,
    # independently of --frandom-seed. Use the same path across checkouts and
    # serialize writers. The private directory is rejected if another user
    # owns it, if it is a symlink, or if other users can access its contents.
    work = Path('/tmp/antfly-gliner25-boundary-attention-build-v1')
    work.mkdir(mode=0o700, exist_ok=True)
    info = work.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise RuntimeError('unsafe or foreign CUDA artifact workspace: '+str(work))
    with (work/'.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield work


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--write', action='store_true')
    mode.add_argument('--check', action='store_true')
    parser.add_argument('--refresh-lock', action='store_true')
    parser.add_argument('--output-dir', type=Path, default=ARTIFACTS)
    parser.add_argument('--cuda', type=Path, default=Path('/usr/local/cuda'))
    parser.add_argument('--torch-include', type=Path, required=True)
    parser.add_argument('--cutlass-include', type=Path, required=True)
    parser.add_argument('--curand-include', type=Path, required=True)
    args = parser.parse_args()
    roots = {name: path.resolve() for name,path in {
        'cuda': args.cuda, 'torch': args.torch_include,
        'cutlass': args.cutlass_include, 'curand': args.curand_include}.items()}
    nvcc = roots['cuda'] / 'bin/nvcc'
    version = subprocess.check_output([nvcc,'--version'], text=True)
    if COMPILER not in version:
        raise RuntimeError('expected pinned CUDA compiler: '+COMPILER+'; got '+version)
    with build_workspace() as work:
        patch = work / 'device-include/ATen/cuda/detail/PhiloxCudaStateRaw.cuh'
        patch.parent.mkdir(parents=True, exist_ok=True)
        original = (roots['torch']/'ATen/cuda/detail/PhiloxCudaStateRaw.cuh').read_text()
        patch.write_text(original.replace('  PhiloxCudaState(', '  __host__ __device__ PhiloxCudaState('))
        includes = ['-I'+str(p) for p in (work/'device-include', roots['cutlass'], roots['torch'], roots['curand'])]
        dependencies = {}
        sources = {}
        for direction in DIRECTIONS:
            name = 'gliner25_boundary_attention_'+direction+'.cu'
            source = (ARTIFACTS/name).read_bytes()
            sources[name] = digest(source)
            (work/name).write_bytes(source)
            output = subprocess.check_output([nvcc, '-M', *OPTIONS, *includes, name], cwd=work, text=True)
            for token in shlex.split(output.replace('\\\n',' ').split(':',1)[1]):
                path = (work/token).resolve()
                if path.is_relative_to(work):
                    continue
                for label,root in roots.items():
                    if path.is_relative_to(root):
                        key = label+'/'+str(path.relative_to(root)); break
                else:
                    # Host system headers do not supply generated device code;
                    # record their identity too rather than silently ignore them.
                    key = 'system/'+str(path).lstrip('/')
                dependencies[key] = digest(path.read_bytes())
        expected = {'compiler': COMPILER, 'headers': dict(sorted(dependencies.items())),
                    'philox_patch_sha256': digest(patch.read_bytes()),
                    'cutlass_commit': 'e51efbfe18fe4f4cbb66ab814c55bf4aa0185491',
                    'torch_git': '5811a8d7da873dd699ff6687092c225caffcf1bb'}
        encoded_lock = (json.dumps(expected,indent=2,sort_keys=True)+'\n').encode()
        if args.refresh_lock:
            LOCK.write_bytes(encoded_lock)
        if not LOCK.exists() or LOCK.read_bytes() != encoded_lock:
            raise RuntimeError('build dependency lock differs; inspect dependencies before --refresh-lock')
        outputs = {}
        for direction in DIRECTIONS:
            stem = 'gliner25_boundary_attention_'+direction
            for suffix,option,arch in (('cubin','-cubin','sm_89'),('sm80.cubin','-cubin','sm_80')):
                name = stem+'.'+suffix
                subprocess.run([nvcc,option,'-arch='+arch,'--frandom-seed='+stem,*OPTIONS,*includes,stem+'.cu','-o',str(work/name)],cwd=work,check=True)
                outputs[name] = (work/name).read_bytes()
        metadata = {'profile': 'gliner25_boundary_attention_cutlass_d32_v1', 'compiler': COMPILER,
                    'options': OPTIONS, 'random_seed': 'translation-unit basename without extension', 'source_directory': str(work), 'source_sha256': sources,
                    'dependency_lock_sha256': digest(encoded_lock),
                    'artifacts': {name:digest(data) for name,data in outputs.items()}}
        outputs['gliner25_boundary_attention.json'] = (json.dumps(metadata,indent=2,sort_keys=True)+'\n').encode()
        for name,data in outputs.items():
            destination = args.output_dir/name
            if args.write:
                staged = None
                try:
                    with tempfile.NamedTemporaryFile(dir=args.output_dir, prefix='.boundary-attention-', delete=False) as f:
                        staged = Path(f.name)
                        f.write(data)
                    staged.chmod(0o644)
                    os.replace(staged,destination)
                finally:
                    if staged is not None:
                        staged.unlink(missing_ok=True)
            elif not destination.exists() or destination.read_bytes()!=data:
                raise RuntimeError('stale boundary-attention artifact: '+name)
        print('Boundary attention artifacts '+('written' if args.write else 'match'))


if __name__ == '__main__':
    main()
