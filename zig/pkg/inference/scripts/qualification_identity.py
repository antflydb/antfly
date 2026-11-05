# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Content identities for immutable loaded model directories in controlled runs."""
import hashlib
import os
from pathlib import Path
import stat

import qualify_read_pool_40g as q

WEIGHTS = {'.gguf', '.safetensors', '.onnx', '.bin', '.pt', '.pth'}
METADATA = {'model_manifest.json', 'config.json', 'antfly_inference_bundle.json'}


def sha256_file(path):
    """Never trust a cached checksum; reject a file changing while it is read."""
    digest = hashlib.sha256()
    with path.open('rb') as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise q.EvidenceUnavailable(f'model artifact is not a regular file: {path}')
        for block in iter(lambda: source.read(8 << 20), b''):
            digest.update(block)
        after = os.fstat(source.fileno())
    def signature(value):
        return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
    if signature(before) != signature(after) or signature(after) != signature(path.stat()):
        raise q.EvidenceUnavailable(f'model artifact changed during hashing: {path}')
    return {'size': after.st_size, 'sha256': digest.hexdigest()}


def model_files(directory):
    files = []
    # Follow model-cache symlinks, including shard directories. Detect cycles;
    # do not silently omit a linked weight. Operational checksum/download caches
    # are excluded; manifests, configs, tokenizers and all weights are hashed.
    visited = set()
    def walk_error(error):
        raise q.EvidenceUnavailable(f'cannot enumerate model artifacts: {error}')
    for root, dirs, names in os.walk(directory, followlinks=True, onerror=walk_error):
        root = Path(root)
        identity = root.stat()
        key = identity.st_dev, identity.st_ino
        if key in visited:
            raise q.EvidenceUnavailable(f'cyclic or aliased model directory: {root}')
        visited.add(key)
        dirs.sort()
        for name in sorted(names):
            if name.endswith('.antfly-sha256-cache') or name == '.antfly-download-complete.json':
                continue
            path = root / name
            files.append({'path': path.relative_to(directory).as_posix(), **sha256_file(path)})
    names = {item['path'] for item in files}
    if not names.intersection(METADATA) or not any(Path(name).suffix in WEIGHTS for name in names):
        raise q.EvidenceUnavailable(f'model lacks manifest/configuration or weight artifacts: {directory}')
    return sorted(files, key=lambda item: item['path'])


def model_artifacts(configuration, url, working_directory):
    try:
        return _model_artifacts(configuration, url, working_directory)
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise q.EvidenceUnavailable(f'model artifact identity unavailable: {error}') from error


def _model_artifacts(configuration, url, working_directory):
    root = Path(configuration['inference']['models_dir'])
    if not root.is_absolute():
        root = working_directory / root
    root = root.resolve(strict=True)
    status, inventory, _ = q.http(url, '/models/instances')
    if status != 200 or not inventory.get('data'):
        raise q.EvidenceUnavailable('loaded model inventory unavailable for artifact identity')
    models = []
    for instance in inventory['data']:
        model = instance['model']
        relative = Path(model)
        if relative.is_absolute() or '..' in relative.parts or instance['state'] != 'loaded':
            raise q.EvidenceUnavailable(f'unresolved loaded model identity: {model}')
        directory = root / relative
        models.append({'model': model, 'backend': instance['backend'],
                       'tasks': sorted(instance['tasks']), 'files': model_files(directory)})
    status, after, _ = q.http(url, '/models/instances')
    if status != 200 or sorted(item['id'] for item in after['data']) != sorted(item['id'] for item in inventory['data']):
        raise q.EvidenceUnavailable('loaded models changed during artifact hashing')
    return {'version': 1, 'models': sorted(models, key=lambda item: item['model'])}


def require_matching_artifacts(left, right):
    for identity in (left, right):
        artifacts = identity.get('model_artifacts')
        if not artifacts or artifacts.get('version') != 1 or not artifacts.get('models'):
            raise q.EvidenceUnavailable('controlled evidence lacks model artifact hashes; rerun qualification')
        for model in artifacts['models']:
            files = model.get('files', [])
            names = {item['path'] for item in files}
            if not names.intersection(METADATA) or not any(Path(name).suffix in WEIGHTS for name in names):
                raise q.EvidenceUnavailable('controlled model evidence lacks metadata or weights')
            for item in files:
                if item.get('size', -1) < 0 or len(item.get('sha256', '')) != 64 or any(c not in '0123456789abcdef' for c in item['sha256']):
                    raise q.EvidenceUnavailable('invalid model artifact hash evidence')
    if left['model_artifacts'] != right['model_artifacts']:
        raise q.EvidenceUnavailable('controlled model artifacts differ')


def verify_artifacts(identity, args):
    current = {'model_artifacts': model_artifacts(identity['configuration'], args.url, Path(identity['working_directory']))}
    require_matching_artifacts(identity, current)
