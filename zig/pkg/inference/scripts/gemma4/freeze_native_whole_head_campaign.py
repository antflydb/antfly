# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Freeze an audited full build and all public performance prerequisites before gold exposure."""
import argparse
import fcntl
import hashlib
import os
from pathlib import Path

import qualify_native_whole_head_quality as campaign
import native_whole_head_campaign as contract


def freeze(repo, root, specification_ref, output):
    root, repo = Path(root).resolve(strict=True), Path(repo).resolve(strict=True)
    specification = contract.json_ref(root, specification_ref, 'campaign preparation specification')
    contract.require(specification.get('schema') == 'antfly.native-whole-head.freeze-input.v2' and
                     set(specification) == {'schema', 'build', 'configuration', 'configuration_audit',
                                            'input_index', 'models', 'performance'}, 'invalid freeze preparation')
    output = Path(output).absolute().resolve(strict=False)
    contract.require(output.is_relative_to(root) and not output.exists() and not output.is_symlink(),
                     'unsafe or existing campaign freeze')
    build_path = contract.regular(root, specification['build']['path'], 'full build')
    contract.read_ref(root, specification['build'], 'full build')
    build = campaign.builds.validate(build_path)
    configuration = contract.json_ref(root, specification['configuration'], 'configuration probe')
    identity = configuration['configuration']
    implementation = identity['implementation_sha256']
    contract.require(type(implementation) is list and len(implementation) == 32 and
                     all(type(byte) is int and 0 <= byte <= 255 for byte in implementation) and any(implementation),
                     'missing compiled implementation identity')
    index = contract.json_ref(root, specification['input_index'], 'quality preparation index')
    corpus = contract.digest(index.get('corpus_fingerprint_sha256'), 'corpus')
    implementation_hex = bytes(implementation).hex()
    frozen = {'schema': 'antfly.native-whole-head.campaign-freeze.v2', 'runtime_frozen': True,
              'release_authority': False, 'source': campaign.git_provenance(repo),
              'binary': {'path': str(build['binary_path']), 'sha256': build['binary_sha256']},
              'implementation_sha256': implementation_hex, 'corpus_fingerprint_sha256': corpus,
              'runtime_environment': {campaign.FLAGS[field]: '1' if identity['execution'][field] else '0'
                                      for field in campaign.EXECUTION_FIELDS},
              'statistical_seed': hashlib.sha256(('antfly.native-whole-head.statistics.v2\n' + corpus +
                                                   '\n' + implementation_hex + '\n').encode()).hexdigest(),
              'preparation_specification': specification_ref,
              **{key: specification[key] for key in ('build', 'configuration', 'configuration_audit',
                                                      'input_index', 'models', 'performance')}}
    with (root / 'native-whole-head-active.lock').open('a+') as active:
        fcntl.flock(active.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        output.parent.mkdir(parents=True, exist_ok=True)
        pending = output.with_name(output.name + '.pending')
        campaign.write_new(pending, frozen)
        pending_ref = campaign.reference(root, pending)
        # This performs full source/build/configuration/corpus/performance
        # reconciliation without decoding scoring answers or loading a model.
        campaign.validate_freeze(repo, root, pending_ref)
        for model in contract.MODELS:
            batch = next(batch for batch in index['batches'] if batch['model'] == model)
            manifest = contract.json_ref(root, batch['input'], 'native model binding')
            campaign.model_artifact(root, frozen, batch, manifest)
        # link() publishes the exact validated bytes exclusively, including if
        # another actor created the destination during validation.
        os.link(pending, output)
        pending.unlink()
        return campaign.reference(root, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--campaign-root', type=Path, required=True)
    parser.add_argument('--specification', type=Path, required=True)
    parser.add_argument('--specification-sha256', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = freeze(args.root, args.campaign_root,
                    {'path': str(args.specification), 'sha256': args.specification_sha256}, args.output)
    print(result['path'] + ' ' + result['sha256'])


if __name__ == '__main__':
    main()
