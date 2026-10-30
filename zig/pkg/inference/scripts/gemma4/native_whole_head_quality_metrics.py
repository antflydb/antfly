# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Paired metrics for authenticated native whole-head observations.

Primary source families are sampling clusters. Both context lengths of a
blueprint remain in that cluster, as do other blueprints sharing the family.
The fixed distractor collection is part of the declared cohort; these bounds
do not claim that every source part in each prompt is independent.
"""
import hashlib
import math
import random
from collections import defaultdict

BUCKETS = frozenset({'document_qa', 'exact_retrieval', 'code', 'structured_tool', 'multiturn'})
CONTEXTS = frozenset({8192, 32768})
MIN_CASES = 100
MIN_CLUSTERS = 100
BOOTSTRAP_ROUNDS = 10000


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def groups(rows):
    require(isinstance(rows, list) and rows, 'empty paired observations')
    blueprints = {}
    families = defaultdict(list)
    for row in rows:
        for field in ['blueprint_id', 'primary_family']:
            require(isinstance(row.get(field), str) and bool(row[field]), 'missing sampling identity')
        blueprint, family = row['blueprint_id'], row['primary_family']
        require(blueprint not in blueprints or blueprints[blueprint] == family,
                'blueprint changed primary family')
        blueprints[blueprint] = family
        families[family].append(row)
    return [families[key] for key in sorted(families)]


def paired_metrics(rows, seed_material, rounds=BOOTSTRAP_ROUNDS):
    require(type(rounds) is int and rounds >= 100, 'insufficient bootstrap rounds')
    require(len(rows) >= MIN_CASES, 'too few paired cases')
    clustered = groups(rows)
    require(len(clustered) >= MIN_CLUSTERS, 'too few independent primary families')
    totals = []
    for cluster in clustered:
        delta, tokens, nll = 0, 0, 0.
        for row in cluster:
            require(type(row.get('exact_correct')) is bool and type(row.get('candidate_correct')) is bool,
                    'nonboolean correctness')
            count = row.get('scored_tokens')
            require(type(count) is int and 2 <= count <= 256, 'invalid teacher token count')
            for key in ['exact_nll_sum', 'candidate_nll_sum']:
                value = row.get(key)
                require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
                        'invalid observed NLL')
            delta += int(row['exact_correct']) - int(row['candidate_correct'])
            tokens += count
            nll += row['candidate_nll_sum'] - row['exact_nll_sum']
        require(math.isfinite(nll), 'nonfinite cluster NLL')
        totals.append((len(cluster), delta, tokens, nll))
    generator = random.Random(int.from_bytes(hashlib.sha256(seed_material.encode()).digest(), 'big'))
    accuracy, nll = [], []
    for _ in range(rounds):
        count, delta, tokens, difference = 0, 0, 0, 0.
        for _ in totals:
            size, loss, scored, likelihood = totals[generator.randrange(len(totals))]
            count += size
            delta += loss
            tokens += scored
            difference += likelihood
        accuracy.append(delta / count)
        nll.append(difference / tokens)
    accuracy.sort()
    nll.sort()
    bound_index = math.ceil(.95 * rounds) - 1
    return {
        'cases': len(rows), 'primary_source_clusters': len(clustered),
        'distinct_blueprints': len({row['blueprint_id'] for row in rows}),
        'exact_correct': sum(row['exact_correct'] for row in rows),
        'candidate_correct': sum(row['candidate_correct'] for row in rows),
        'accuracy_drop': sum(total[1] for total in totals) / len(rows),
        'accuracy_drop_upper95': accuracy[bound_index],
        'scored_tokens': sum(total[2] for total in totals),
        'nll_delta_per_token': math.fsum(total[3] for total in totals) / sum(total[2] for total in totals),
        'nll_delta_per_token_upper95': nll[bound_index],
        'confidence': 'one_sided_95_percentile_primary_source_cluster_bootstrap',
        'bootstrap_rounds': rounds,
    }


def evaluate(rows, seed_material, rounds=BOOTSTRAP_ROUNDS):
    require(type(rows) is list and len({row.get('id') for row in rows}) == len(rows),
            'duplicate paired case ids')
    require(all(isinstance(row.get('id'), str) and row['id'] for row in rows), 'missing paired case id')
    require({row.get('bucket') for row in rows} == BUCKETS and
            {row.get('context_tokens') for row in rows} == CONTEXTS,
            'quality coverage differs from declared cohort')
    cohorts, blueprints = defaultdict(list), defaultdict(list)
    for row in rows:
        cohorts[(row['context_tokens'], row['bucket'])].append(row)
        blueprints[row['blueprint_id']].append(row)
    expected = {(context, bucket) for context in CONTEXTS for bucket in BUCKETS}
    require(set(cohorts) == expected and all(len(cell) >= MIN_CASES for cell in cohorts.values()) and
            len({len(cell) for cell in cohorts.values()}) == 1,
            'missing imbalanced or undersized quality cell')
    require(all(len(pair) == 2 and {row['context_tokens'] for row in pair} == CONTEXTS and
                len({row['bucket'] for row in pair}) == 1 for pair in blueprints.values()),
            'blueprint context pairing changed')
    overall = paired_metrics(rows, seed_material + ':aggregate', rounds)
    cells = {f'{context}/{bucket}': paired_metrics(cohorts[(context, bucket)],
             seed_material + f':{context}:{bucket}', rounds) for context, bucket in sorted(expected)}
    buckets = {bucket: paired_metrics([row for row in rows if row['bucket'] == bucket],
               seed_material + ':bucket:' + bucket, rounds) for bucket in sorted(BUCKETS)}
    passed = (overall['accuracy_drop_upper95'] <= .02 and
              all(cell['accuracy_drop_upper95'] <= .05 for cell in cells.values()) and
              overall['nll_delta_per_token'] <= math.log(1.05))
    return {
        'schema': 'antfly.native-whole-head.clustered-quality.v2', 'passed': passed,
        'production_ready': False, 'release_authority': False, 'overall': overall,
        'cells': cells, 'buckets': buckets,
        'baseline_failure_case_ids': [row['id'] for row in rows if not row['exact_correct']],
        'regression_case_ids': [row['id'] for row in rows if row['exact_correct'] and not row['candidate_correct']],
        'improvement_case_ids': [row['id'] for row in rows if not row['exact_correct'] and row['candidate_correct']],
        'thresholds': {'aggregate_accuracy_drop_upper95': .02, 'cell_accuracy_drop_upper95': .05,
                       'nll_delta_per_token': math.log(1.05)},
        'sampling_unit': 'primary source family, retaining both context lengths and all repeated blueprints',
        'all_source_parts_independent': False, 'fixed_distractor_cohort': True,
    }
