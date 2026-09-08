"""Explicit controls and treatments for same-binary source-store experiments."""

FIRST = {
    'metadata': {'ANTFLY_SOURCE_VECTOR_METADATA_ONLY': '1'},
    'cache': {'ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES': '65536'},
    'segments': {'ANTFLY_SOURCE_VECTOR_TARGET_SEGMENT_BYTES': '8388608'},
    'gc': {'ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES': '8388608'},
    'batch': {'ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS': '64'},
}
FIRST['combined'] = {key: value for flags in FIRST.values() for key, value in flags.items()}
NEXT = {
    'payload_segments': {'ANTFLY_SOURCE_VECTOR_APPEND_ONLY': '1'},
    'selective_gc': {'ANTFLY_SOURCE_VECTOR_SELECTIVE_GC': '1'},
    'ownership': {'ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX': '1'},
    'group_commit': {'ANTFLY_SOURCE_VECTOR_GROUP_COMMIT': '1'},
    'snapshot_reads': {'ANTFLY_SOURCE_VECTOR_SNAPSHOT_READS': '1'},
    'adaptive_cache': {'ANTFLY_SOURCE_VECTOR_ADAPTIVE_CACHE': '1'},
}
NEXT['next_combined'] = {
    **{key: value for flags in NEXT.values() for key, value in flags.items()},
    'ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES': '8388608',
    'ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES': '65536',
}
TREATMENTS = {**FIRST, **NEXT}
TREATMENTS['segment_gc'] = {
    'ANTFLY_SOURCE_VECTOR_APPEND_ONLY': '1',
    'ANTFLY_SOURCE_VECTOR_SELECTIVE_GC': '1',
}
CONTROLS = {
    'metadata': {'ANTFLY_SOURCE_VECTOR_METADATA_ONLY': '0'},
    'combined': {'ANTFLY_SOURCE_VECTOR_METADATA_ONLY': '0'},
    # Hold the established batching optimization constant in the next round.
    **{name: {'ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS': '64'} for name in NEXT},
}
CONTROLS['selective_gc'].update(ANTFLY_SOURCE_VECTOR_APPEND_ONLY='1', ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES='8388608')
CONTROLS['ownership'].update(ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES='8388608')
CONTROLS['adaptive_cache'].update(ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES='65536')
CONTROLS['segment_gc'] = {
    'ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS': '64',
    'ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES': '8388608',
}
# Isolate the GC shape from ownership maintenance, then test their combination.
TREATMENTS['bounded_gc'] = {
    'ANTFLY_SOURCE_VECTOR_MARK_STEP_ROWS': '16384',
    'ANTFLY_SOURCE_VECTOR_COALESCE_DIRECTORY': '1',
}
CONTROLS['bounded_gc'] = {**CONTROLS['segment_gc'], **TREATMENTS['segment_gc']}
TREATMENTS['bounded_gc_ownership'] = {
    **TREATMENTS['bounded_gc'], 'ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX': '1',
}
CONTROLS['bounded_gc_ownership'] = dict(CONTROLS['bounded_gc'])
TREATMENTS['gc_ownership'] = {'ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX': '1'}
CONTROLS['gc_ownership'] = {**CONTROLS['bounded_gc'], **TREATMENTS['bounded_gc']}
# Preserve the previously qualified row-bound/coalescing/ownership baseline.
# These treatments isolate elapsed budgeting and moving the scan out of locks.
for name, flags in {
    'mark_time': {'ANTFLY_SOURCE_VECTOR_MARK_STEP_US': '2000'},
    'mark_unlocked': {'ANTFLY_SOURCE_VECTOR_MARK_OUTSIDE_LOCK': '1'},
    'foreground_gc': {'ANTFLY_SOURCE_VECTOR_MARK_STEP_US': '2000',
                      'ANTFLY_SOURCE_VECTOR_MARK_OUTSIDE_LOCK': '1'},
}.items():
    TREATMENTS[name] = flags
    CONTROLS[name] = {**CONTROLS['bounded_gc_ownership'], **TREATMENTS['bounded_gc_ownership']}
# Same pinned foreground-scan baseline for independent progress and protection.
for name, flags in {
    'scan_progress': {'ANTFLY_SOURCE_VECTOR_SCAN_DUTY_PERCENT': '50'},
    'rescue_reappends': {'ANTFLY_SOURCE_VECTOR_RESCUE_REAPPENDS': '1'},
    'scan_progress_rescue': {'ANTFLY_SOURCE_VECTOR_SCAN_DUTY_PERCENT': '50',
                             'ANTFLY_SOURCE_VECTOR_RESCUE_REAPPENDS': '1'},
}.items():
    TREATMENTS[name] = flags
    CONTROLS[name] = {**CONTROLS['foreground_gc'], **TREATMENTS['foreground_gc']}
ALL_FLAGS = sorted({key for flags in [*TREATMENTS.values(), *CONTROLS.values()] for key in flags})


def configure(environment, experiment, candidate):
    result = environment.copy()
    for key in ALL_FLAGS:
        result.pop(key, None)
    result.update(CONTROLS.get(experiment, {}))
    if candidate:
        result.update(TREATMENTS[experiment])
    elif experiment in ('metadata', 'combined'):
        result['ANTFLY_SOURCE_VECTOR_METADATA_ONLY'] = '0'
    return result
