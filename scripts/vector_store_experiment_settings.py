"""Explicit controls and treatments for same-binary source-store experiments."""

FIRST = {
    "metadata": {"ANTFLY_SOURCE_VECTOR_METADATA_ONLY": "1"},
    "cache": {"ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES": "65536"},
    "segments": {"ANTFLY_SOURCE_VECTOR_TARGET_SEGMENT_BYTES": "8388608"},
    "gc": {"ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES": "8388608"},
    "batch": {"ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS": "64"},
}
FIRST["combined"] = {
    key: value for flags in FIRST.values() for key, value in flags.items()
}
NEXT = {
    "payload_segments": {"ANTFLY_SOURCE_VECTOR_APPEND_ONLY": "1"},
    "selective_gc": {"ANTFLY_SOURCE_VECTOR_SELECTIVE_GC": "1"},
    "ownership": {"ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX": "1"},
    "group_commit": {"ANTFLY_SOURCE_VECTOR_GROUP_COMMIT": "1"},
    "snapshot_reads": {"ANTFLY_SOURCE_VECTOR_SNAPSHOT_READS": "1"},
    "adaptive_cache": {"ANTFLY_SOURCE_VECTOR_ADAPTIVE_CACHE": "1"},
}
NEXT["next_combined"] = {
    **{key: value for flags in NEXT.values() for key, value in flags.items()},
    "ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES": "8388608",
    "ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES": "65536",
}
TREATMENTS = {**FIRST, **NEXT}
TREATMENTS["segment_gc"] = {
    "ANTFLY_SOURCE_VECTOR_APPEND_ONLY": "1",
    "ANTFLY_SOURCE_VECTOR_SELECTIVE_GC": "1",
}
CONTROLS = {
    "metadata": {"ANTFLY_SOURCE_VECTOR_METADATA_ONLY": "0"},
    "combined": {"ANTFLY_SOURCE_VECTOR_METADATA_ONLY": "0"},
    # Hold the established batching optimization constant in the next round.
    **{name: {"ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS": "64"} for name in NEXT},
}
CONTROLS["selective_gc"].update(
    ANTFLY_SOURCE_VECTOR_APPEND_ONLY="1", ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES="8388608"
)
CONTROLS["ownership"].update(ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES="8388608")
CONTROLS["adaptive_cache"].update(ANTFLY_SOURCE_VECTOR_LOCATION_CACHE_ENTRIES="65536")
CONTROLS["segment_gc"] = {
    "ANTFLY_ENRICHMENT_ARTIFACT_BATCH_ITEMS": "64",
    "ANTFLY_SOURCE_VECTOR_GC_STEP_BYTES": "8388608",
}
# Isolate the GC shape from ownership maintenance, then test their combination.
TREATMENTS["bounded_gc"] = {
    "ANTFLY_SOURCE_VECTOR_MARK_STEP_ROWS": "16384",
    "ANTFLY_SOURCE_VECTOR_COALESCE_DIRECTORY": "1",
}
CONTROLS["bounded_gc"] = {**CONTROLS["segment_gc"], **TREATMENTS["segment_gc"]}
TREATMENTS["bounded_gc_ownership"] = {
    **TREATMENTS["bounded_gc"],
    "ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX": "1",
}
CONTROLS["bounded_gc_ownership"] = dict(CONTROLS["bounded_gc"])
TREATMENTS["gc_ownership"] = {"ANTFLY_SOURCE_VECTOR_OWNERSHIP_INDEX": "1"}
CONTROLS["gc_ownership"] = {**CONTROLS["bounded_gc"], **TREATMENTS["bounded_gc"]}
# Preserve the previously qualified row-bound/coalescing/ownership baseline.
# These treatments isolate elapsed budgeting and moving the scan out of locks.
for name, flags in {
    "mark_time": {"ANTFLY_SOURCE_VECTOR_MARK_STEP_US": "2000"},
    "mark_unlocked": {"ANTFLY_SOURCE_VECTOR_MARK_OUTSIDE_LOCK": "1"},
    "foreground_gc": {
        "ANTFLY_SOURCE_VECTOR_MARK_STEP_US": "2000",
        "ANTFLY_SOURCE_VECTOR_MARK_OUTSIDE_LOCK": "1",
    },
}.items():
    TREATMENTS[name] = flags
    CONTROLS[name] = {
        **CONTROLS["bounded_gc_ownership"],
        **TREATMENTS["bounded_gc_ownership"],
    }
# Same pinned foreground-scan baseline for independent progress and protection.
for name, flags in {
    "scan_progress": {"ANTFLY_SOURCE_VECTOR_SCAN_DUTY_PERCENT": "50"},
    "rescue_reappends": {"ANTFLY_SOURCE_VECTOR_RESCUE_REAPPENDS": "1"},
    "scan_progress_rescue": {
        "ANTFLY_SOURCE_VECTOR_SCAN_DUTY_PERCENT": "50",
        "ANTFLY_SOURCE_VECTOR_RESCUE_REAPPENDS": "1",
    },
}.items():
    TREATMENTS[name] = flags
    CONTROLS[name] = {**CONTROLS["foreground_gc"], **TREATMENTS["foreground_gc"]}
# A selected treatment must also beat the prior row-bounded locked baseline;
# improving the previously regressed 2 ms combination alone is insufficient.
for name in ("scan_progress", "rescue_reappends", "scan_progress_rescue"):
    TREATMENTS[name + "_baseline"] = {**TREATMENTS["foreground_gc"], **TREATMENTS[name]}
    CONTROLS[name + "_baseline"] = dict(CONTROLS["foreground_gc"])
# Structural experiments hold the same active-scan policy constant. No reuse.
for name, flag in {
    "independent_scan": "ANTFLY_SOURCE_VECTOR_INDEPENDENT_SCAN",
    "shared_catalog": "ANTFLY_SOURCE_VECTOR_SHARED_CATALOG",
    "incremental_inventory": "ANTFLY_SOURCE_VECTOR_INCREMENTAL_INVENTORY",
}.items():
    TREATMENTS[name] = {flag: "1"}
    CONTROLS[name] = {**CONTROLS["scan_progress"], **TREATMENTS["scan_progress"]}
# Defer occurrence-map reconstruction when authenticated receipt totals suffice.
TREATMENTS["inventory_lazy"] = {"ANTFLY_SOURCE_VECTOR_LAZY_INVENTORY": "1"}
CONTROLS["inventory_lazy"] = {
    **CONTROLS["incremental_inventory"],
    **TREATMENTS["incremental_inventory"],
}
TREATMENTS["catalog_inventory"] = {
    **TREATMENTS["shared_catalog"],
    **TREATMENTS["incremental_inventory"],
    **TREATMENTS["inventory_lazy"],
}
CONTROLS["catalog_inventory"] = dict(CONTROLS["shared_catalog"])
TREATMENTS["catalog_inventory_baseline"] = {
    **TREATMENTS["foreground_gc"],
    **TREATMENTS["scan_progress"],
    **TREATMENTS["catalog_inventory"],
}
CONTROLS["catalog_inventory_baseline"] = dict(CONTROLS["foreground_gc"])
# Cost-recovery treatments use the qualified eager catalog/inventory control.
# The cutoff is experimental; 50K/1M only bracket a possible crossover.
_COST_BASE = {
    **CONTROLS["foreground_gc"],
    **TREATMENTS["foreground_gc"],
    **TREATMENTS["scan_progress"],
    **TREATMENTS["shared_catalog"],
    **TREATMENTS["incremental_inventory"],
    "ANTFLY_SOURCE_VECTOR_LAZY_INVENTORY": "0",
}
for name, flags in {
    "cost_delta_inventory": {"ANTFLY_SOURCE_VECTOR_DELTA_INVENTORY": "1"},
    "cost_debt_scheduling": {"ANTFLY_SOURCE_VECTOR_DEBT_SCHEDULING": "1"},
    "cost_bitmap_marking": {"ANTFLY_SOURCE_VECTOR_BITMAP_MARKING": "1"},
    "cost_small_inventory": {"ANTFLY_SOURCE_VECTOR_INVENTORY_MIN_PAYLOADS": "131072"},
}.items():
    CONTROLS[name] = dict(_COST_BASE)
    TREATMENTS[name] = flags
_COST_ALL = {
    key: value
    for name in (
        "cost_delta_inventory",
        "cost_debt_scheduling",
        "cost_bitmap_marking",
        "cost_small_inventory",
    )
    for key, value in TREATMENTS[name].items()
}
CONTROLS["cost_prior_baseline"] = dict(CONTROLS["foreground_gc"])
TREATMENTS["cost_prior_baseline"] = {**_COST_BASE, **_COST_ALL}
CONTROLS["cost_source_defaults"] = {}
TREATMENTS["cost_source_defaults"] = {**_COST_BASE, **_COST_ALL}
# Isolate locator cost against the bitmap prototype, then compare the complete
# compact shape against the hash-map control. Sparse batching uses matched debt
# scheduling to exercise the delayed sparse reclamation observed previously.
CONTROLS["mark_locator"] = {**_COST_BASE, "ANTFLY_SOURCE_VECTOR_BITMAP_MARKING": "1"}
TREATMENTS["mark_locator"] = {"ANTFLY_SOURCE_VECTOR_BITMAP_LOCATOR": "1"}
CONTROLS["mark_locator_shape"] = dict(_COST_BASE)
TREATMENTS["mark_locator_shape"] = {
    "ANTFLY_SOURCE_VECTOR_BITMAP_MARKING": "1",
    "ANTFLY_SOURCE_VECTOR_BITMAP_LOCATOR": "1",
}
CONTROLS["mark_sparse_batch"] = {
    **_COST_BASE,
    "ANTFLY_SOURCE_VECTOR_DEBT_SCHEDULING": "1",
}
TREATMENTS["mark_sparse_batch"] = {
    "ANTFLY_SOURCE_VECTOR_SPARSE_GC_COPY_BYTES": "67108864"
}
# Density planning is measured alone before combining it with sparse fallback.
CONTROLS["mark_planning"] = {
    **_COST_BASE,
    **TREATMENTS["mark_locator_shape"],
}
TREATMENTS["mark_planning"] = {"ANTFLY_SOURCE_VECTOR_INCREMENTAL_PLANNING": "1"}
CONTROLS["mark_planning_sparse"] = dict(CONTROLS["mark_planning"])
TREATMENTS["mark_planning_sparse"] = {
    **TREATMENTS["mark_planning"],
    **TREATMENTS["mark_sparse_batch"],
}
ALL_FLAGS = sorted(
    {key for flags in [*TREATMENTS.values(), *CONTROLS.values()] for key in flags}
)


def configure(environment, experiment, candidate):
    result = environment.copy()
    for key in ALL_FLAGS:
        result.pop(key, None)
    result.update(CONTROLS.get(experiment, {}))
    if candidate:
        result.update(TREATMENTS[experiment])
    elif experiment in ("metadata", "combined"):
        result["ANTFLY_SOURCE_VECTOR_METADATA_ONLY"] = "0"
    return result
