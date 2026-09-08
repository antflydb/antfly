"""Independent native experiments and evidence gates for matched qualification.

An enabled flag is not evidence that a treatment ran. The caller must also
retain the ordinary execution, restart, visibility and paired recall gates.
"""

import json
import math
import re
import shutil

REFINEMENTS = {
    "staged_readers": ["ANTFLY_EXPERIMENT_STAGE_POSTING_READERS"],
    "staged_rebase": ["ANTFLY_EXPERIMENT_STAGE_POSTING_REBASE"],
    "certified_subgroups": ["ANTFLY_EXPERIMENT_CERTIFIED_SUBGROUPS"],
    "reused_posting_rows": ["ANTFLY_EXPERIMENT_REUSE_POSTING_ROWS"],
    "deferred_capture": ["ANTFLY_EXPERIMENT_DEFER_SOURCE_CAPTURE"],
    "coalesced_deletes": ["ANTFLY_EXPERIMENT_COALESCE_REPLAY_DELETES"],
    "reused_delete_vectors": ["ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS"],
    "stable_posting_origins": ["ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS"],
    "posting_row_deltas": ["ANTFLY_EXPERIMENT_POSTING_ROW_DELTAS"],
    "dense_delete_plan": [
        "ANTFLY_EXPERIMENT_COALESCE_REPLAY_DELETES",
        "ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS",
    ],
}


def require_disk_headroom(root, case):
    """Leave room for a fresh arm's live, staged and restart generations.

    This is a launch guard, not a reservation against unrelated disk writers.
    It never reclaims existing evidence or baseline data automatically.
    """
    needed = (12 if case == "Performance768D1M" else 3) * 1024**3
    free = shutil.disk_usage(root).free
    if free < needed:
        raise RuntimeError(
            f"insufficient disk headroom for {case}: {free / 1024**3:.1f} GiB free, "
            f"{needed / 1024**3:.0f} GiB required; no test database was created"
        )


def events(lines, marker):
    return [
        dict(re.findall(r"(\w+)=([^\s,]+)", line)) for line in lines if marker in line
    ]


def checkpoint_evidence(lines, environment):
    published_rows = events(lines, "dense posting checkpoint published ")
    publications = {
        row["generation"] for row in published_rows if row.get("generation") is not None
    }
    observations = {}
    for flag, marker, field in (
        (
            "ANTFLY_EXPERIMENT_POSTING_ROW_DELTAS",
            "dense posting row checkpoint ",
            "leaves",
        ),
        (
            "ANTFLY_EXPERIMENT_STAGE_POSTING_READERS",
            "dense checkpoint worker ",
            "readers_stage_ns",
        ),
        (
            "ANTFLY_EXPERIMENT_STAGE_POSTING_REBASE",
            "dense checkpoint rebase worker ",
            "rebase_stage_ns",
        ),
        (
            "ANTFLY_EXPERIMENT_REUSE_POSTING_ROWS",
            "dense checkpoint encoded row reuse ",
            "reused_bytes",
        ),
    ):
        if environment.get(flag) != "1":
            continue
        rows = events(lines, marker)
        if field == "reused_bytes":
            for row in rows:
                if row.get("generation") is None:
                    # The first row-reuse trace predates a generation field.
                    # Accept its sequence only when exactly one durable full
                    # publication matches; never guess among maintenance runs.
                    matches = {
                        p["generation"]
                        for p in published_rows
                        if p.get("generation") is not None
                        and p.get("kind") == "full"
                        and p.get("sequence") is not None
                        and p.get("sequence") == row.get("sequence")
                    }
                    if len(matches) == 1:
                        row["generation"] = matches.pop()
        try:
            values = [
                int(row[field])
                for row in rows
                if row.get("generation") in publications
                and row.get("success", "true") == "true"
            ]
        except (ValueError, KeyError) as error:
            raise RuntimeError(f"invalid treatment evidence for {flag}") from error
        if not values or any(value < 0 for value in values) or max(values) <= 0:
            raise RuntimeError(f"inert or unpublished treatment: {flag}")
        observations[field] = {
            "published_events": len(values),
            "sum": sum(values),
            "max": max(values),
        }
    return observations


def validate_native_treatment(arm, environment):
    # The archived harness predates the live runner's retry gate. A successful
    # client process does not qualify a run that retried failed public writes.
    required = {arm / "vdbbench-live.log", arm / "antfly-initial.log"}
    for path in required:
        if not path.is_file():
            raise RuntimeError(f"missing workload log: {path}")
    for path in sorted(required | set(arm.glob("antfly-*.log"))):
        with path.open(errors="replace") as stream:
            validate_workload_lines(stream, path.name)
    active = {
        flag
        for flags in REFINEMENTS.values()
        for flag in flags
        if environment.get(flag) == "1"
    }
    if not active:
        return None
    lines = (arm / "antfly-initial.log").read_text(errors="replace").splitlines()
    result = checkpoint_evidence(lines, environment)
    result.update(delete_preparation_evidence(lines, environment))
    if "ANTFLY_EXPERIMENT_CERTIFIED_SUBGROUPS" in active:
        profile = json.loads((arm / "public-query-profile.json").read_text())
        for key in (
            "hbc_subgroup_leaves_scored",
            "hbc_subgroup_vectors_skipped",
            "hbc_traversal_bound_stops",
        ):
            try:
                value = float(profile["profile_values"][key]["mean"])
            except (ValueError, KeyError, TypeError) as error:
                raise RuntimeError(
                    f"missing certified-routing observation: {key}"
                ) from error
            if not math.isfinite(value) or value <= 0:
                raise RuntimeError(f"inert certified-routing treatment: {key}")
            result[key] = value
    return result


def validate_workload_lines(lines, filename="workload log"):
    failures = (
        "Antfly insert error:",
        "Insert failed,",
        "public table batch failed",
        "VectorPayloadStorePoisoned",
        "err=error.OutOfMemory",
        "err=OutOfMemory",
        "PostingWalMutationOutsideCapture",
        "MissingPostingChunk",
        "PostingChunkIdentityConflict",
    )
    for number, line in enumerate(lines, 1):
        if any(marker in line for marker in failures):
            raise RuntimeError(
                f"unqualified workload failure in {filename}:{number}: {line.strip()[:500]}"
            )


def delete_preparation_evidence(lines, environment):
    """Require actual work reduction; ordinary lifecycle/recall gates still apply."""
    result = {}
    for flag, marker, key in (
        (
            "ANTFLY_EXPERIMENT_POSTING_ROW_DELTAS",
            "dense delete preserved rows ",
            "native_vector_rows",
        ),
        (
            "ANTFLY_EXPERIMENT_COALESCE_REPLAY_DELETES",
            "dense replay delete plan ",
            "deduplicated_keys",
        ),
        (
            "ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS",
            "dense delete apply ",
            "reused_vector_rows",
        ),
        (
            "ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS",
            "dense delete preserved rows ",
            "preserved_vector_rows",
        ),
    ):
        if environment.get(flag) != "1":
            continue
        try:
            rows = events(lines, marker)
            values = [
                (
                    int(row["requested"]) - int(row["unique"])
                    if key == "deduplicated_keys"
                    else int(
                        row["native_rows"]
                        if key == "native_vector_rows"
                        else (
                            row["rows"]
                            if key == "preserved_vector_rows"
                            else row["reused_rows"]
                        )
                    )
                )
                for row in rows
            ]
        except (KeyError, ValueError) as error:
            raise RuntimeError(f"invalid delete treatment evidence: {flag}") from error
        # Stable-origin filtering can eliminate every eager refresh. When
        # reuse is common to both arms, zero reused rows is then expected in
        # the candidate; the following stable-origin gate must still prove
        # positive preserved work. Never relax the standalone reuse gate.
        superseded = flag == "ANTFLY_EXPERIMENT_REUSE_DELETE_VECTORS" and (
            environment.get("ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS") == "1"
            or environment.get("ANTFLY_EXPERIMENT_POSTING_ROW_DELTAS") == "1"
        )
        if not values or min(values) < 0 or (max(values) <= 0 and not superseded):
            raise RuntimeError(f"inert delete treatment: {flag}")
        result[key] = {"events": len(values), "sum": sum(values), "max": max(values)}
    return result
