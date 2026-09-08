"""Evidence for the single-index deferred-capture public-API experiment.

These stage observations do not replace ordinary visibility, restart, recall,
memory and latency qualification. Collection time is not a measured speedup.
"""

from native_preparation_experiments import events

FLAG = "ANTFLY_EXPERIMENT_DEFER_SOURCE_CAPTURE"


def capture_preparation_evidence(lines, deferred):
    rows = events(lines, "dense replay collection ")
    committed = {}
    observed = []
    try:
        for row in events(lines, "dense replay capture finish "):
            if (
                row.get("success") != "true"
                or row.get("applied_sequence_persisted") != "true"
            ):
                continue
            token, sequence = int(row["token"]), int(row["sequence"])
            if (
                not 0 < token <= 2**64 - 1
                or not 0 < sequence <= 2**64 - 1
                or token in committed
            ):
                raise ValueError("invalid or reused committed capture identity")
            committed[token] = sequence
        for row in rows:
            if row["deferred_capture"] != str(deferred).lower():
                raise ValueError("effective treatment differs from receipt")
            if row["capture_before_collection"] not in ("true", "false"):
                raise ValueError("invalid capture ownership observation")
            values = {
                key: int(row[key])
                for key in (
                    "records",
                    "applied_windows",
                    "collect_ns",
                    "apply_ns",
                )
            }
            if any(value < 0 for value in values.values()):
                raise ValueError("negative replay observation")
            if not values["records"] or not values["applied_windows"]:
                continue
            if (
                not 0 < int(row["sequence"]) <= 2**64 - 1
                or not 0 < int(row["token"]) <= 2**64 - 1
            ):
                raise ValueError("invalid transaction identity")
            # A capture may contain multiple calls/windows. Its final durable
            # watermark must cover this observation, not equal every window.
            if committed.get(int(row["token"]), 0) < int(row["sequence"]):
                continue
            observed.append(
                {
                    **values,
                    "capture_before_collection": row["capture_before_collection"]
                    == "true",
                }
            )
    except (ValueError, KeyError) as error:
        raise RuntimeError("invalid deferred-capture evidence") from error
    if not observed:
        raise RuntimeError("no committed capture preparation observations")
    outside = sum(not row["capture_before_collection"] for row in observed)
    if (deferred and not outside) or (not deferred and outside):
        raise RuntimeError("capture preparation treatment was inert or mismatched")
    return {
        "committed_observations": len(observed),
        "observations_starting_outside_capture": outside,
        "collection_ns": sum(row["collect_ns"] for row in observed),
        "apply_ns": sum(row["apply_ns"] for row in observed),
    }
