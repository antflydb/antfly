"""Vector fixture/calibration support for the local scheduling harness.

Uses the existing VectorDBBench Parquet columns and Antfly external-vector API.
No dataset download, approximate ground-truth generation, or automatic retuning.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import re
import statistics
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPException
from pathlib import Path
from typing import Any

TABLE = "workload_vector_fixture"
INDEX = "vec"
EFFORTS = [0, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.5, 0.75, 1]
WORK_COUNTERS = (
    "resolved_search_width",
    "hbc_leaves_explored",
    "hbc_approx_vectors_scored",
    "hbc_exact_vectors_scored",
    "hbc_reranked_vectors",
)


def specification(rows: int = 256, dimensions: int = 16) -> dict[str, Any]:
    small = rows <= 4096
    return {
        "source": "deterministic_cosine" if small else "vdbbench_parquet",
        "rows": rows,
        "dimensions": dimensions,
        "metric": "cosine",
        "k": 10 if small else 100,
        "seed": 72319,
        "calibration_queries": 16 if small else 200,
        "held_out_queries": 16 if small else 200,
        "calibration_trials": 3,
        "calibration_concurrency": 1 if small else 30,
        "calibration_repeats": 2 if small else 5,
        "calibration_seconds": 0 if small else 10,
        "profile_queries": 4,
        "recall_floor": 0.95,
        "efforts": EFFORTS.copy(),
        "files": {}
        if small
        else {
            key: {"path": None, "sha256": None}
            for key in ("train", "test", "neighbors")
        },
        "provenance": "Separate deterministic smoke fixture; not a retained benchmark dataset."
        if small
        else "REPLACE_WITH_RETAINED_BENCHMARK_DATASET_RECEIPT_REFERENCE",
    }


def validate(spec: dict[str, Any], *, qualification: bool) -> None:
    if (
        spec.get("source") not in {"deterministic_cosine", "vdbbench_parquet"}
        or spec.get("metric") != "cosine"
    ):
        raise ValueError(
            "vector source must be deterministic_cosine or pinned vdbbench_parquet; metric is cosine"
        )
    bounds = {
        "rows": (10, 1_000_000),
        "dimensions": (2, 1536),
        "k": (1, 100),
        "calibration_queries": (1, 1000),
        "held_out_queries": (1, 1000),
        "calibration_trials": (3, 20),
        "calibration_concurrency": (1, 80),
        "calibration_repeats": (1, 100),
        "calibration_seconds": (0, 60),
        "profile_queries": (1, 20),
        "seed": (0, 2**32 - 1),
    }
    for key, (low, high) in bounds.items():
        if type(spec.get(key)) is not int or not low <= spec[key] <= high:
            raise ValueError(f"vector {key} must be an integer in [{low}, {high}]")
    if (
        spec["k"] > spec["rows"]
        or spec["profile_queries"] > spec["calibration_queries"]
    ):
        raise ValueError("vector k/profile query count exceeds available rows")
    if spec.get("recall_floor") != 0.95:
        raise ValueError("vector comparison uses the declared 95% recall floor")
    if qualification and spec["calibration_seconds"] < 10:
        raise ValueError(
            "qualification calibration requires at least 10 seconds per timing trial"
        )
    efforts = spec.get("efforts", [])
    if (
        not efforts
        or len(efforts) > 100
        or any(
            type(e) not in (int, float) or not math.isfinite(e) or not 0 <= e <= 1
            for e in efforts
        )
        or len(set(efforts)) != len(efforts)
        or min(efforts) != 0
        or max(efforts) != 1
        or not any(0 < e < 0.35 for e in efforts)
    ):
        raise ValueError(
            "vector effort grid must include 0, 1 and positive efforts below .35"
        )
    if spec["source"] == "deterministic_cosine":
        if qualification or spec["rows"] > 4096 or spec["dimensions"] > 64:
            raise ValueError(
                "synthetic vectors are bounded smoke evidence, not retained qualification datasets"
            )
    else:
        if not spec.get("provenance") or spec["provenance"].startswith("REPLACE_"):
            raise ValueError("retain the original benchmark dataset provenance")
        if set(spec.get("files", {})) != {"train", "test", "neighbors"}:
            raise ValueError("pin train/test/neighbors files")
        for value in spec["files"].values():
            if (
                not re.fullmatch(r"[0-9a-f]{64}", value.get("sha256") or "")
                or not Path(value.get("path") or "").is_file()
            ):
                raise ValueError(
                    "vector dataset files require existing paths and SHA-256 digests"
                )


def _save(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def vector(value: Any, dimensions: int) -> list[float]:
    if (
        not isinstance(value, list)
        or len(value) != dimensions
        or any(type(x) not in (int, float) or not math.isfinite(x) for x in value)
    ):
        raise ValueError("invalid vector dimension/value")
    if sum(x * x for x in value) == 0:
        raise ValueError("cosine vector must be nonzero")
    return value


class Fixture:
    def __init__(self, spec: dict[str, Any]):
        self.spec = spec
        self.training: list[list[float]] | None = None
        self.queries: list[list[float]] = []
        self.neighbors: list[list[str]] = []
        self.source_receipt: dict[str, Any] = {
            "spec": spec,
            "ground_truth": "exact cosine over all training float32 vectors",
        }
        total = spec["calibration_queries"] + spec["held_out_queries"]
        if spec["source"] == "deterministic_cosine":
            rng = random.Random(spec["seed"])

            def generate() -> list[float]:
                values = [rng.gauss(0, 1) for _ in range(spec["dimensions"])]
                norm = math.sqrt(sum(value * value for value in values))
                return [
                    struct.unpack("f", struct.pack("f", value / norm))[0]
                    for value in values
                ]

            self.training = [generate() for _ in range(spec["rows"])]
            self.queries = [generate() for _ in range(total)]
            for query in self.queries:
                scored = [
                    (
                        sum(a * b for a, b in zip(query, row))
                        / math.sqrt(sum(v * v for v in row)),
                        i,
                    )
                    for i, row in enumerate(self.training)
                ]
                scored.sort(key=lambda value: (-value[0], value[1]))
                self.neighbors.append([f"key:{i}" for _, i in scored[: spec["k"]]])
            self.source_receipt["corpus_sha256"] = hashlib.sha256(
                json.dumps(self.training, separators=(",", ":")).encode()
            ).hexdigest()
        else:
            import pyarrow.parquet as pq

            self.verify_files()
            files = spec["files"]
            counts = {
                key: pq.ParquetFile(value["path"]).metadata.num_rows
                for key, value in files.items()
            }
            if (
                counts["train"] != spec["rows"]
                or counts["test"] != counts["neighbors"]
                or counts["test"] < total
            ):
                raise ValueError(f"dataset row count mismatch: {counts}")

            def first_rows(key: str, column: str) -> list[Any]:
                result = []
                for batch in pq.ParquetFile(files[key]["path"]).iter_batches(
                    batch_size=min(total, 256), columns=[column]
                ):
                    result.extend(batch.column(0).to_pylist())
                    if len(result) >= total:
                        return result[:total]
                raise ValueError("insufficient query/truth rows")

            self.queries = [
                vector(value, spec["dimensions"]) for value in first_rows("test", "emb")
            ]
            for row in first_rows("neighbors", "neighbors_id"):
                selected = row[: spec["k"]]
                if (
                    len(selected) != spec["k"]
                    or len(set(selected)) != spec["k"]
                    or any(
                        type(value) is not int or not 0 <= value < spec["rows"]
                        for value in selected
                    )
                ):
                    raise ValueError("invalid retained nearest-neighbor IDs")
                self.neighbors.append([f"key:{i}" for i in selected])
            self.source_receipt.update(
                row_counts=counts,
                ground_truth="retained VectorDBBench neighbors.parquet, not recomputed or replaced",
            )

    def verify_files(self) -> None:
        for value in self.spec["files"].values():
            if digest(Path(value["path"])) != value["sha256"]:
                raise ValueError(
                    "vector dataset digest changed or did not match the pinned input"
                )

    def batches(self):
        if self.training is not None:
            for start in range(0, len(self.training), 64):
                yield [
                    (i, self.training[i])
                    for i in range(start, min(start + 64, len(self.training)))
                ]
        else:
            import pyarrow.parquet as pq

            seen = bytearray(self.spec["rows"])
            for batch in pq.ParquetFile(
                self.spec["files"]["train"]["path"]
            ).iter_batches(batch_size=64, columns=["id", "emb"]):
                rows = []
                for i, values in zip(
                    batch.column(0).to_pylist(), batch.column(1).to_pylist()
                ):
                    if type(i) is not int or not 0 <= i < len(seen) or seen[i]:
                        raise ValueError(
                            "training IDs must cover the retained 0..rows-1 space exactly"
                        )
                    seen[i] = 1
                    rows.append((i, vector(values, self.spec["dimensions"])))
                yield rows
            if not all(seen):
                raise ValueError("missing training IDs")

    def query(
        self, query_id: int, effort: float, *, profile: bool = False
    ) -> dict[str, Any]:
        return {
            "embeddings": {INDEX: self.queries[query_id]},
            "search_effort": effort,
            "limit": self.spec["k"],
            "fields": [],
            "profile": profile,
        }

    def result(self, query_id: int, status: int, body: bytes) -> dict[str, Any]:
        if status != 200:
            raise ValueError(f"vector query HTTP {status}: {body[:200]!r}")
        rows = json.loads(body)["responses"]
        if len(rows) != 1 or rows[0].get("error"):
            raise ValueError("partial/failed vector response")
        ids = [hit["_id"] for hit in rows[0]["hits"]["hits"]]
        if (
            len(ids) != self.spec["k"]
            or len(set(ids)) != len(ids)
            or any(
                not re.fullmatch(r"key:\d+", value)
                or int(value[4:]) >= self.spec["rows"]
                for value in ids
            )
        ):
            raise ValueError("invalid vector result IDs/count")
        return {
            "query_id": query_id,
            "ids": ids,
            "expected_ids": self.neighbors[query_id],
            "matched_neighbors": len(set(ids) & set(self.neighbors[query_id])),
            "recall": len(set(ids) & set(self.neighbors[query_id])) / self.spec["k"],
            "profile": (rows[0].get("profile") or {}).get("dense_search"),
        }

    def retain(self, directory: Path) -> None:
        _save(
            directory / "vector-fixture.json",
            {
                **self.source_receipt,
                "query_splits": {
                    "calibration": list(range(self.spec["calibration_queries"])),
                    "held_out": list(
                        range(self.spec["calibration_queries"], len(self.queries))
                    ),
                },
                "queries": self.queries,
                "neighbors": self.neighbors,
            },
        )


def seed(http, port: int, fixture: Fixture, directory: Path) -> None:
    client = http(port, 300)
    receipts = []

    def request(label: str, method: str, path: str, body: Any = None):
        status, data, _ = client.request(method, f"/db/v1/tables/{TABLE}" + path, body)
        receipts.append(
            {
                "operation": label,
                "status": status,
                "body": data.decode(errors="replace"),
            }
        )
        if status not in {200, 201, 202}:
            raise RuntimeError(
                f"vector fixture {label} failed (not replayed): {status} {data!r}"
            )

    try:
        request("create", "POST", "", {"num_shards": 1})
        request("remove_unrelated_full_text", "DELETE", "/indexes/full_text_index_v0")
        request(
            "external_index",
            "POST",
            f"/indexes/{INDEX}",
            {
                "name": INDEX,
                "type": "embeddings",
                "external": True,
                "dimension": fixture.spec["dimensions"],
                "distance_metric": fixture.spec["metric"],
            },
        )
        loaded = 0
        for rows in fixture.batches():
            inserts = {
                f"key:{i}": {"id": i, "_embeddings": {INDEX: values}}
                for i, values in rows
            }
            loaded += len(rows)
            request(
                f"insert-through-{loaded}",
                "POST",
                "/batch",
                {
                    "inserts": inserts,
                    "sync_level": "full_index"
                    if loaded == fixture.spec["rows"]
                    else "write",
                },
            )
        if loaded != fixture.spec["rows"]:
            raise ValueError("vector load count mismatch")
        request("index_status_after_sync", "GET", f"/indexes/{INDEX}")
    finally:
        client.close()
        _save(directory / "vector-seed.json", receipts)


def select(points: list[dict[str, Any]], floor: float) -> dict[str, Any]:
    eligible = [
        point
        for point in points
        if all(
            trial["recall"] >= floor and trial["errors"] == 0
            for trial in point["trials"]
        )
    ]
    if not eligible:
        raise ValueError(
            "no tested vector effort cleared calibration recall/error floor"
        )
    return max(
        eligible, key=lambda point: (point["median_completed_qps"], -point["effort"])
    )


def calibrate(
    http, port: int, fixture: Fixture, directory: Path, timeout: float
) -> float:
    spec = fixture.spec
    points: list[dict[str, Any]] = []
    report: dict[str, Any] = {
        "status": "incomplete",
        "selection_rule": "highest median completed QPS across repeated fixed-concurrency calibration trials, every trial recall >= 0.95 and zero errors; held-out is never used to retune",
        "points": points,
        "calibration_concurrency": spec["calibration_concurrency"],
    }
    raw_path = directory / "vector-calibration.jsonl"
    lock = threading.Lock()
    try:
        with raw_path.open("w") as raw:

            def trial(
                effort: float,
                query_ids: list[int],
                label: str,
                number: int,
                *,
                profile: bool = False,
            ) -> dict[str, Any]:
                local = threading.local()
                clients = []
                samples = []
                completed = 0
                matched = 0

                def query(query_id: int):
                    nonlocal completed, matched
                    if not hasattr(local, "client"):
                        local.client = http(port, timeout)
                        with lock:
                            clients.append(local.client)
                    sample: dict[str, Any] = {
                        "phase": label,
                        "trial": number,
                        "effort": effort,
                        "query_id": query_id,
                    }
                    started = time.monotonic()
                    try:
                        status, body, _ = local.client.request(
                            "POST",
                            f"/db/v1/tables/{TABLE}/query",
                            fixture.query(query_id, effort, profile=profile),
                        )
                        sample.update(
                            fixture.result(query_id, status, body), status=status
                        )
                        if time.monotonic() - started > timeout:
                            raise ValueError("query exceeded its original deadline")
                    except (
                        OSError,
                        HTTPException,
                        ValueError,
                        TypeError,
                        KeyError,
                        IndexError,
                    ) as error:
                        sample["error"] = f"{type(error).__name__}: {error}"
                    sample["elapsed_ms"] = (time.monotonic() - started) * 1000
                    with lock:
                        raw.write(json.dumps(sample, separators=(",", ":")) + "\n")
                        if profile:
                            samples.append(sample)
                        if "error" not in sample:
                            completed += 1
                            matched += sample["matched_neighbors"]

                started = time.monotonic()
                try:
                    with ThreadPoolExecutor(
                        max_workers=spec["calibration_concurrency"]
                    ) as executor:
                        # Finite closed-loop workers pull one query at a time; no
                        # unbounded request submission or omitted arrivals.
                        dispatched = 0
                        minimum_seconds = (
                            spec["calibration_seconds"] if label == "calibration" else 0
                        )

                        def worker():
                            nonlocal dispatched
                            while True:
                                with lock:
                                    if (
                                        dispatched >= len(query_ids)
                                        and time.monotonic() - started
                                        >= minimum_seconds
                                    ):
                                        return
                                    query_id = query_ids[dispatched % len(query_ids)]
                                    dispatched += 1
                                query(query_id)

                        futures = [
                            executor.submit(worker)
                            for _ in range(spec["calibration_concurrency"])
                        ]
                        for future in futures:
                            future.result()
                finally:
                    for client in clients:
                        client.close()
                elapsed = time.monotonic() - started
                return {
                    "trial": number,
                    "queries": dispatched,
                    "completed": completed,
                    "errors": dispatched - completed,
                    "seconds": elapsed,
                    "completed_qps": completed / elapsed,
                    "recall": matched / (dispatched * spec["k"]),
                    "samples": samples,
                }

            # Rotate order across repetitions so monotonic warmup/drift does not
            # consistently favor the last (or first) effort. Warm every effort.
            by_effort = {
                effort: {"effort": effort, "trials": []} for effort in spec["efforts"]
            }
            ids = list(range(spec["calibration_queries"]))
            for effort in spec["efforts"]:
                trial(effort, ids, "warmup", 0)
            for number in range(spec["calibration_trials"]):
                offset = number % len(spec["efforts"])
                order = spec["efforts"][offset:] + spec["efforts"][:offset]
                if number % 2:
                    order.reverse()
                for effort in order:
                    result = trial(
                        effort, ids * spec["calibration_repeats"], "calibration", number
                    )
                    result.pop("samples")
                    by_effort[effort]["trials"].append(result)
            signatures = []
            work_signatures = []
            profiles_available = True
            for effort, point in by_effort.items():
                point["median_completed_qps"] = statistics.median(
                    value["completed_qps"] for value in point["trials"]
                )
                point["qps_min"] = min(
                    value["completed_qps"] for value in point["trials"]
                )
                point["qps_max"] = max(
                    value["completed_qps"] for value in point["trials"]
                )
                profile = trial(
                    effort,
                    ids[: spec["profile_queries"]],
                    "work_profile",
                    0,
                    profile=True,
                )
                point["work_profile_errors"] = profile["errors"]
                signatures.append(
                    sorted(
                        [
                            (value["query_id"], value.get("ids"))
                            for value in profile["samples"]
                        ]
                    )
                )
                work_signatures.append(
                    sorted(
                        [
                            (
                                value["query_id"],
                                {
                                    key: value["profile"][key]
                                    for key in WORK_COUNTERS
                                    if key in value.get("profile", {})
                                },
                            )
                            for value in profile["samples"]
                            if value.get("profile")
                        ]
                    )
                )
                profiles_available = (
                    profiles_available
                    and len(work_signatures[-1]) == spec["profile_queries"]
                    and all(value[1] for value in work_signatures[-1])
                )
                points.append(point)
            selected = select(points, spec["recall_floor"])
            recall_values = [
                round(statistics.mean(value["recall"] for value in point["trials"]), 12)
                for point in points
            ]
            report.update(
                selected_effort=selected["effort"],
                flat_calibration_recall=len(set(recall_values)) == 1,
                identical_profile_result_ids=all(
                    value == signatures[0] for value in signatures
                ),
                work_counters_available=profiles_available,
                identical_work_counters=all(
                    value == work_signatures[0] for value in work_signatures
                )
                if profiles_available
                else None,
                flat_curve_interpretation="advisory: saturation or ineffective tuning requires further diagnosis; flat recall is not proof of a product bug",
            )
            held_out = trial(
                selected["effort"],
                list(range(spec["calibration_queries"], len(fixture.queries))),
                "held_out",
                0,
            )
            held_out.pop("samples")
            report["held_out"] = held_out
            if held_out["errors"] or held_out["recall"] < spec["recall_floor"]:
                raise ValueError(
                    "selected vector effort failed held-out recall; no automatic retuning"
                )
            report["status"] = "frozen_for_performance"
            return selected["effort"]
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        _save(directory / "vector-calibration.json", report)
