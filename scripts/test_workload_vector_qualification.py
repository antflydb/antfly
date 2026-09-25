"""Offline tests for independent vector truth, calibration and frozen load settings."""

import importlib.util
import json
import math
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import workload_qualification as harness
import workload_vector_qualification as vectors


class VectorQualificationTests(unittest.TestCase):
    def fixture(self):
        spec = vectors.specification(32, 8)
        spec.update(
            calibration_queries=2,
            held_out_queries=2,
            profile_queries=1,
            calibration_repeats=1,
            efforts=[0, 0.05, 1],
        )
        return vectors.Fixture(spec)

    def fake_http(self, fixture, *, held_out_failure=False):
        calls = []

        class HTTP:
            def __init__(self, *_):
                self.connection = SimpleNamespace(timeout=1, sock=None)

            def close(self):
                pass

            def request(self, method, route, body):
                query_id = fixture.queries.index(body["embeddings"]["vec"])
                effort = body["search_effort"]
                calls.append((query_id, effort, body["profile"]))
                ids = fixture.neighbors[query_id].copy()
                if effort == 0 or (
                    held_out_failure and query_id >= fixture.spec["calibration_queries"]
                ):
                    ids[-1] = next(
                        f"key:{i}"
                        for i in range(fixture.spec["rows"])
                        if f"key:{i}" not in ids
                    )
                if effort == 0.05:
                    time.sleep(0.003)
                result = {
                    "responses": [
                        {
                            "hits": {"hits": [{"_id": value} for value in ids]},
                            "profile": {
                                "dense_search": {
                                    "resolved_search_width": 10,
                                    "hbc_exact_vectors_scored": 32,
                                }
                            },
                        }
                    ]
                }
                return 200, json.dumps(result).encode(), {}

        return HTTP, calls

    def test_deterministic_queries_have_independent_exact_cosine_truth(self):
        fixture = self.fixture()
        self.assertEqual(fixture.queries, self.fixture().queries)
        self.assertEqual(
            fixture.source_receipt["corpus_sha256"],
            self.fixture().source_receipt["corpus_sha256"],
        )
        for query, expected in zip(fixture.queries, fixture.neighbors):
            self.assertNotIn(query, fixture.training)
            norm = math.sqrt(math.fsum(value * value for value in query))
            actual = sorted(
                range(len(fixture.training)),
                key=lambda i: (
                    -math.fsum(a * b for a, b in zip(query, fixture.training[i]))
                    / (
                        norm
                        * math.sqrt(
                            math.fsum(value * value for value in fixture.training[i])
                        )
                    )
                ),
            )
            self.assertEqual(
                expected, [f"key:{i}" for i in actual[: fixture.spec["k"]]]
            )

    def test_fastest_qualifying_median_wins_not_minimum_effort_or_lucky_trial(self):
        def point(effort, rates, recalls, errors=(0, 0, 0)):
            return {
                "effort": effort,
                "median_completed_qps": sorted(rates)[1],
                "trials": [
                    {"recall": recall, "errors": error}
                    for recall, error in zip(recalls, errors)
                ],
            }

        points = [
            point(0, [1000] * 3, [0.94] * 3),
            point(0.05, [500, 10, 11], [1] * 3),
            point(0.2, [100] * 3, [0.95] * 3),
            point(1, [200] * 3, [1] * 3, (0, 1, 0)),
        ]
        self.assertEqual(vectors.select(points, 0.95)["effort"], 0.2)
        with self.assertRaises(ValueError):
            vectors.select([points[0], points[3]], 0.95)

    def test_calibration_repeats_and_freezes_before_disjoint_held_out(self):
        fixture = self.fixture()
        http, calls = self.fake_http(fixture)
        with tempfile.TemporaryDirectory() as tmp:
            effort = vectors.calibrate(http, 1, fixture, Path(tmp), 1)
            report = json.loads((Path(tmp) / "vector-calibration.json").read_text())
            samples = [
                json.loads(line)
                for line in (Path(tmp) / "vector-calibration.jsonl")
                .read_text()
                .splitlines()
            ]
        self.assertEqual(effort, 1)
        self.assertEqual(report["status"], "frozen_for_performance")
        self.assertEqual(report["held_out"]["recall"], 1)
        self.assertTrue(all(len(point["trials"]) == 3 for point in report["points"]))
        calibration_ids = {
            sample["query_id"] for sample in samples if sample["phase"] == "calibration"
        }
        held_ids = {
            sample["query_id"] for sample in samples if sample["phase"] == "held_out"
        }
        self.assertFalse(calibration_ids & held_ids)
        self.assertEqual(
            {effort for query, effort, _ in calls if query in held_ids}, {1}
        )
        self.assertTrue(report["identical_work_counters"])
        self.assertFalse(report["flat_calibration_recall"])

    def test_held_out_failure_retains_evidence_and_does_not_retune(self):
        fixture = self.fixture()
        http, calls = self.fake_http(fixture, held_out_failure=True)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "held-out"):
                vectors.calibrate(http, 1, fixture, Path(tmp), 1)
            report = json.loads((Path(tmp) / "vector-calibration.json").read_text())
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["held_out"]["recall"], 0.9)
        held = [(query, effort) for query, effort, _ in calls if query >= 2]
        self.assertEqual(held, [(2, 1), (3, 1)])

    def test_performance_freezes_effort_and_counts_actual_recall(self):
        fixture = self.fixture()
        http, _ = self.fake_http(fixture)
        workload = {
            "name": "vector",
            "kind": "vector",
            "_fixture": fixture,
            "_effort": 0,
        }
        plan = harness.template("process")
        with tempfile.TemporaryDirectory() as tmp, patch.object(harness, "HTTP", http):
            path = Path(tmp) / "samples.jsonl"
            point = harness.run_load(
                1, workload, plan, path, seconds=0.01, concurrency=1
            )
            samples = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertGreater(point["counts"]["completed"], 0)
        self.assertEqual(point["vector"]["recall"], 0.9)
        self.assertFalse(point["vector"]["recall_floor_pass"])
        self.assertFalse(harness.successful_baseline(point))
        self.assertEqual({value["search_effort"] for value in samples}, {0})
        self.assertTrue(
            all(
                value["query_id"] >= fixture.spec["calibration_queries"]
                for value in samples
            )
        )

    def test_vector_specs_require_below_point35_grid_and_pinned_retained_data(self):
        spec = self.fixture().spec
        vectors.validate(spec, qualification=False)
        for changes in (
            {"efforts": [0.35, 1]},
            {"calibration_trials": 1},
            {"recall_floor": 0.9},
        ):
            with self.assertRaises(ValueError):
                vectors.validate({**spec, **changes}, qualification=False)
        with self.assertRaises(ValueError):
            vectors.validate(spec, qualification=True)
        for rows, dimensions in ((50_000, 1536), (1_000_000, 768)):
            full = vectors.specification(rows, dimensions)
            self.assertEqual(full["source"], "vdbbench_parquet")
            self.assertIsNone(full["files"]["train"]["sha256"])
            with self.assertRaises(ValueError):
                vectors.validate(full, qualification=True)

    def test_vector_load_does_not_accept_a_write_success_status(self):
        class WrongStatusHTTP:
            def __init__(self, *_):
                self.connection = SimpleNamespace(timeout=1, sock=None)

            def close(self):
                pass

            def request(self, *_):
                return 201, b"{}", {}

        workload = {
            "name": "vector",
            "kind": "vector",
            "_fixture": self.fixture(),
            "_effort": 1,
        }
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(harness, "HTTP", WrongStatusHTTP),
        ):
            point = harness.run_load(
                1,
                workload,
                harness.template("process"),
                Path(tmp) / "samples.jsonl",
                seconds=0.01,
                concurrency=1,
            )
        self.assertGreater(point["counts"]["unexpected_http_error"], 0)
        self.assertFalse(point["vector"]["recall_floor_pass"])

    def test_calibration_trials_honor_minimum_timing_window(self):
        fixture = self.fixture()
        # Short fractional test window; production validation requires integer
        # seconds, and qualification requires at least ten seconds per trial.
        fixture.spec["calibration_seconds"] = 0.01
        http, _ = self.fake_http(fixture)
        with tempfile.TemporaryDirectory() as tmp:
            vectors.calibrate(http, 1, fixture, Path(tmp), 1)
            report = json.loads((Path(tmp) / "vector-calibration.json").read_text())
        self.assertTrue(
            all(
                trial["seconds"] >= 0.01
                for point in report["points"]
                for trial in point["trials"]
            )
        )
        self.assertTrue(
            any(
                trial["queries"] > 2
                for point in report["points"]
                for trial in point["trials"]
            )
        )

    def test_changed_dataset_is_rejected_before_loading(self):
        spec = vectors.specification()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "data"
            path.write_bytes(b"original")
            spec["files"] = {
                "train": {"path": str(path), "sha256": vectors.digest(path)}
            }
            fixture = vectors.Fixture(spec)
            fixture.verify_files()
            path.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "digest"):
                fixture.verify_files()

    def test_rejected_calibration_cannot_be_selected_as_fastest(self):
        class RejectedHTTP:
            def __init__(self, *_):
                pass

            def close(self):
                pass

            def request(self, *_):
                return 429, b'{"reason":"instance_busy"}', {}

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "no tested vector effort"):
                vectors.calibrate(RejectedHTTP, 1, self.fixture(), Path(tmp), 1)
            report = json.loads((Path(tmp) / "vector-calibration.json").read_text())
            self.assertTrue(
                all(
                    trial["errors"] == trial["queries"]
                    for point in report["points"]
                    for trial in point["trials"]
                )
            )
            self.assertNotIn("held_out", report)

    @unittest.skipUnless(
        importlib.util.find_spec("pyarrow"), "optional Parquet input requires PyArrow"
    )
    def test_retained_parquet_inputs_keep_ground_truth_and_require_complete_unique_ids(
        self,
    ):
        import pyarrow as pa
        import pyarrow.parquet as pq

        synthetic = self.fixture()
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            pq.write_table(
                pa.table({"id": list(range(32)), "emb": synthetic.training}),
                directory / "train.parquet",
            )
            pq.write_table(
                pa.table({"emb": synthetic.queries}), directory / "test.parquet"
            )
            pq.write_table(
                pa.table(
                    {
                        "neighbors_id": [
                            [int(value[4:]) for value in row]
                            for row in synthetic.neighbors
                        ]
                    }
                ),
                directory / "neighbors.parquet",
            )
            spec = {
                **synthetic.spec,
                "source": "vdbbench_parquet",
                "provenance": "test-only generated Parquet fixture, not retained workload",
                "files": {
                    key: {
                        "path": str(directory / f"{key}.parquet"),
                        "sha256": vectors.digest(directory / f"{key}.parquet"),
                    }
                    for key in ("train", "test", "neighbors")
                },
            }
            vectors.validate(spec, qualification=False)
            fixture = vectors.Fixture(spec)
            self.assertEqual(fixture.neighbors, synthetic.neighbors)
            self.assertEqual(fixture.queries, synthetic.queries)
            self.assertEqual(sum(len(batch) for batch in fixture.batches()), 32)
            self.assertEqual(
                fixture.source_receipt["row_counts"],
                {"train": 32, "test": 4, "neighbors": 4},
            )
            pq.write_table(
                pa.table({"id": [0] * 32, "emb": synthetic.training}),
                directory / "train.parquet",
            )
            spec["files"]["train"]["sha256"] = vectors.digest(
                directory / "train.parquet"
            )
            duplicate = vectors.Fixture(spec)
            with self.assertRaisesRegex(ValueError, "exactly"):
                list(duplicate.batches())


if __name__ == "__main__":
    unittest.main()
