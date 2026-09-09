import copy
import unittest

from qualify_documents import evaluate_run, summarize
from test_compare import run


def sample(sync="full_index", cap=268435456):
    value = run()
    value["provenance"].update(
        sync_level=sync,
        consumers=2,
        read_profile=True,
        render_memory_bytes=cap,
        render_workers=4,
        render_prefetch=1,
        reader_batch_size=4,
    )
    for row in value["results"]:
        row["consumer_results"] = [
            {
                "unit_text_sha256": row["unit_text_sha256"],
                "unit_render_geometry": row["unit_render_geometry"],
                "searchable_vectors": 1,
                "manifest_counts": row["manifests"],
            }
        ]
    log = "\n".join(
        [
            "read-profile phase=pdf_render source_fingerprint=source page=1 failure=null",
            "read-profile phase=pdf_render_window peak_bytes=100 requested_parallelism=4 peak_parallelism=1 failure=null",
        ]
        * 3
    )
    return {"sync_level": sync, "memory_bytes": cap, "run": value, "log": log}


class DocumentQualificationTests(unittest.TestCase):
    def test_requires_both_paths_at_each_memory_cap(self):
        runs = [
            sample(sync, cap)
            for sync in ("full_index", "write")
            for cap in (134217728, 268435456)
        ]
        self.assertTrue(summarize(runs, 3)["pass"])
        self.assertFalse(summarize(runs[:-1], 3)["pass"])
        self.assertFalse(summarize(runs + runs[:1], 3)["pass"])

    def test_render_reuse_requires_physical_page_evidence(self):
        entry = sample()
        for log in (
            "",
            entry["log"] + entry["log"],
            entry["log"].replace("failure=null", "failure=OutOfMemory"),
            entry["log"].replace("peak_bytes=100", "peak_bytes=999999999"),
            entry["log"].replace("peak_parallelism=1", "peak_parallelism=5"),
            entry["log"].replace("page=1", "page=null"),
        ):
            with self.subTest(log=log):
                self.assertFalse(
                    evaluate_run(entry["run"], log, 3, entry["memory_bytes"])["pass"]
                )

    def test_page_counts_alone_do_not_hide_identity_multiplicity(self):
        entry = sample()
        log = entry["log"].replace(
            "source_fingerprint=source", "source_fingerprint=other", 1
        )
        self.assertFalse(
            evaluate_run(entry["run"], log, 3, entry["memory_bytes"])["pass"]
        )

    def test_output_or_binary_drift_is_not_a_qualified_pressure_fallback(self):
        runs = [
            sample(sync, cap)
            for sync in ("full_index", "write")
            for cap in (134217728, 268435456)
        ]
        changed = copy.deepcopy(runs)
        changed[-1]["run"]["results"][0]["unit_text_sha256"] = {
            "wrong": {"page:000001": "different"}
        }
        self.assertFalse(summarize(changed, 3)["pass"])
        changed = copy.deepcopy(runs)
        changed[-1]["run"]["provenance"]["binary_sha256"] = "different"
        self.assertFalse(summarize(changed, 3)["pass"])

    def test_missing_consumer_and_incomplete_indexing_fail(self):
        for mutation in ("consumer", "indexing"):
            entry = sample()
            if mutation == "consumer":
                entry["run"]["results"][0]["consumer_results"] = []
            else:
                entry["run"]["results"][0]["passed"] = False
            self.assertFalse(
                evaluate_run(entry["run"], entry["log"], 3, entry["memory_bytes"])[
                    "pass"
                ]
            )


if __name__ == "__main__":
    unittest.main()
