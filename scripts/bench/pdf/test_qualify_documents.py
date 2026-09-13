import copy
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from benchmark import completed_log_offset
from qualify_documents import evaluate_run, summarize, verify_trial_profile
from render_matrix import render_observations
from test_compare import run


def sample(sync="full_index", cap=268435456):
    value = run()
    value["provenance"]["selected"][0]["pages"] = 1
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
        row["unit_render_geometry"]["scan.pdf"]["page:000001"]["page_number"] = 1
        row["consumer_results"] = [
            {
                "unit_text_sha256": copy.deepcopy(row["unit_text_sha256"]),
                "unit_render_geometry": copy.deepcopy(row["unit_render_geometry"]),
                "searchable_vectors": 1,
                "manifest_counts": copy.deepcopy(row["manifests"]),
            }
        ]
        row["manifests"]["scan.pdf"]["source_fingerprint"] = "source"
    log = (
        "\n".join(
            [
                "read-profile phase=pdf_render source_fingerprint=source page=1 failure=null",
                "read-profile phase=pdf_render_window source_fingerprint=source first_page=1 last_page=1 pages=1 peak_bytes=100 requested_parallelism=4 peak_parallelism=1 failure=null",
            ]
        )
        + "\n"
    )
    entry = {"sync_level": sync, "memory_bytes": cap, "run": value}
    attach_profile(entry, [log] * 3)
    return entry


def attach_profile(entry, logs):
    offset = 0
    for trial, (row, log) in enumerate(zip(entry["run"]["results"], logs, strict=True)):
        row["trial"] = trial
        size = len(log.encode("utf-8"))
        row["profile_log"] = {"start_byte": offset, "end_byte": offset + size}
        offset += size
    entry["log"] = "".join(logs)


class DocumentQualificationTests(unittest.TestCase):
    def test_checkpoints_during_split_background_writes_preserve_qualification(self):
        entry = sample()
        block = entry["log"][
            : entry["run"]["results"][0]["profile_log"]["end_byte"]
        ].encode()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            with path.open("wb", buffering=0) as writer:
                for row in entry["run"]["results"]:
                    writer.write(b"background caf\xc3")  # split UTF-8, not a record
                    start = completed_log_offset(path)
                    writer.write(b"\xa9\r\n" + block + b"next background message")
                    end = completed_log_offset(path)
                    row["profile_log"] = {"start_byte": start, "end_byte": end}
                    writer.write(b" completed\n")
            entry["log"] = path.read_bytes()
        result = evaluate_run(entry["run"], entry["log"], 3, entry["memory_bytes"])
        self.assertTrue(result["pass"], result)
        # A render begun after the final checkpoint is still outside its trial;
        # normalizing offsets must not suppress it once the record completes.
        self.assertFalse(
            evaluate_run(entry["run"], entry["log"] + block, 3, entry["memory_bytes"])[
                "pass"
            ]
        )

    def assert_profile_fails(self, entry):
        result = evaluate_run(entry["run"], entry["log"], 3, entry["memory_bytes"])
        self.assertFalse(result["pass"], result)

    def test_consistently_wrong_secondary_output_fails(self):
        for field, bad in (
            ("unit_text_sha256", {"scan.pdf": {"page:000001": "wrong"}}),
            (
                "unit_render_geometry",
                {"scan.pdf": {"page:000001": {"page_number": 999}}},
            ),
            (
                "manifest_counts",
                {
                    "scan.pdf": dict(
                        run()["results"][0]["manifests"]["scan.pdf"], chunk_count=2
                    )
                },
            ),
            ("searchable_vectors", 2),
        ):
            with self.subTest(field=field):
                runs = [
                    sample(sync, cap)
                    for sync in ("full_index", "write")
                    for cap in (134217728, 268435456)
                ]
                for entry in runs:
                    for row in entry["run"]["results"]:
                        row["consumer_results"][0][field] = copy.deepcopy(bad)
                self.assertFalse(summarize(runs, 3)["pass"])

    def test_profiles_require_expected_sources_pages_and_every_window(self):
        entry = sample()
        block = entry["log"][: entry["run"]["results"][0]["profile_log"]["end_byte"]]
        for bad in (
            block.replace("page=1", "page=999"),
            block.replace("source_fingerprint=source", "source_fingerprint=unknown"),
            block.splitlines()[0] + "\n",  # no admission window
            block.replace("last_page=1", "last_page=2"),
            block.replace("pages=1", "pages=2"),
            block.replace("requested_parallelism=4", "requested_parallelism=8"),
            block + block.splitlines()[1] + "\n",  # overlapping window
        ):
            with self.subTest(log=bad):
                changed = copy.deepcopy(entry)
                attach_profile(changed, [bad] * 3)
                self.assert_profile_fails(changed)

    def test_aggregate_counts_cannot_hide_cross_trial_duplicates(self):
        entry = sample()
        block = entry["log"][: entry["run"]["results"][0]["profile_log"]["end_byte"]]
        attach_profile(entry, [block * 2, "no rendering in this trial\n", block])
        self.assert_profile_fails(entry)

    def test_page_expectations_require_corpus_and_manifest_evidence(self):
        for mutation in ("fingerprint", "corpus", "geometry", "extra_source"):
            with self.subTest(mutation=mutation):
                entry = sample()
                row = entry["run"]["results"][0]
                if mutation == "fingerprint":
                    row["manifests"]["scan.pdf"].pop("source_fingerprint")
                elif mutation == "corpus":
                    entry["run"]["provenance"]["selected"][0]["pages"] = 2
                elif mutation == "geometry":
                    row["unit_render_geometry"]["scan.pdf"]["page:000001"][
                        "page_number"
                    ] = 999
                else:
                    row["unit_render_geometry"]["unknown.pdf"] = {
                        "page:000001": {"page_number": 1}
                    }
                # Preserve consumer parity so only provenance validation fails.
                row["consumer_results"][0]["unit_render_geometry"] = copy.deepcopy(
                    row["unit_render_geometry"]
                )
                self.assert_profile_fails(entry)

    def test_trial_boundaries_are_complete_disjoint_and_byte_based(self):
        entry = sample()
        block = entry["log"][: entry["run"]["results"][0]["profile_log"]["end_byte"]]
        attach_profile(
            entry,
            [
                ("diagnostic café\r\n" + block).replace(
                    "failure=null\n", "failure=null\r\n"
                )
            ]
            * 3,
        )
        self.assertTrue(
            evaluate_run(entry["run"], entry["log"], 3, entry["memory_bytes"])["pass"]
        )
        for mutation in ("missing", "overlap", "extra"):
            changed = copy.deepcopy(entry)
            if mutation == "missing":
                changed["run"]["results"][0].pop("profile_log")
            elif mutation == "overlap":
                changed["run"]["results"][1]["profile_log"]["start_byte"] = 0
            else:
                changed["log"] += block
            self.assert_profile_fails(changed)

    def test_terminal_partial_windows_cover_multiple_sources(self):
        lines = []
        expected = Counter()
        for source, pages in (("a", 5), ("b", 2)):
            for page in range(1, pages + 1):
                expected[(source, page)] += 1
                lines.append(
                    f"read-profile phase=pdf_render source_fingerprint={source} page={page} failure=null"
                )
            for first in range(1, pages + 1, 4):
                last = min(first + 3, pages)
                lines.append(
                    f"read-profile phase=pdf_render_window source_fingerprint={source} first_page={first} last_page={last} pages={last - first + 1} peak_bytes=100 requested_parallelism=4 peak_parallelism=1 failure=null"
                )
        observations = render_observations("\n".join(lines))
        verify_trial_profile(observations, expected, 1000, 4)
        with self.assertRaises(ValueError):
            verify_trial_profile(observations[:-1], expected, 1000, 4)

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
