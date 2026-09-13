from __future__ import annotations

import copy
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

import audit_massive11_preparation as audit
import download_massive11 as download
import evaluation_contract as evaluation
import oracle
import prepare_massive11 as massive


class AnnotationTest(unittest.TestCase):
    def test_original_final_lf_is_optional_but_json_and_row_budget_are_strict(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.jsonl"
            path.write_bytes(b'{"id":"1"}\n{"id":"2"}')
            self.assertEqual([{"id": "1"}, {"id": "2"}], list(massive.raw_rows(path)))
            for raw in (b'{"id":1,"id":2}', b'{"id":NaN}', b'{}{}', b' ' * (massive.MAX_ROW_BYTES + 1)):
                path.write_bytes(raw)
                with self.assertRaises(ValueError): list(massive.raw_rows(path))

    def test_exact_repeated_values_and_unicode_offsets(self):
        text = "😀 café then café"
        facts, policy = massive.annotation_spans(text, "😀 [food_type : café] then [food_type : café]", "en-US")
        self.assertEqual("exact", policy)
        self.assertEqual([(5, 10), (16, 21)], [(fact["span"]["start"], fact["span"]["end"]) for fact in facts])
        self.assertEqual(text.encode()[16:21].decode(), facts[1]["span"]["text"])

    def test_cjk_projection_keeps_original_bytes_and_internal_spaces(self):
        text = "明天去New York"
        facts, policy = massive.annotation_spans(text, "[date : 明天] 去 [place_name : New York]", "zh-CN")
        self.assertEqual("cjk_ascii_space_projection", policy)
        self.assertEqual("New York", facts[1]["span"]["text"])
        self.assertEqual((9, 17), (facts[1]["span"]["start"], facts[1]["span"]["end"]))

    def test_non_space_mutation_and_undeclared_projection_fail(self):
        for text, annotated, locale in (("明天", "[date : 明年]", "zh-CN"), ("foobar", "[person : foo] bar", "en-US"),
                                       ("foo", "[person : ]foo", "en-US"), ("foo", "[person :  foo]", "en-US")):
            with self.subTest(annotated=annotated), self.assertRaises(evaluation.EvaluationError):
                massive.annotation_spans(text, annotated, locale)


class SplitTest(unittest.TestCase):
    def row(self, source_id, split, locale, text):
        return {"source_id": source_id, "partition": split, "locale": locale, "text": text, "facts": []}

    def test_translation_union_removes_the_entire_lower_priority_family(self):
        rows = [self.row("1", "test", "en-US", "Alarm"), self.row("1", "test", "de-DE", "Wecker"),
                self.row("2", "train", "en-US", " alarm "), self.row("2", "train", "de-DE", "Unrelated"),
                self.row("3", "test", "en-US", "ALARM"), self.row("4", "dev", "en-US", "Different")]
        retained, audit = massive.split_families(rows)
        self.assertEqual(["2"], audit["excluded_source_ids"])
        self.assertEqual(4, len(retained))
        self.assertEqual(1, len({row["family_id"] for row in retained if row["source_id"] in ("1", "3")}))
        self.assertEqual(3, sum(row["partition"] == "test" for row in retained))
        changed = copy.deepcopy(rows)
        for row in changed: row["facts"] = [{"unrelated_gold": True}]
        self.assertEqual(audit, massive.split_families(changed)[1])

    def test_source_id_must_not_cross_partitions(self):
        with self.assertRaisesRegex(evaluation.EvaluationError, "translation"):
            massive.split_families([self.row("1", "test", "en-US", "a"), self.row("1", "train", "de-DE", "b")])


class ContractTest(unittest.TestCase):
    def setUp(self):
        self.manifest = oracle.read_json(massive.MANIFEST)

    def test_full_ontologies_and_constraints_are_fixed_for_all_profiles(self):
        profiles = massive.profile_definitions(self.manifest)
        self.assertEqual(10, len(profiles))
        self.assertEqual(55, len(self.manifest["slot_types"]))
        self.assertEqual(60, len(self.manifest["intents"]))
        for profile in profiles:
            schema, metrics = massive.schema_and_metrics(profile, self.manifest)
            self.assertLessEqual(len(metrics), 64)
            if profile["task"] == "entities": self.assertEqual(self.manifest["slot_types"], schema["entities"])
            else: self.assertEqual(self.manifest["intents"], schema["classifications"][0]["labels"])
            if profile["task"] == "intent_scenario": self.assertEqual(60, len(schema["classification_constraints"]))

    def request(self, profile):
        schema, _ = massive.schema_and_metrics(profile, self.manifest)
        return {"text": "😀 hello", "schema": schema, "offset_unit": "utf8_bytes",
                "options": dict(self.manifest["request_defaults"], word_splitter=profile["word_splitter"])}

    def test_blind_entity_conversion_checks_full_ontology_and_original_bytes(self):
        profile = massive.profile_definitions(self.manifest)[0]
        request = self.request(profile)
        output = {"entities": {name: [] for name in self.manifest["slot_types"]}}
        output["entities"]["person"] = [{"text": "hello", "start": 2, "end": 7, "confidence": 0.7}]
        facts = massive.prediction_facts(request, output, "python", profile, self.manifest)
        self.assertEqual(5, facts["entity_exact"][0]["span"]["start"])
        del output["entities"]["time"]
        with self.assertRaises(evaluation.EvaluationError): massive.prediction_facts(request, output, "python", profile, self.manifest)

    def test_classification_records_constraints_without_using_gold(self):
        profile = next(p for p in massive.profile_definitions(self.manifest) if p["task"] == "intent_scenario")
        request = self.request(profile)
        intent = self.manifest["intents"][0]
        expected = self.manifest["intent_to_scenario"][intent]
        wrong = next(value for value in self.manifest["scenarios"] if value != expected)
        output = {"intent": {"value": intent, "confidence": .7}, "scenario": {"value": wrong, "confidence": .8}}
        facts = massive.prediction_facts(request, output, "python", profile, self.manifest)
        self.assertEqual([{"valid": False}], facts["constraints_satisfied"])
        self.assertEqual([{"label": intent}], facts["intent_exact"])
        output["intent"]["confidence"] = float("nan")
        with self.assertRaises(evaluation.EvaluationError): massive.prediction_facts(request, output, "python", profile, self.manifest)

    def test_immutable_file_tampering_fails_before_parsing_corpus(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / self.manifest["archive"]["path"]
            path.write_bytes(b"wrong")
            with self.assertRaises(oracle.ContractError): massive.source_rows(Path(temporary), self.manifest)


class ArchiveTest(unittest.TestCase):
    def pin(self, name="data/en-US.jsonl", data=b"original bytes"):
        return {"path": name, "size_bytes": len(data), "sha256": evaluation.digest(data)}

    def archive(self, path, members):
        with tarfile.open(path, "w:gz") as archive:
            for name, data, kind in members:
                info = tarfile.TarInfo(name)
                info.type = kind
                if kind == tarfile.REGTYPE:
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
                else:
                    info.linkname = "../../outside"
                    archive.addfile(info)

    def test_selected_bytes_only_without_extracting_unrequested_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); archive = root / "source.tar.gz"; output = root / "output"; output.mkdir()
            self.archive(archive, [("1.1/data/en-US.jsonl", b"original bytes", tarfile.REGTYPE),
                                   ("../../outside", b"discard", tarfile.REGTYPE)])
            download.extract_selected(archive, output, [self.pin()], "1.1/")
            self.assertEqual(b"original bytes", (output / "data/en-US.jsonl").read_bytes())
            self.assertEqual(["data/en-US.jsonl"], [str(path.relative_to(output)) for path in output.rglob("*") if path.is_file()])

    def test_duplicate_link_missing_and_corrupt_members_fail(self):
        regular = ("1.1/data/en-US.jsonl", b"original bytes", tarfile.REGTYPE)
        cases = [[regular, regular], [(regular[0], b"", tarfile.SYMTYPE)],
                 [("1.1/other", b"unused", tarfile.REGTYPE)], [(regular[0], b"modified bytes", tarfile.REGTYPE)]]
        for members in cases:
            with self.subTest(members=members), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); output = root / "output"; output.mkdir()
                self.archive(root / "source.tar.gz", members)
                with self.assertRaises(evaluation.EvaluationError):
                    download.extract_selected(root / "source.tar.gz", output, [self.pin()], "1.1/")

    def test_declared_paths_and_archive_member_budget_are_bounded(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); archive = root / "source.tar.gz"; output = root / "output"; output.mkdir()
            self.archive(archive, [(f"1.1/ignored/{index}", b"", tarfile.REGTYPE) for index in range(257)])
            for pins, prefix in (([self.pin("../outside")], "1.1/"), ([self.pin("/absolute")], "1.1/"),
                                 ([self.pin()], "../"), ([self.pin(), self.pin()], "1.1/"), ([self.pin()], "1.1/")):
                with self.subTest(pins=pins, prefix=prefix), self.assertRaises(evaluation.EvaluationError):
                    download.extract_selected(archive, output, pins, prefix)


class TransportTest(unittest.TestCase):
    def prepare(self, root):
        requests = [{"request_id": f"r{index}", "request_sha256": str(index) * 64} for index in range(3)]
        lock, prepared = "a" * 64, "b" * 64
        manifest = {"scope": "gliner25_transport_manifest/v1", "qualification": False, "records": len(requests),
            "request_ids_sha256": evaluation.digest(evaluation.encoded([row["request_id"] for row in requests])),
            "request_sha256s_sha256": evaluation.digest(evaluation.encoded([row["request_sha256"] for row in requests])),
            "completion_policy": "exact_global_id_coverage_and_order_all_errors_retained_atomically_aggregate_before_quality_report", "shards": []}
        for index, (start, end) in enumerate(((0, 2), (2, 3))):
            body = {"scope": "gliner25_blinded_transport_shard/v1", "qualification": False, "lock_sha256": lock,
                "prepared_sha256": prepared, "global_records": len(requests), "index": index, "start": start,
                "end": end, "requests": requests[start:end]}
            pin = self.write(root, f"shard{index}.json", body)
            manifest["shards"].append({"index": index, "start": start, "end": end, "file": pin})
        return requests, manifest, lock, prepared

    def write(self, root, name, value):
        path = root / name; oracle.write_json(path, value)
        return {"path": name, "size_bytes": path.stat().st_size, "sha256": oracle.sha256_file(path)}

    def test_shards_prove_complete_ordered_global_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); requests, manifest, lock, prepared = self.prepare(root)
            result = audit.validate_transport(root, requests, self.write(root, "transport.json", manifest), lock, prepared)
            self.assertEqual([2, 1], result["rows_per_shard"])
            self.assertEqual(3, result["records"])

    def test_rehashed_gap_reordering_gold_and_identity_changes_still_fail(self):
        for mutation in ("gap", "missing", "order", "gold", "lock", "duplicate_id"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); requests, manifest, lock, prepared = self.prepare(root)
                if mutation == "gap": manifest["shards"][1]["start"] = 1
                elif mutation == "missing": manifest["shards"].pop()
                elif mutation == "duplicate_id": requests[1]["request_id"] = requests[0]["request_id"]
                else:
                    body = oracle.read_json(root / "shard0.json")
                    if mutation == "order": body["requests"].reverse()
                    elif mutation == "gold": body["gold"] = []
                    elif mutation == "lock": body["lock_sha256"] = "c" * 64
                    manifest["shards"][0]["file"] = self.write(root, "shard0.json", body)
                with self.assertRaises(evaluation.EvaluationError):
                    audit.validate_transport(root, requests, self.write(root, "transport.json", manifest), lock, prepared)


if __name__ == "__main__":
    unittest.main()
