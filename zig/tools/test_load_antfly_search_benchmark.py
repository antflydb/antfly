import io
import json
import unittest

import load_antfly_search_benchmark as loader


class AntflyLoaderTest(unittest.TestCase):
    def test_entry_preserves_unicode_and_ordinal(self):
        entry = loader.encode_entry('{"text":"Héllo 世界"}\n'.encode(), 7)
        parsed = json.loads(b"{" + entry + b"}")
        self.assertEqual({"corpus_ordinal": 7, "body": "Héllo 世界"}, parsed["doc:7"])

    def test_batches_preserve_zero_based_nonblank_ordinal(self):
        source = io.BytesIO(b'{"text":"one"}\n\n{"text":"two"}\n{"text":"three"}\n')
        output = list(loader.batches(source, 100))
        documents = {}
        for entries, _ in output:
            documents.update(json.loads(b"{" + entries + b"}"))
        self.assertEqual(["doc:0", "doc:1", "doc:2"], list(documents))
        self.assertEqual(3, output[-1][1])

    def test_payload_declares_sync_level(self):
        payload = loader.batch_payload(loader.encode_entry(b'{"text":"one"}', 0), "full_index")
        self.assertEqual("full_index", json.loads(payload)["sync_level"])

    def test_batches_resume_at_stable_corpus_ordinal(self):
        source = io.BytesIO(b'{"text":"zero"}\n{"text":"one"}\n{"text":"two"}\n{"text":"three"}\n')
        output = list(loader.batches(source, 100, start_document=2))
        documents = {}
        for entries, _ in output:
            documents.update(json.loads(b"{" + entries + b"}"))
        self.assertEqual(["doc:2", "doc:3"], list(documents))
        self.assertEqual(4, output[-1][1])

    def test_rejects_missing_text(self):
        with self.assertRaisesRegex(ValueError, "missing string text"):
            loader.encode_entry(b'{"body":"wrong"}\n', 3)


if __name__ == "__main__":
    unittest.main()
