import io
import json
import unittest

import load_quickwit_search_benchmark as loader


class QuickwitLoaderTest(unittest.TestCase):
    def test_encode_document_preserves_unicode_and_ordinal(self):
        encoded = loader.encode_document('{"text":"Héllo 世界"}\n'.encode(), 7)
        self.assertEqual({"corpus_ordinal": 7, "body": "Héllo 世界"}, json.loads(encoded))

    def test_batches_preserve_zero_based_nonblank_ordinal(self):
        source = io.BytesIO(b'{"text":"one"}\n\n{"text":"two"}\n{"text":"three"}\n')
        output = list(loader.batches(source, 70))
        documents = [json.loads(line) for payload, _ in output for line in payload.splitlines()]
        self.assertEqual([0, 1, 2], [document["corpus_ordinal"] for document in documents])
        self.assertEqual(3, output[-1][1])

    def test_max_documents_applies_to_nonblank_documents(self):
        source = io.BytesIO(b'\n{"text":"one"}\n{"text":"two"}\n')
        output = list(loader.batches(source, 1024, max_documents=1))
        self.assertEqual(1, output[-1][1])

    def test_rejects_missing_text(self):
        with self.assertRaisesRegex(ValueError, "missing string text"):
            loader.encode_document(b'{"body":"wrong"}\n', 3)


if __name__ == "__main__":
    unittest.main()
