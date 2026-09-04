import io
import json
import unittest
from unittest import mock

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
        payload = loader.batch_payload(
            loader.encode_entry(b'{"text":"one"}', 0), "full_index"
        )
        self.assertEqual("full_index", json.loads(payload)["sync_level"])

    def test_batches_resume_at_stable_corpus_ordinal(self):
        source = io.BytesIO(
            b'{"text":"zero"}\n{"text":"one"}\n{"text":"two"}\n{"text":"three"}\n'
        )
        output = list(loader.batches(source, 100, start_document=2))
        documents = {}
        for entries, _ in output:
            documents.update(json.loads(b"{" + entries + b"}"))
        self.assertEqual(["doc:2", "doc:3"], list(documents))
        self.assertEqual(4, output[-1][1])

    def test_rejects_missing_text(self):
        with self.assertRaisesRegex(ValueError, "missing string text"):
            loader.encode_entry(b'{"body":"wrong"}\n', 3)

    def test_ingest_retries_explicit_backpressure(self):
        class Response:
            def __init__(self, status, body):
                self.status = status
                self._body = body

            def read(self):
                return self._body

        class Connection:
            def __init__(self, *_, **__):
                self.requests = 0

            def request(self, *_, **__):
                self.requests += 1

            def getresponse(self):
                if self.requests == 1:
                    return Response(429, b"table backpressured")
                return Response(201, b'{"inserted":1}')

            def close(self):
                pass

        with (
            mock.patch.object(loader.http.client, "HTTPConnection", Connection),
            mock.patch.object(loader.time, "sleep"),
        ):
            client = loader.AntflyClient("http://127.0.0.1:8080/db/v1", "docs", 1)
            self.assertEqual(1, client.ingest(b'"doc:1":{}', "write"))
            self.assertEqual(2, client.connection.requests)
            self.assertEqual(1, client.backpressure_retries)
            self.assertGreater(client.backpressure_wait_seconds, 0)


if __name__ == "__main__":
    unittest.main()
