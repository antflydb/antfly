import unittest

from check_source_publication_churn import check_identity, check_restored_recall


class PublicationChurnGatesTest(unittest.TestCase):
    def test_identity_and_restored_count_are_required(self):
        payload = {
            "status": {"readiness": {"incarnation": "clone"}, "doc_count": 50000}
        }
        check_identity(payload, "clone")
        with self.assertRaises(RuntimeError):
            check_identity(payload, "another-server")
        payload["status"]["doc_count"] = 49000
        with self.assertRaises(RuntimeError):
            check_identity(payload, "clone")

    def test_missing_nonfinite_and_degraded_recall_fail_closed(self):
        before = {"count": 1000, "recall": 0.99}
        check_restored_recall(before, {"count": 1000, "recall": 0.98})
        for after in (
            {},
            {"count": 1000, "recall": float("nan")},
            {"count": 10, "recall": 0.99},
            {"count": 1000, "recall": 0.979},
        ):
            with self.assertRaises(RuntimeError):
                check_restored_recall(before, after)


if __name__ == "__main__":
    unittest.main()
