"""Reject incomplete sync-wait evidence before accepting churn attribution."""
import unittest
from profile_vector_store_churn import parse_batch_profiles


class ProfileTests(unittest.TestCase):
    def test_exact_rows_and_fence(self):
        text = ("info: antfly_bench_batch sequence=1 writes=2 deletes=0 sync=write sync_wait_ms=0\n"
                "info: antfly_bench_batch sequence=2 writes=1 deletes=0 sync=full_index sync_wait_ms=31\n")
        rows = parse_batch_profiles(text, 3, 2)
        self.assertEqual(sum(int(p['sync_wait_ms']) for p in rows), 31)
        for invalid in [text.splitlines()[0], text + text, text.replace('full_index', 'write')]:
            with self.assertRaises(RuntimeError):
                parse_batch_profiles(invalid, 3, 2)


if __name__ == '__main__':
    unittest.main()
