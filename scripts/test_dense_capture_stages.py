import unittest
from unittest.mock import Mock

from summarize_dense_capture_stages import summarize


class CaptureStagesTest(unittest.TestCase):
    def test_nested_timers_and_incomplete_records_remain_separate(self):
        path = Mock()
        path.read_text.return_value = (
            "info: dense capture stages batch=1 sequence=7 completed=true patch_ns=2000000 wal_ns=3000000 publication_ns=4000000 total_ns=9000000\n"
            "info: dense WAL stages batch=1 sequence=7 records=4 bytes=100 encode_ns=1000000 append_sync_ns=2000000 sync=true\n"
            "info: dense capture stages batch=2 sequence=9 completed=false total_ns=5000000\n"
            "info: unrelated line\n"
        )
        result = summarize(path)
        self.assertEqual(result["completed_captures"], 1)
        self.assertEqual(result["incomplete_captures"], 1)
        self.assertEqual(result["capture_stages"]["total_ns"]["sum_ms"], 9)
        self.assertEqual(result["wal_stages"]["append_sync_ns"]["sum_ms"], 2)
        self.assertEqual(result["wal_bytes"], 100)
        self.assertEqual(result["synced_wal_appends"], 1)
        self.assertEqual(result["unsynced_wal_appends"], 0)
        load = summarize(path, through_sequence=7)
        self.assertEqual(load["completed_captures"], 1)
        self.assertEqual(load["incomplete_captures"], 0)
        self.assertEqual(load["wal_appends"], 1)
        mixed = summarize(path, after_sequence=7, through_sequence=9)
        self.assertEqual(mixed["completed_captures"], 0)
        self.assertEqual(mixed["incomplete_captures"], 1)
        self.assertEqual(mixed["wal_appends"], 0)


if __name__ == "__main__":
    unittest.main()
