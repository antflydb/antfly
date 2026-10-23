import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from native_checkpoint_unified_host import discover_unified_worker, sum_listener_rss, qualification_admission_capacity


class UnifiedHostEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root/'antfly'; self.binary.touch()
        for task in ('10', '11'):
            path = self.root/'10'/'task'/task
            path.mkdir(parents=True)
            (path/'children').write_text('')
    def child(self, pid, command=b'inference\0_worker\0', binary=None):
        path=self.root/str(pid); path.mkdir(exist_ok=True)
        (path/'exe').symlink_to(binary or self.binary)
        (path/'cmdline').write_bytes(str(self.binary).encode()+b'\0'+command)
    def test_worker_spawned_by_nonleader_thread_is_discovered_once(self):
        self.child(12)
        (self.root/'10/task/11/children').write_text('12')
        self.assertEqual(discover_unified_worker(10,self.binary,self.root),12)
        (self.root/'10/task/10/children').write_text('12')
        self.assertEqual(discover_unified_worker(10,self.binary,self.root),12)
    def test_wrong_command_or_binary_cannot_attest_worker(self):
        self.child(12,b'standalone\0')
        (self.root/'10/task/11/children').write_text('12')
        with self.assertRaisesRegex(RuntimeError,'found 0'): discover_unified_worker(10,self.binary,self.root)
        (self.root/'12/cmdline').write_bytes(b'antfly\0inference\0_worker\0')
        (self.root/'12/exe').unlink()
        other=self.root/'other';other.touch();(self.root/'12/exe').symlink_to(other)
        with self.assertRaisesRegex(RuntimeError,'found 0'): discover_unified_worker(10,self.binary,self.root)
    def test_ambiguous_workers_fail_closed(self):
        self.child(12);self.child(13)
        (self.root/'10/task/11/children').write_text('12 13')
        with self.assertRaisesRegex(RuntimeError,'found 2'): discover_unified_worker(10,self.binary,self.root)
    def test_memory_cap_includes_listener_and_worker(self):
        (self.root/'10/status').write_text('Name:\tantfly\nVmRSS:\t1024 kB\n')
        row={'pid':12,'host_rss_bytes':2048,'gpu_allocations':[]}
        result=sum_listener_rss(row,10,self.root)
        self.assertEqual(result['host_rss_bytes'],1024*1024+2048)
        self.assertEqual(result['worker_host_rss_bytes'],2048)
        self.assertEqual(row['host_rss_bytes'],2048)
    def test_missing_listener_memory_is_not_zero(self):
        (self.root/'10/status').write_text('Name:\tantfly\n')
        with self.assertRaisesRegex(RuntimeError,'missing listener RSS'):
            sum_listener_rss({'host_rss_bytes':2048},10,self.root)
    def test_overlap_can_budget_weighted_work_without_changing_other_campaigns(self):
        self.assertEqual(qualification_admission_capacity(SimpleNamespace()), 2)
        self.assertEqual(qualification_admission_capacity(
            SimpleNamespace(inference_admission_capacity=48)), 48)
        for invalid in (0, -1, True, 1.5, '48', None):
            with self.subTest(capacity=invalid), self.assertRaises(ValueError):
                qualification_admission_capacity(SimpleNamespace(inference_admission_capacity=invalid))


if __name__=='__main__': unittest.main()
