import unittest
from native_phase_timing import generation_timing, prepare_timing

GENERATION = 'generate_timing_ms: prompt_format=0 tokenize=1 multimodal_prepare=0 runtime_prepare=1 prefill=10 decode=3 text_decode=1 total=16\n'
PREPARE = 'info: native_checkpoint_prepare_work source_tokens=8189 outcome=compacted query_ns=100 snapshot_ns=200 fit_ns=300 export_write_ns=400 export_clock_valid=1 checkpoint_bytes=201728532 terminal_complete=0\n'


class PhaseTimingTests(unittest.TestCase):
    def test_generation_reports_exclusive_additive_phases(self):
        row = generation_timing('other logs\n' + GENERATION, .020)
        self.assertEqual(row['decode'], 3)
        self.assertEqual(row['text_decode'], 1)
        self.assertEqual(row['total'], 16)

    def test_generation_rejects_ambiguous_or_missing_requests(self):
        for text in ['', GENERATION * 2]:
            with self.assertRaises(ValueError): generation_timing(text, 1)

    def test_generation_rejects_corrupt_phases_and_impossible_wall_times(self):
        for text in [GENERATION.replace('total=16', 'total=17'),
                     GENERATION.replace('decode=3', 'decode=-3'),
                     GENERATION.replace('total=16', 'total=16 total=16'),
                     GENERATION.replace('prefill=10 ', '')]:
            with self.assertRaises(ValueError): generation_timing(text, 1)
        for wall in [.001, float('nan'), float('inf'), 0, True]:
            with self.assertRaises(ValueError): generation_timing(GENERATION, wall)

    def test_prepare_reports_work_without_claiming_terminal_completion(self):
        row = prepare_timing(PREPARE, 8189, 201728532, 1)
        self.assertEqual(row['measured_work_sum_ns'], 1000)
        self.assertEqual(row['terminal_complete'], 0)

    def test_prepare_rejects_wrong_export_and_missing_clock(self):
        for old, new in [('source_tokens=8189', 'source_tokens=32752'),
                         ('checkpoint_bytes=201728532', 'checkpoint_bytes=2'),
                         ('outcome=compacted', 'outcome=unchanged_insufficient_savings'),
                         ('export_clock_valid=1', 'export_clock_valid=0'),
                         ('terminal_complete=0', 'terminal_complete=1'),
                         ('fit_ns=300', 'fit_ns=0')]:
            with self.assertRaises(ValueError):
                prepare_timing(PREPARE.replace(old, new), 8189, 201728532, 1)

    def test_prepare_rejects_duplicate_and_impossible_intervals(self):
        for text in ['', PREPARE * 2]:
            with self.assertRaises(ValueError): prepare_timing(text, 8189, 201728532, 1)
        with self.assertRaises(ValueError): prepare_timing(PREPARE, 8189, 201728532, 1e-9)


if __name__ == '__main__': unittest.main()
