import copy
import unittest
from qualify_rotated4_checkpoint import summarize, latency_gate, latency_eligible, latency_sampling_qualified


class QualityGateTests(unittest.TestCase):
    def test_sampling_profiles_do_not_mislabel_smoke_or_extended_evidence(self):
        self.assertTrue(latency_sampling_qualified('bounded', 5, 20))
        self.assertFalse(latency_sampling_qualified('bounded', 5, 19))
        self.assertFalse(latency_sampling_qualified('bounded', 4, 20))
        self.assertFalse(latency_sampling_qualified('bounded', 5, 20, smoke=True))
        self.assertFalse(latency_sampling_qualified('extended', 5, 20))
        self.assertTrue(latency_sampling_qualified('extended', 5, 100))

    def fixture(self):
        cases, rows = [], []
        good = {'choices': [{'finish_reason': 'stop', 'message': {'content': 'ok'}}]}
        bad = {'choices': [{'finish_reason': 'stop', 'message': {'content': 'wrong'}}]}
        for category in ('fact', 'instruction', 'code', 'tool_call'):
            for index in range(16):
                case_id = f'{category}-{index}'
                cases.append({'case_id': case_id, 'category': category, 'critical': index < 4,
                              'body': {}, 'expected': {'kind': 'text', 'value': 'ok'}})
                rows.append({'case_id': case_id, 'full_kv': good, 'f16': good, 'rotated4': good,
                             'checkpoints': {'f16': {'metadata': {'outcome': 'compacted'}}, 'rotated4': {'metadata': {
                                 'encoding': 'rotated4', 'wire_version': 4, 'outcome': 'compacted',
                                 'global_savings_ratio': 2, 'validated_heads': 8,
                                 'rotated4_heads': 2, 'rotated4_fallback_heads': 6,
                                 'rotated4_eligible_rows': 80, 'rotated4_compressed_rows': 15,
                                 'rotated4_exact_rows': 65}}}})
        return copy.deepcopy((cases, rows, bad))

    def test_complete_paired_gate_and_existing_failures(self):
        cases, rows, bad = self.fixture()
        self.assertTrue(summarize(cases, rows)['passed'])
        rows[0].update(full_kv=bad, f16=bad, rotated4=bad)
        result = summarize(cases, rows)
        self.assertTrue(result['passed'])
        self.assertEqual(result['vs_fp16']['baseline_failure_case_ids'], ['fact-0'])

    def test_regression_is_not_offset_by_improvement(self):
        cases, rows, bad = self.fixture()
        rows[0]['rotated4'] = bad
        rows[1]['f16'] = bad
        result = summarize(cases, rows)
        self.assertFalse(result['passed'])
        self.assertEqual(result['vs_fp16']['regression_case_ids'], ['fact-0'])

    def test_bounded_quality_requires_balanced_critical_cohort(self):
        cases, rows, _ = self.fixture()
        indices = [i for i, case in enumerate(cases) if case['critical']]
        cases, rows = [cases[i] for i in indices], [rows[i] for i in indices]
        self.assertTrue(summarize(cases, rows, 'bounded')['passed'])
        self.assertFalse(summarize(cases, rows)['passed'])
        self.assertFalse(summarize(cases, rows[:-1], 'bounded')['passed'])
        cases[0]['critical'] = False
        self.assertFalse(summarize(cases, rows, 'bounded')['passed'])
        cases[0]['critical'] = True
        cases[0]['category'] = 'code'
        self.assertFalse(summarize(cases, rows, 'bounded')['passed'])

    def test_shared_compaction_failure_is_distinct_from_shared_model_failure(self):
        cases, rows, bad = self.fixture()
        rows[0].update(f16=bad, rotated4=bad)
        result = summarize(cases, rows)
        self.assertTrue(result['passed'])  # Codec preserved its FP16 baseline.
        self.assertFalse(result['full_kv_preservation_passed'])
        self.assertEqual(result['fp16_vs_full_kv']['regression_case_ids'], ['fact-0'])
        rows[0]['full_kv'] = bad
        self.assertTrue(summarize(cases, rows)['full_kv_preservation_passed'])

    def test_codec_cannot_override_fp16_publication_even_with_correct_answer(self):
        cases, rows, _ = self.fixture()
        rows[0]['checkpoints']['f16']['metadata']['outcome'] = 'unchanged_insufficient_savings'
        result = summarize(cases, rows)
        self.assertFalse(result['passed'])
        self.assertEqual(result['publication_policy_mismatch_case_ids'], ['fact-0'])
        self.assertEqual(result['vs_fp16']['regression_case_ids'], [])

    def test_partial_and_fallback_only_cannot_qualify(self):
        cases, rows, _ = self.fixture()
        self.assertFalse(summarize(cases, rows[:-1])['passed'])
        for row in rows[:16]:
            row['checkpoints']['rotated4']['metadata'].update(rotated4_heads=0, rotated4_fallback_heads=8,
                                                                         rotated4_compressed_rows=0, rotated4_exact_rows=80)
        self.assertFalse(summarize(cases, rows)['passed'])

    def test_malformed_or_reordered_evidence_fails_closed(self):
        cases, rows, _ = self.fixture()
        for field, value in [('wire_version', 3), ('global_savings_ratio', float('nan')),
                             ('outcome', 'unknown'), ('rotated4_heads', True)]:
            altered = copy.deepcopy(rows)
            altered[0]['checkpoints']['rotated4']['metadata'][field] = value
            with self.subTest(field=field), self.assertRaises((ValueError, AssertionError)):
                summarize(cases, altered)
        with self.assertRaises(ValueError): summarize(cases, list(reversed(rows)))

    def test_latency_gate_requires_samples_and_each_paired_phase_budget(self):
        seconds = {encoding: {metric: {'p50': 1.0, 'p95': 2.0} for metric in
                   ('total_seconds', 'first_content_seconds', 'first_content_to_done_seconds')}
                   for encoding in ('f16', 'rotated4')}
        self.assertFalse(latency_gate(seconds, False)['passed'])
        self.assertTrue(latency_gate(seconds, True)['passed'])
        seconds['rotated4']['first_content_seconds']['p95'] = 3.01
        self.assertFalse(latency_gate(seconds, True)['passed'])
        seconds['rotated4']['first_content_seconds']['p95'] = float('nan')
        with self.assertRaises(ValueError): latency_gate(seconds, True)

    def test_latency_selection_requires_real_paired_work_before_timing(self):
        cases, rows, bad = self.fixture()
        case, row = cases[0], rows[0]
        row['checkpoints']['f16'] = {'metadata': {'outcome': 'compacted'}}
        row['f16']['usage'] = {'completion_tokens': 8}
        row['rotated4']['usage'] = {'completion_tokens': 8}
        self.assertTrue(latency_eligible(case, row))
        for encoding in ('f16', 'rotated4'):
            altered = copy.deepcopy(row)
            altered['checkpoints'][encoding]['metadata']['outcome'] = 'unchanged_insufficient_savings'
            self.assertFalse(latency_eligible(case, altered))
            altered = copy.deepcopy(row)
            altered[encoding] = bad
            self.assertFalse(latency_eligible(case, altered))
        altered = copy.deepcopy(row)
        altered['checkpoints']['rotated4']['metadata']['rotated4_compressed_rows'] = 0
        self.assertFalse(latency_eligible(case, altered))
        altered = copy.deepcopy(row)
        altered['rotated4'] = copy.deepcopy(altered['rotated4'])
        altered['rotated4']['usage']['completion_tokens'] = 9
        self.assertFalse(latency_eligible(case, altered))
        self.assertFalse(latency_eligible(dict(case, category='tool_call'), row))



if __name__ == '__main__': unittest.main()
