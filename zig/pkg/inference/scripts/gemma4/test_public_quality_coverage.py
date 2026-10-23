import copy
import unittest
from qualify_native_checkpoint_quality import bounded_quality, completed_summary, coverage, fp8_noninferiority
from native_quality_score import evaluate

class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.cases = [{'case_id': 'fact-0', 'category': 'fact', 'critical': True, 'expected_code': 'a'},
                      {'case_id': 'tool-0', 'category': 'tool_call', 'critical': True, 'expected_code': 'b'}]
        self.responses = [{'response': {'choices': [{'finish_reason': 'stop', 'message': {'content': 'a'}}]}},
                          {'response': {'choices': [{'finish_reason': 'tool_calls', 'message': {'content': '', 'tool_calls': [
                              {'id': 'call1', 'type': 'function', 'function': {'name': 'lookup_access_record', 'arguments': '{"access_code":"b"}'}}]}}]}}]
        self.checkpoints = [{'case_id': 'fact-0', 'metadata': {'outcome': 'compacted', 'global_savings_ratio': 1.7, 'validated_heads': 6}},
                            {'case_id': 'tool-0', 'metadata': {'outcome': 'unchanged_insufficient_savings', 'global_savings_ratio': 1., 'validated_heads': 0}}]
    def evaluate(self):
        return coverage(self.cases, self.responses, self.responses, self.checkpoints)
    def test_fallback_is_never_counted_as_compaction(self):
        result = self.evaluate()
        self.assertEqual(result['outcomes'], {'compacted': 1, 'unchanged_insufficient_savings': 1})
        self.assertFalse(result['every_category_exercised_compaction'])
        self.assertFalse(result['compacted_quality_passed'])
        self.assertEqual(result['categories']['tool_call']['compacted'], 0)
        self.assertEqual(result['categories']['fact']['compacted_resume_correct'], 1)
    def test_reordered_or_incomplete_evidence_rejected(self):
        self.checkpoints.reverse()
        with self.assertRaises(ValueError): self.evaluate()
        self.checkpoints.pop()
        with self.assertRaises(ValueError): self.evaluate()
    def test_false_savings_claim_rejected(self):
        for field, value in [('global_savings_ratio', 1.4), ('global_savings_ratio', float('nan')), ('validated_heads', 0)]:
            saved = copy.deepcopy(self.checkpoints)
            self.checkpoints[0]['metadata'][field] = value
            with self.assertRaises(ValueError): self.evaluate()
            self.checkpoints = saved
        self.checkpoints[1]['metadata']['validated_heads'] = 1
        with self.assertRaises(ValueError): self.evaluate()
    def test_wrong_compacted_answer_is_scored(self):
        self.responses[0]['response']['choices'][0]['message']['content'] = 'wrong'
        self.assertEqual(self.evaluate()['categories']['fact']['compacted_resume_correct'], 0)
    def test_fallback_improvement_cannot_hide_compacted_regression(self):
        cases = [dict(self.cases[0], case_id=f'fact-{i}') for i in range(2)]
        good = self.responses[0]
        bad = copy.deepcopy(good)
        bad['response']['choices'][0]['message']['content'] = 'wrong'
        checkpoints = [dict(self.checkpoints[0], case_id='fact-0'),
                       dict(self.checkpoints[1], case_id='fact-1')]
        result = coverage(cases, [good, bad], [bad, good], checkpoints)
        self.assertTrue(result['every_category_exercised_compaction'])
        self.assertEqual(result['compacted_drop_percentage_points'], 100)
        self.assertFalse(result['compacted_quality_passed'])
    def test_interrupted_run_reports_only_complete_pairs_without_passing(self):
        cases = [dict(c, body={'case': c['case_id']}) for c in self.cases]
        rows = [dict(r, fixture=c['case_id'], body=c['body'])
                for c, r in zip(cases, self.responses)]
        result = completed_summary(cases, rows, rows[:1], self.checkpoints)
        self.assertEqual(result['completed_cases'], 1)
        self.assertEqual(result['requested_cases'], 2)
        self.assertEqual(result['quality']['overall']['cases'], 1)
        self.assertFalse(result['quality']['passed'])
        self.assertEqual(result['coverage']['outcomes'], {'compacted': 1})
        self.assertEqual(completed_summary(cases, [], [], [])['completed_cases'], 0)
        with self.assertRaises(ValueError):
            completed_summary(cases, rows[:1], rows, self.checkpoints)

    def test_fp8_fallback_is_not_quantization_coverage(self):
        for checkpoint in self.checkpoints:
            metadata = checkpoint['metadata']
            metadata.update(encoding='fp8', fp8_heads=0, fp8_fallback_heads=metadata['validated_heads'])
        result = coverage(self.cases, self.responses, self.responses, self.checkpoints, encoding='fp8')
        self.assertFalse(result['every_category_exercised_fp8'])
        self.assertFalse(result['fp8_quality_passed'])
        self.checkpoints[0]['metadata']['fp8_heads'] = 5
        self.checkpoints[0]['metadata']['fp8_fallback_heads'] = 1
        result = self.evaluate()
        self.assertEqual(result['categories']['fact']['fp8_cases'], 1)
        self.assertFalse(result['fp8_quality_passed'])

    def test_fp8_regression_cannot_be_hidden_by_improvement(self):
        cases = [dict(self.cases[0], case_id=f'fact-{i}') for i in range(2)]
        good = self.responses[0]
        bad = copy.deepcopy(good)
        bad['response']['choices'][0]['message']['content'] = 'wrong'
        checkpoints = [dict(self.checkpoints[0], case_id=c['case_id'], metadata=dict(
            self.checkpoints[0]['metadata'], encoding='fp8', fp8_heads=5, fp8_fallback_heads=1)) for c in cases]
        result = coverage(cases, [good,bad], [bad,good], checkpoints, encoding='fp8')
        self.assertTrue(result['every_category_exercised_fp8'])
        self.assertFalse(result['fp8_quality_passed'])
        self.assertEqual(result['categories']['fact']['fp8_regression_case_ids'], ['fact-0'])

    def test_fp8_accounting_must_match_encoding_and_validated_heads(self):
        for changes in [dict(encoding='f16',fp8_heads=1), dict(encoding='fp8'),
                        dict(encoding='fp8',fp8_heads=7,fp8_fallback_heads=0),
                        dict(encoding='fp8',fp8_heads=True,fp8_fallback_heads=5),
                        dict(encoding='fp8',fp8_heads=-1,fp8_fallback_heads=7)]:
            saved = copy.deepcopy(self.checkpoints)
            self.checkpoints[0]['metadata'].update(changes)
            with self.assertRaises(ValueError): self.evaluate()
            self.checkpoints = saved
        with self.assertRaises(ValueError):
            coverage(self.cases, self.responses, self.responses, self.checkpoints, encoding='fp8')

    def test_fp8_noninferiority_separates_baseline_failures_without_waiving_coverage(self):
        cases, rows, checkpoints = [], [], []
        for category in ('fact', 'instruction', 'code', 'tool_call'):
            for i in range(16):
                fixture = 1 if category == 'tool_call' else 0
                case = dict(self.cases[fixture], case_id=f'{category}-{i}', category=category,
                            critical=i < 4, body={'case': f'{category}-{i}'})
                cases.append(case)
                rows.append(dict(copy.deepcopy(self.responses[fixture]), fixture=case['case_id'], body=case['body']))
                checkpoints.append({'case_id': case['case_id'], 'metadata': dict(
                    self.checkpoints[0]['metadata'], encoding='fp8', fp8_heads=5, fp8_fallback_heads=1)})
        rows[0]['response']['choices'][0]['message']['content'] = 'baseline error'
        after = copy.deepcopy(rows)
        result = fp8_noninferiority(cases, rows, after, checkpoints)
        self.assertTrue(result['passed'])
        self.assertEqual(result['reference_failure_case_ids'], ['fact-0'])
        self.assertFalse(evaluate(cases, rows, after)['passed'])
        self.assertFalse(fp8_noninferiority(cases[:4], rows[:4], after[:4], checkpoints[:4])['passed'])
        after[1]['response']['choices'][0]['message']['content'] = 'new error'
        self.assertFalse(fp8_noninferiority(cases, rows, after, checkpoints)['passed'])
        for checkpoint in checkpoints:
            checkpoint['metadata'].update(fp8_heads=0, fp8_fallback_heads=6)
        self.assertFalse(fp8_noninferiority(cases, rows, rows, checkpoints)['passed'])


    def bounded_fixture(self):
        cases, rows, checkpoints = [], [], []
        for category in ('fact', 'instruction', 'code', 'tool_call'):
            for i in range(4):
                fixture = 1 if category == 'tool_call' else 0
                case_id = f'{category}-{i}'
                case = dict(self.cases[fixture], case_id=case_id, category=category,
                            critical=True, body={'case': case_id})
                cases.append(case)
                rows.append(dict(copy.deepcopy(self.responses[fixture]), fixture=case_id, body=case['body']))
                checkpoints.append({'case_id': case_id, 'metadata': dict(
                    self.checkpoints[0]['metadata'], encoding='fp8', fp8_heads=5, fp8_fallback_heads=1)})
        return cases, rows, checkpoints

    def test_bounded_fp8_requires_complete_balanced_critical_cohort(self):
        cases, rows, checkpoints = self.bounded_fixture()
        self.assertTrue(bounded_quality(cases, rows, rows, checkpoints, rows)['passed'])
        self.assertTrue(fp8_noninferiority(cases, rows, rows, checkpoints, 'bounded')['passed'])
        self.assertFalse(fp8_noninferiority(cases, rows, rows, checkpoints)['passed'])
        self.assertFalse(bounded_quality(cases[:-1], rows[:-1], rows[:-1], checkpoints[:-1])['passed'])
        cases[0]['critical'] = False
        self.assertFalse(bounded_quality(cases, rows, rows, checkpoints)['passed'])
        self.assertFalse(fp8_noninferiority(cases, rows, rows, checkpoints, 'bounded')['passed'])
        cases[0]['critical'] = True
        cases[0]['category'] = 'instruction'
        self.assertFalse(bounded_quality(cases, rows, rows, checkpoints)['passed'])
        self.assertFalse(fp8_noninferiority(cases, rows, rows, checkpoints, 'bounded')['passed'])

    def test_bounded_fp8_reports_shared_model_failures_but_rejects_fitter_regressions(self):
        cases, rows, checkpoints = self.bounded_fixture()
        rows[0]['response']['choices'][0]['message']['content'] = 'model error'
        result = bounded_quality(cases, rows, rows, checkpoints, rows)
        self.assertTrue(result['passed'])
        self.assertEqual(result['baseline_failure_case_ids'], ['fact-0'])
        self.assertTrue(fp8_noninferiority(cases, rows, rows, checkpoints, 'bounded')['passed'])
        failed = copy.deepcopy(rows)
        failed[1]['response']['choices'][0]['message']['content'] = 'fitter error'
        self.assertTrue(fp8_noninferiority(cases, failed, failed, checkpoints, 'bounded')['passed'])
        self.assertFalse(bounded_quality(cases, rows, failed, checkpoints, failed)['passed'])
        # An FP8 improvement cannot hide a separately prepared FP16 regression.
        self.assertFalse(bounded_quality(cases, rows, rows, checkpoints, failed)['passed'])
        self.assertFalse(fp8_noninferiority(cases, rows, failed, checkpoints, 'bounded')['passed'])

    def test_bounded_fp8_requires_actual_compaction_and_codec_coverage(self):
        cases, rows, checkpoints = self.bounded_fixture()
        for checkpoint in checkpoints[:4]:
            checkpoint['metadata'].update(fp8_heads=0, fp8_fallback_heads=6)
        self.assertTrue(bounded_quality(cases, rows, rows, checkpoints, rows)['passed'])
        self.assertFalse(fp8_noninferiority(cases, rows, rows, checkpoints, 'bounded')['passed'])
        for checkpoint in checkpoints[:4]:
            checkpoint['metadata'].update(outcome='unchanged_insufficient_savings',
                global_savings_ratio=1, validated_heads=0, fp8_fallback_heads=0)
        self.assertFalse(bounded_quality(cases, rows, rows, checkpoints, rows)['passed'])
        self.assertFalse(fp8_noninferiority(cases, rows, rows, checkpoints, 'bounded')['passed'])

if __name__ == '__main__': unittest.main()
