import copy
import unittest
from qualify_native_checkpoint_quality import coverage

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

if __name__ == '__main__': unittest.main()
