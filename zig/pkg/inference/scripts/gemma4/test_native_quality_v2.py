import copy
import json
import unittest

from native_quality_score import evaluate, same_json, score, strict_json
from prepare_native_quality_suite_v2 import build_cases, task


def response_for(case):
    expected = case['expected']
    if expected['kind'] == 'tool_call':
        return {'choices': [{'finish_reason': 'tool_calls', 'message': {'content': None,
            'tool_calls': [{'id': 'call-fixture', 'type': 'function', 'function': {
                'name': expected['name'], 'arguments': json.dumps(expected['arguments'])}}]}}]}
    content = expected['value'] if expected['kind'] == 'text' else json.dumps(expected['value'])
    return {'choices': [{'finish_reason': 'stop', 'message': {'content': content}}]}


class QualityV2Tests(unittest.TestCase):
    def setUp(self):
        self.cases = build_cases()

    def test_fixed_suite_covers_families_positions_and_critical_cases(self):
        self.assertEqual(self.cases, build_cases())
        self.assertEqual(len({c['case_id'] for c in self.cases}), 64)
        self.assertEqual(sum(c['critical'] for c in self.cases), 16)
        for category in ('fact', 'instruction', 'code', 'tool_call'):
            cases = [c for c in self.cases if c['category'] == category]
            self.assertEqual(len(cases), 16)
            for family in range(4):
                self.assertEqual({c['data_position_fraction'] for c in cases if c['task_family'] == family},
                                 {.10, .25, .45, .60})
        for case in self.cases:
            prefix = case['checkpoint_body']
            self.assertEqual(case['body']['messages'][:-2], prefix['messages'])
            self.assertNotIn(case['body']['messages'][-1]['content'], prefix['messages'][-1]['content'])
            self.assertEqual(case['body'].get('tools'), prefix.get('tools'))
            self.assertNotIn('tool_calls', case['body']['messages'][-2])

    def test_oracle_responses_and_critical_failure(self):
        rows = [{'fixture': c['case_id'], 'body': c['body'], 'response': response_for(c)} for c in self.cases]
        self.assertTrue(evaluate(self.cases, rows, rows)['passed'])
        altered = copy.deepcopy(rows)
        altered[0]['response']['choices'][0]['finish_reason'] = 'length'
        self.assertFalse(evaluate(self.cases, rows, altered)['passed'])

    def test_code_answer_keys_agree_with_python_execution(self):
        # Execute only snippets authored by this deterministic fixture generator,
        # never model output or a user-supplied suite.
        for index in range(16):
            _, data, _, expected, _ = task('code', index)
            namespace = {}
            exec(compile(data.removeprefix('Python:\n'), f'<code-fixture-{index}>', 'exec'), namespace)
            actual = namespace['result']
            if expected['kind'] == 'json':
                self.assertTrue(same_json(actual, expected['value']), index)
            else:
                self.assertEqual(str(actual), expected['value'], index)

    def test_json_rejects_duplicate_keys_nonfinite_values_and_boolean_integer_aliases(self):
        case = {'expected': {'kind': 'json', 'value': {'count': 1}}}
        response = {'choices': [{'finish_reason': 'stop', 'message': {'content': '{"count": 1}'}}]}
        self.assertTrue(score(case, response))
        response['choices'][0]['message']['content'] = '{"count": 1.0}'
        self.assertTrue(score(case, response))
        for content in ('{"count":true}', '{"count":0,"count":1}',
                        '{"count":NaN}', '{"count":Infinity}', '{"count":1e309}',
                        '{"count":1,"extra":0}', '```json\n{"count":1}\n```'):
            response['choices'][0]['message']['content'] = content
            self.assertFalse(score(case, response), content)
        for content in ('NaN', 'Infinity', '-Infinity', '1e309', '-1e309'):
            with self.assertRaises(ValueError):
                strict_json(content)

    def test_nested_tool_arguments_and_arrays_are_strict(self):
        for case in (c for c in self.cases[:16] if c['category'] == 'tool_call'):
            good = response_for(case)
            self.assertTrue(score(case, good))
            bad = copy.deepcopy(good)
            fn = bad['choices'][0]['message']['tool_calls'][0]['function']
            args = json.loads(fn['arguments'])
            args['extra'] = True
            fn['arguments'] = json.dumps(args)
            self.assertFalse(score(case, bad))
            bad = copy.deepcopy(good)
            bad['choices'][0]['message']['tool_calls'].append(copy.deepcopy(good['choices'][0]['message']['tool_calls'][0]))
            self.assertFalse(score(case, bad))
        nested = self.cases[15]
        bad = response_for(nested)
        fn = bad['choices'][0]['message']['tool_calls'][0]['function']
        args = json.loads(fn['arguments'])
        args['options']['dry_run'] = 1
        fn['arguments'] = json.dumps(args)
        self.assertFalse(score(nested, bad))


if __name__ == '__main__':
    unittest.main()
