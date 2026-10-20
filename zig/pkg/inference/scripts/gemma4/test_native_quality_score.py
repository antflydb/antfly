import copy,json,unittest
from native_quality_score import score,evaluate
from prepare_native_quality_suite import build_cases
class QualityTests(unittest.TestCase):
    def setUp(self):
        self.cases=build_cases()
        self.requests=[]
        for case in self.cases:
            msg={'content':case['expected_code']};finish='stop'
            if case['category']=='tool_call':
                finish='tool_calls';msg={'content':None,'tool_calls':[{'id':'call-test','type':'function','function':{'name':'lookup_access_record','arguments':json.dumps({'access_code':case['expected_code']})}}]}
            self.requests.append({'fixture':case['case_id'],'body':case['body'],'response':{'choices':[{'message':msg,'finish_reason':finish}]}})
    def test_complete_and_count_gate(self):
        self.assertTrue(evaluate(self.cases,self.requests,self.requests)['passed'])
        self.assertFalse(evaluate(self.cases[:4],self.requests[:4],self.requests[:4])['passed'])
    def test_category_drop_and_critical_are_independent(self):
        changed=copy.deepcopy(self.requests);changed[10]['response']['choices'][0]['message']['content']='wrong'
        report=evaluate(self.cases,self.requests,changed)
        self.assertLess(report['overall']['drop_percentage_points'],2)
        self.assertEqual(report['categories']['fact']['drop_percentage_points'],6.25)
        self.assertFalse(report['passed'])
        changed=copy.deepcopy(self.requests);changed[0]['response']['choices'][0]['message']['content']='wrong'
        self.assertFalse(evaluate(self.cases,changed,changed)['critical_passed'])
    def test_body_and_order_cannot_change(self):
        changed=copy.deepcopy(self.requests);changed[0]['body']['max_tokens']=999
        with self.assertRaises(ValueError):evaluate(self.cases,self.requests,changed)
        with self.assertRaises(ValueError):evaluate(self.cases,self.requests,self.requests[::-1])
    def test_tool_structure_duplicate_arguments_and_wrong_values(self):
        case=self.cases[-1];good=self.requests[-1]['response']
        self.assertTrue(score(case,good))
        for mutation in ['duplicate','wrong_function','extra_key','text']:
            bad=copy.deepcopy(good);m=bad['choices'][0]['message'];fn=m['tool_calls'][0]['function']
            if mutation=='duplicate':fn['arguments']='{"access_code":"wrong","access_code":'+json.dumps(case['expected_code'])+'}'
            elif mutation=='wrong_function':fn['name']='wrong'
            elif mutation=='extra_key':fn['arguments']=json.dumps({'access_code':case['expected_code'],'extra':True})
            else:m['content']=case['expected_code']
            self.assertFalse(score(case,bad))
if __name__=='__main__':unittest.main()
