import json
import math
from collections import Counter

def unique_object(pairs):
    out={}
    for key,value in pairs:
        if key in out: raise ValueError('duplicate tool argument')
        out[key]=value
    return out

def strict_json(text):
    def invalid(value):
        raise ValueError(f'nonfinite JSON value: {value}')
    def finite_float(text):
        value = float(text)
        if not math.isfinite(value):
            invalid(text)
        return value
    return json.loads(text, object_pairs_hook=unique_object, parse_constant=invalid, parse_float=finite_float)

def same_json(actual, expected):
    # Python considers True == 1; tool argument types are part of the contract.
    # JSON numbers may spell an integral value as either 1 or 1.0.
    if type(actual) in (int, float) and type(expected) in (int, float):
        return actual == expected
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(same_json(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(same_json(a, b) for a, b in zip(actual, expected))
    return actual == expected

def score(case, response):
    try:
        choices=response['choices']
        if len(choices)!=1:return False
        choice=choices[0];message=choice['message']
        expected = case.get('expected')
        if expected is None:
            expected = ({'kind': 'tool_call', 'name': 'lookup_access_record',
                         'arguments': {'access_code': case['expected_code']}}
                        if case['category'] == 'tool_call' else
                        {'kind': 'text', 'value': case['expected_code']})
        if expected['kind'] in ('text', 'json'):
            if choice.get('finish_reason')!='stop' or message.get('tool_calls') or not isinstance(message.get('content'),str):
                return False
            content = message['content'].strip()
            return content == expected['value'] if expected['kind'] == 'text' else same_json(strict_json(content), expected['value'])
        if expected['kind'] != 'tool_call':
            return False
        calls=message.get('tool_calls',[])
        if choice.get('finish_reason')!='tool_calls' or len(calls)!=1 or message.get('content') not in (None,''):return False
        call=calls[0];fn=call['function']
        if call.get('type')!='function' or not isinstance(call.get('id'),str) or not call['id'] or fn.get('name')!=expected['name']:return False
        return same_json(strict_json(fn['arguments']), expected['arguments'])
    except (KeyError,TypeError,ValueError):return False

def evaluate(cases, baseline, probe):
    if len(cases)!=len(baseline) or len(cases)!=len(probe):raise ValueError('incomplete quality evidence')
    if len({c['case_id'] for c in cases})!=len(cases):raise ValueError('duplicate case IDs')
    scores=[]
    for case,before,after in zip(cases,baseline,probe):
        if before['fixture']!=case['case_id'] or after['fixture']!=case['case_id'] or before['body']!=case['body'] or after['body']!=case['body']:raise ValueError('changed or reordered quality requests')
        scores.append({'case_id':case['case_id'],'category':case['category'],'critical':case['critical'],'baseline_correct':score(case,before['response']),'compacted_correct':score(case,after['response'])})
    def metrics(rows):
        n=len(rows);b=sum(r['baseline_correct'] for r in rows);p=sum(r['compacted_correct'] for r in rows)
        return {'cases':n,'baseline_correct':b,'compacted_correct':p,'drop_percentage_points':100*(b-p)/n}
    categories={key:metrics([r for r in scores if r['category']==key]) for key in sorted({r['category'] for r in scores})}
    overall=metrics(scores)
    counts=Counter(c['category'] for c in cases)
    count_met=set(counts)=={'fact','instruction','code','tool_call'} and all(n>=16 for n in counts.values())
    critical_pass=all(r['baseline_correct'] and r['compacted_correct'] for r in scores if r['critical']) and sum(r['critical'] for r in scores)>=16
    passed=count_met and critical_pass and overall['drop_percentage_points']<=2 and all(c['drop_percentage_points']<=5 for c in categories.values())
    return {'passed':passed,'sample_count_met':count_met,'critical_passed':critical_pass,'overall':overall,
            'categories':categories,'cases':scores,'production_ready':False,
            'baseline_failure_case_ids':[r['case_id'] for r in scores if not r['baseline_correct']],
            'regression_case_ids':[r['case_id'] for r in scores if r['baseline_correct'] and not r['compacted_correct']],
            'improvement_case_ids':[r['case_id'] for r in scores if not r['baseline_correct'] and r['compacted_correct']],
            'critical_failure_case_ids':[r['case_id'] for r in scores if r['critical'] and not (r['baseline_correct'] and r['compacted_correct'])]}
