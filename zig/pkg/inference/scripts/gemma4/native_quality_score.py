import json
from collections import Counter

def unique_object(pairs):
    out={}
    for key,value in pairs:
        if key in out: raise ValueError('duplicate tool argument')
        out[key]=value
    return out

def score(case, response):
    try:
        choices=response['choices']
        if len(choices)!=1:return False
        choice=choices[0];message=choice['message']
        if case['category']!='tool_call':
            return (choice.get('finish_reason')=='stop' and not message.get('tool_calls') and
                    isinstance(message.get('content'),str) and message['content'].strip()==case['expected_code'])
        calls=message.get('tool_calls',[])
        if choice.get('finish_reason')!='tool_calls' or len(calls)!=1 or message.get('content') not in (None,''):return False
        call=calls[0];fn=call['function']
        if call.get('type')!='function' or not isinstance(call.get('id'),str) or not call['id'] or fn.get('name')!='lookup_access_record':return False
        return json.loads(fn['arguments'],object_pairs_hook=unique_object)=={'access_code':case['expected_code']}
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
    return {'passed':passed,'sample_count_met':count_met,'critical_passed':critical_pass,'overall':overall,'categories':categories,'cases':scores,'production_ready':False}
