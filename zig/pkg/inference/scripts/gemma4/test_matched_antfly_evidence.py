import copy
import io
import json
import time
import unittest

from qualify_matched_antfly import read_timed_response,reference_result,validate_observation


def result(cached=False):
    usage={'prompt_tokens':8189,'completion_tokens':5,'total_tokens':8194}
    if cached:usage['cached_prompt_tokens']=8189
    return {'choices':[{'index':0,'message':{'role':'assistant','content':'violet-729'},'finish_reason':'stop'}],
            'usage':usage}


def event(value):return ('data: '+json.dumps(value)+'\n\n').encode()


class MatchedAntflyEvidenceTests(unittest.TestCase):
    def test_content_timing_stops_at_done_without_waiting_for_eof(self):
        wire=(event({'choices':[{'index':0,'delta':{'role':'assistant'}}]})+
              event({'choices':[{'index':0,'delta':{'content':'violet-729'},'finish_reason':'stop'}]})+
              event({'choices':[],'usage':result()['usage']})+b'data: [DONE]\n\nnot EOF yet')
        stream=io.BytesIO(wire)
        row=read_timed_response(stream,time.monotonic())
        validate_observation(row,result(),8189,'replay')
        self.assertEqual(stream.read(),b'\nnot EOF yet')
        self.assertGreater(row['first_content_seconds'],0)
        self.assertGreaterEqual(row['first_content_to_done_seconds'],0)

    def test_terminal_or_role_only_events_do_not_supply_content_timing(self):
        wire=event({'choices':[{'index':0,'delta':{'role':'assistant'},'finish_reason':'stop'}],
                    'usage':result()['usage']})+b'data: [DONE]\n\n'
        with self.assertRaisesRegex(AssertionError,'first-content'):
            read_timed_response(io.BytesIO(wire),time.monotonic())
        with self.assertRaisesRegex(AssertionError,'without DONE'):
            read_timed_response(io.BytesIO(event({'choices':[{'delta':{'content':'hi'}}]})),time.monotonic())

    def test_replay_cannot_count_restored_prefix_as_fresh_work(self):
        row={'result':result(True),'first_content_seconds':1.,'total_seconds':2.}
        validate_observation(row,result(True),8189,'portable_restore')
        with self.assertRaisesRegex(AssertionError,'prefix reuse'):
            validate_observation(row,result(True),8189,'replay')
        row['result']=result()
        with self.assertRaisesRegex(AssertionError,'prefix reuse'):
            validate_observation(row,result(),8189,'portable_restore')

    def test_invalid_output_accounting_and_timings_fail(self):
        row={'result':result(),'first_content_seconds':1.,'total_seconds':2.}
        for field,value in [('first_content_seconds',float('nan')),('total_seconds',.5)]:
            changed=copy.deepcopy(row);changed[field]=value
            with self.subTest(field=field),self.assertRaisesRegex(AssertionError,'timings'):
                validate_observation(changed,result(),8189,'replay')
        changed=copy.deepcopy(row);changed['result']['usage']['completion_tokens']=6
        with self.assertRaisesRegex(AssertionError,'accounting'):
            validate_observation(changed,result(),8189,'replay')
        changed=copy.deepcopy(row);changed['result']['choices'][0]['message']['content']='wrong'
        with self.assertRaisesRegex(AssertionError,'output'):
            validate_observation(changed,result(),8189,'replay')

    def test_selects_exact_baseline_and_rejects_missing_or_ambiguous_reference(self):
        longer=result();longer['usage']['prompt_tokens']=9626
        reference={'exporter':{'requests':[{'kind':'baseline','result':longer},{'kind':'baseline','result':result()}]}}
        self.assertEqual(reference_result(reference,'baseline',8189),result())
        with self.assertRaisesRegex(AssertionError,'missing/ambiguous'):
            reference_result(reference,'baseline',32752)
        reference['exporter']['requests'].append({'kind':'baseline','result':result()})
        with self.assertRaisesRegex(AssertionError,'missing/ambiguous'):
            reference_result(reference,'baseline',8189)


if __name__=='__main__':unittest.main()
