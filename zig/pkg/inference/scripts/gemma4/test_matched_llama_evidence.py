import copy
import io
import json
import time
import unittest

from qualify_matched_llama import percentiles,read_completion,validate_result


def result():
    return {'content':'violet-729','stop_type':'eos','truncated':False,
            'tokens_evaluated':8189,'tokens_predicted':5,
            'timings':{'prompt_n':8189,'cache_n':0,'predicted_n':5,'prompt_ms':2000.,'predicted_ms':100.}}


class MatchedLlamaEvidenceTests(unittest.TestCase):
    def test_replay_cannot_hide_cached_prefix(self):
        value=result();validate_result(value,8189,5,'violet-729','replay')
        value['timings'].update(prompt_n=1,cache_n=8188)
        with self.assertRaisesRegex(AssertionError,'cached work'):
            validate_result(value,8189,5,'violet-729','replay')
        validate_result(value,8189,5,'violet-729','resident')

    def test_resident_cannot_count_full_replay(self):
        with self.assertRaisesRegex(AssertionError,'did not reuse'):
            validate_result(result(),8189,5,'violet-729','resident')

    def test_wrong_prompt_output_or_truncation_fails(self):
        for changes in [{'tokens_evaluated':8190},{'tokens_predicted':6},
                        {'truncated':True},{'content':'violet-728'},{'stop_type':'limit'}]:
            value=result();value.update(changes)
            with self.subTest(changes=changes),self.assertRaises(AssertionError):
                validate_result(value,8189,5,'violet-729','replay')

    def test_invalid_or_unmeasured_engine_phase_fails(self):
        for timing in [float('nan'),float('inf'),-1.,0.]:
            value=result();value['timings']['predicted_ms']=timing
            with self.subTest(timing=timing),self.assertRaises(AssertionError):
                validate_result(value,8189,5,'violet-729','replay')
        value=result();value['timings'].update(prompt_n=0,cache_n=8189,prompt_ms=0.)
        validate_result(value,8189,5,'violet-729','resident')
        self.assertEqual(percentiles([0.,0.],allow_zero=True)['p95'],0.)

    def test_stream_preserves_final_accounting_and_first_token(self):
        final=result();final.update(stop=True,content='',tokens=[])
        chunks=[{'index':0,'stop':False,'content':'violet','tokens':[1]},
                {'index':0,'stop':False,'content':'-729','tokens':[2,3,4]},final]
        wire=b''.join(('data: '+json.dumps(row)+'\n\n').encode() for row in chunks)
        observation=read_completion(io.BytesIO(wire),time.monotonic())
        self.assertEqual(observation['result']['content'],'violet-729')
        self.assertEqual(observation['result']['tokens'],[1,2,3,4])
        self.assertGreater(observation['ttft_seconds'],0)
        validate_result(observation['result'],8189,5,'violet-729','replay')

    def test_done_without_accounting_or_unstreamed_result_fails(self):
        for wire in [b'data: [DONE]\n\n',b'data: {"stop":true,"content":"violet-729"}\n\n']:
            with self.subTest(wire=wire),self.assertRaises(AssertionError):
                read_completion(io.BytesIO(wire),time.monotonic())


if __name__=='__main__':unittest.main()
