import copy
import unittest
from compare_matched_latency import compare
from native_benchmark_fixture import MODEL_IDENTITY


def reports():
    common={'passed':True,'sampling_qualified':True,'smoke':False,'mode':'replay','size':'8k',
            'warmups_requested':5,'samples_requested':100,'fixture':{'identity':MODEL_IDENTITY,
            'logical_tokens':8189,'checkpoint_sha256_to_reverify':'a'*64,'token_bytes_sha256':'b'*64},
            'hardware':[{'name':'NVIDIA L4','uuid':'GPU-test','driver_version':'test'}]}
    native=copy.deepcopy(common);llama=copy.deepcopy(common)
    native.update(input_sha256={'model.gguf':'c'*64},request={'temperature':0,'max_tokens':16,'prompt_cache':False,'cache_dtype':'f16'},
                  worker={'server_kind':'standalone','stopped':True})
    llama.update(model_sha256='c'*64,request_settings={'temperature':0,'n_predict':16,'cache_prompt':False,'repeat_penalty':1},
                 process_group_stopped=True,command=['llama-server','-ctk','f16','-ctv','f16'])
    for report,seconds in [(native,1.2),(llama,1.)]:
        report['observations']=[]
        for index in range(106):
            row={'index':index,'passed':True,'cold':index==0,'warmup':0<index<=5,
                 'total_seconds':seconds,'first_content_seconds':seconds/2}
            if report is native:
                row['result']={'usage':{'prompt_tokens':8189,'completion_tokens':5},
                    'choices':[{'finish_reason':'stop','message':{'content':'violet-729'}}]}
            else:
                row['result']={'tokens_evaluated':8189,'tokens_predicted':5,'content':'violet-729',
                    'tokens':[68002,236772,236832,236778,236819],
                    'stop_type':'eos','truncated':False,'timings':{'prompt_n':8189,'cache_n':0,'predicted_n':5}}
            report['observations'].append(row)
    return native,llama


class ComparisonTests(unittest.TestCase):
    def test_eos_inclusive_llama_count_preserves_full_wall_latency(self):
        a,b=reports()
        for row in b['observations']:
            row['result']['tokens'].append(106)
            row['result']['tokens_predicted']=6
            row['result']['timings']['predicted_n']=6
        value=compare(a,b)
        self.assertEqual(value['output_tokens'],5)
        self.assertEqual(value['latency_seconds']['llama']['total_seconds']['p50'],1.)
        self.assertAlmostEqual(value['antfly_over_llama']['total_seconds']['p50'],1.2)
        self.assertEqual(b['observations'][0]['result']['tokens_predicted'],6)

    def test_bounded_profile_requires_explicit_matching_scope_and_complete_samples(self):
        a,b=reports()
        for report in (a,b):
            report.update(sampling_profile='bounded',samples_requested=20)
            report['observations']=report['observations'][:26]
        result=compare(a,b)
        self.assertEqual(result['samples'],{'antfly':20,'llama':20})
        self.assertEqual(result['sampling_profile'],'bounded')
        self.assertTrue(result['latency_thresholds_met'])
        b.pop('sampling_profile')
        with self.assertRaises(ValueError): compare(a,b)
        b['sampling_profile']='bounded'
        b['observations'].pop()
        with self.assertRaises(ValueError): compare(a,b)

    def test_matched_complete_campaign_meets_only_scoped_gate(self):
        a,b=reports();result=compare(a,b)
        self.assertTrue(result['latency_thresholds_met'])
        self.assertAlmostEqual(result['antfly_over_llama']['total_seconds']['p50'],1.2)
        self.assertFalse(result['production_ready'])
        self.assertFalse(result['portable_restore_comparison_qualified'])

    def test_recomputes_percentiles_and_preserves_slow_tail_failure(self):
        a,b=reports();a['latency_seconds']={'total_seconds':{'p50':.001,'p95':.001}}
        for row in a['observations'][-10:]:row['total_seconds']=1.6
        value=compare(a,b)
        self.assertFalse(value['latency_thresholds_met'])
        self.assertEqual(value['antfly_over_llama']['total_seconds']['p95'],1.6)
        self.assertEqual(value['antfly_over_llama']['total_seconds']['p50'],1.2)

    def test_rejects_smoke_missing_or_failed_samples(self):
        for mutation in [lambda r:r.update(smoke=True),lambda r:r['observations'].pop(),
                         lambda r:r['observations'][-1].update(passed=False),
                         lambda r:r['observations'][-1].update(index=0)]:
            a,b=reports();mutation(a)
            with self.assertRaises(ValueError):compare(a,b)

    def test_rejects_different_model_tokens_hardware_or_precision(self):
        for mutation in [lambda r:r.update(model_sha256='d'*64),
                         lambda r:r['fixture'].update(token_bytes_sha256='d'*64),
                         lambda r:r['hardware'][0].update(driver_version='other'),
                         lambda r:r['command'].__setitem__(-1,'q8_0')]:
            a,b=reports();mutation(b)
            with self.assertRaises(ValueError):compare(a,b)

    def test_rejects_portable_or_resident_workloads(self):
        for mode in ['portable_restore','resident']:
            a,b=reports();a['mode']=mode
            with self.assertRaisesRegex(ValueError,'full prompt replay'):compare(a,b)

    def test_rejects_hidden_cache_and_different_output_counts(self):
        a,b=reports();b['observations'][20]['result']['timings'].update(prompt_n=1,cache_n=8188)
        with self.assertRaisesRegex(ValueError,'reused'):compare(a,b)
        a,b=reports();b['observations'][20]['result'].update(tokens_predicted=6)
        b['observations'][20]['result']['timings']['predicted_n']=6
        b['observations'][20]['result']['tokens'].append(7)
        with self.assertRaisesRegex(ValueError,'output lengths'):compare(a,b)
        a,b=reports();a['observations'][20]['result']['usage']['cached_prompt_tokens']=8189
        with self.assertRaisesRegex(ValueError,'reused'):compare(a,b)


if __name__=='__main__':unittest.main()
