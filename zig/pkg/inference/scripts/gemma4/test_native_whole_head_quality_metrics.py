# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import copy,math,unittest
import native_whole_head_quality_metrics as m

def row(i,rep=0):
 return {'id':f'case-{i}-{rep}','blueprint_id':f'blueprint-{i}','primary_family':f'family-{i}','context_tokens':8192 if rep==0 else 32768,'bucket':'code','exact_correct':True,'candidate_correct':True,'scored_tokens':2,'exact_nll_sum':2.,'candidate_nll_sum':2.}
class Metrics(unittest.TestCase):
 def test_cluster_resampling_does_not_count_repeated_contexts_as_independent(self):
  rows=[]
  for i in range(100):
   for rep in range(10):
    r=row(i,rep);r['candidate_correct']=i>=10;rows.append(r)
  r=m.paired_metrics(rows,'correlated',1000)
  self.assertEqual(r['primary_source_clusters'],100);self.assertAlmostEqual(r['accuracy_drop'],.1);self.assertGreaterEqual(r['accuracy_drop_upper95'],.14)
 def test_duplicating_whole_cohort_cannot_narrow_confidence(self):
  base=[row(i) for i in range(100)]
  for r in base[:10]:r['candidate_correct']=False
  repeated=base+copy.deepcopy(base)
  a=m.paired_metrics(base,'fixed',1000);b=m.paired_metrics(repeated,'fixed',1000)
  self.assertEqual(a['accuracy_drop_upper95'],b['accuracy_drop_upper95'])
  self.assertEqual(m.paired_metrics(list(reversed(base)),'fixed',1000)['accuracy_drop_upper95'],a['accuracy_drop_upper95'])
 def test_nll_is_token_weighted_and_baseline_failures_remain(self):
  rows=[row(i) for i in range(100)]
  rows[0].update(exact_correct=False,candidate_correct=False,scored_tokens=200,candidate_nll_sum=4.)
  r=m.paired_metrics(rows,'tokens',100)
  self.assertEqual(r['cases'],100);self.assertEqual(r['exact_correct'],99);self.assertAlmostEqual(r['nll_delta_per_token'],2/398)
 def test_one_family_cannot_supply_one_hundred_independent_cases(self):
  rows=[row(i) for i in range(100)]
  for r in rows:r['primary_family']='one'
  with self.assertRaisesRegex(ValueError,'independent'):m.paired_metrics(rows,'bad',100)
 def test_invalid_observations_rejected(self):
  for field,value in [('exact_correct',1),('scored_tokens',True),('candidate_nll_sum',float('nan')),('exact_nll_sum',-1.)]:
   rows=[row(i) for i in range(100)];rows[0][field]=value
   with self.assertRaises(ValueError):m.paired_metrics(rows,'bad',100)
 def test_coverage_and_failures_preserved_in_complete_cohort(self):
  rows=[]
  for b in sorted(m.BUCKETS):
   for i in range(100):
    for rep in range(2):
     r=row(i,rep);r['blueprint_id']=b+r['blueprint_id'];r['primary_family']=b+r['primary_family'];r['id']=b+r['id'];r['bucket']=b;rows.append(r)
  rows[0].update(exact_correct=False,candidate_correct=False)
  r=m.evaluate(rows,'complete',100);self.assertTrue(r['passed']);self.assertEqual(r['baseline_failure_case_ids'],[rows[0]['id']]);self.assertFalse(r['release_authority'])
  with self.assertRaises(ValueError):m.evaluate(rows[:-1],'incomplete',100)
if __name__=='__main__':unittest.main()
