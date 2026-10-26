#!/usr/bin/env python3
"""One-seed diagnostic of the Python outer API over the reviewed dynamic native CG CLI."""
from __future__ import annotations
import argparse,hashlib,importlib.util,json,os,time
from pathlib import Path
import numpy as np

def sha(p):
 h=hashlib.sha256()
 with Path(p).open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def edge(p):p=Path(p).resolve(strict=True);return {'path':str(p),'sha256':sha(p),'bytes':p.stat().st_size}
def req(x,m):
 if not x:raise ValueError(m)
def write(path,value):
 data=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode();fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 try:
  v=memoryview(data)
  while v:n=os.write(fd,v);req(n>0,'short write');v=v[n:]
  os.fsync(fd)
 finally:os.close(fd)
def load_module(path,digest):
 req(sha(path)==digest,'adapter changed');s=importlib.util.spec_from_file_location('native_cg_adapter_bound',path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def main():
 p=argparse.ArgumentParser();p.add_argument('--adapter',type=Path,required=True);p.add_argument('--adapter-sha256',required=True);p.add_argument('--fixture',type=Path,required=True);p.add_argument('--fixture-sha256',required=True);p.add_argument('--oracle',type=Path,required=True);p.add_argument('--oracle-sha256',required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--scratch',type=Path,required=True);a=p.parse_args()
 req(not a.output.exists() and not a.output.is_symlink() and a.output.parent.is_dir(),'output not fresh');req(a.scratch.is_absolute() and not a.scratch.exists() and not a.scratch.is_symlink() and a.scratch.parent.is_dir(),'scratch not fresh');a.scratch.mkdir(mode=0o700);os.environ['ANTFLY_NATIVE_CG_SCRATCH_ROOT']=str(a.scratch)
 fe=edge(a.fixture);oe=edge(a.oracle);req(fe['sha256']==a.fixture_sha256 and oe['sha256']==a.oracle_sha256,'input edge changed')
 fixture=json.loads(a.fixture.read_text());oracle=json.loads(a.oracle.read_text());req(fixture.get('schema')=='antfly.attention-matching.mass-full-cg-fixture.v2' and fixture.get('completed') is True and fixture.get('fit_invoked') is False and fixture.get('held_tensors_loaded') is False,'invalid fixture')
 req(oracle.get('schema')=='antfly.attention-matching.mass-python-full-cg-oracle.v2' and oracle.get('completed') is True and oracle.get('fixture')=={'path':fe['path'],'sha256':fe['sha256']} and oracle.get('warm_rows') is None,'invalid oracle')
 me=fixture['matrix'];mp=a.fixture.parent/me['path'];req(edge(mp)=={'path':str(mp),'sha256':me['sha256'],'bytes':me['bytes']} and me['shape']==[3000,288] and me['dtype']=='float64-le','matrix changed')
 ae=oracle['terminal']['arrays'];ap=Path(ae['path']);req(edge(ap)==ae,'oracle arrays changed')
 matrix=np.fromfile(mp,dtype='<f8').reshape(me['shape']);adapter=load_module(a.adapter,a.adapter_sha256)
 started=time.monotonic();result=adapter.solve_mass_lp(matrix,560.);elapsed=time.monotonic()-started
 req(result.get('accepted') is True and type(result.get('solution')) is dict,'native adapter did not accept')
 solution=result['solution'];cg=solution['constraint_generation'];expected_rounds=oracle['rounds']
 req(len(cg['rounds'])==len(expected_rounds) and all(x['added_rows']==y['added_rows'] for x,y in zip(cg['rounds'],expected_rounds)),'CG row trajectory changed')
 req(cg['selected_row_indices']==oracle['terminal']['selected_row_indices'] and solution['iterations']==oracle['terminal']['iterations'],'terminal selection/iterations changed')
 req(solution['objective']==oracle['terminal']['objective'] and cg['full_witness']['t']==oracle['terminal']['full_witness']['t'] and cg['full_witness']['objective']==oracle['terminal']['full_witness']['objective'],'terminal objective/t changed')
 req(solution['constraint_generation']['full_witness']['all_checks_passed'] is True and solution['constraint_generation']['stored_f32_log_bias_mass']['available'] is True,'Python audit failed')
 hashes={}
 with np.load(ap,allow_pickle=False) as z:
  for name in ('raw_weights','bias','stored_weights','mass','alpha','beta'):
   expected=np.ascontiguousarray(z[name]);actual=np.ascontiguousarray(solution[name]);req(expected.dtype==actual.dtype and expected.shape==actual.shape and expected.tobytes()==actual.tobytes(),'outer API array changed: '+name);hashes[name]=hashlib.sha256(actual.tobytes()).hexdigest()
 req(hashes==oracle['terminal']['array_hashes'],'oracle array hashes changed')
 req(edge(a.fixture)==fe and edge(a.oracle)==oe and sha(a.adapter)==a.adapter_sha256,'inputs changed during adapter call')
 life=cg['subprocess_lifecycle'];req(life['worker_same_process_group'] is True and life['worker_drained'] is True,'native lifecycle incomplete')
 write(a.output,{'schema':'antfly.attention-matching.native-dynamic-cg-outer-api-equivalence.v1','completed':True,'diagnostic_only':True,'execution_authority':False,'publication_authority':False,'fixture':fe,'oracle':oe,'adapter':edge(a.adapter),'accepted':True,'exact_round_row_trajectory':True,'exact_selected_rows':True,'exact_total_iterations':True,'exact_python_api_array_hashes':hashes,'full_witness_all_checks_passed':True,'stored_f32_log_bias_mass_available':True,'subprocess_lifecycle':life,'elapsed_seconds':elapsed,'scope':{'single_original_seed_owner14_block2048':True,'full_fitter':False,'held_data':False,'serving_latency':False,'persistent_basis_reuse':False,'production_qualification':False}})
if __name__=='__main__':main()
