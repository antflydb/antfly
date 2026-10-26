#!/usr/bin/env python3
from __future__ import annotations
import importlib.util,json,os,tempfile,time
from pathlib import Path
from unittest.mock import patch
import numpy as np
HERE=Path(__file__).parent
P=HERE/'attention_mass_native_cg_subprocess_adapter_v2.py'
spec=importlib.util.spec_from_file_location('native_adapter',P);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def emit(root,a,accepted=True,bad_rows=False):
    out=root/'output';out.mkdir()
    rows,cols=a.shape;raw=np.full(cols,1./cols,np.float64);bias=np.asarray(np.log(raw),np.float32);stored=np.exp(bias.astype(np.float64));mass=a@stored
    vals={'raw_weights':raw,'bias':bias,'stored_weights':stored,'mass':mass,'alpha':np.zeros(rows),'beta':np.zeros(rows),'selected_rows':np.arange(min(512,rows),dtype=np.uint64)};edges={}
    for name,v in vals.items():
        p=out/(name+'.bin');p.write_bytes(v.tobytes());edges[name]={'path':p.name,'sha256':m.sha(p),'bytes':p.stat().st_size,'count':len(v),'dtype':m.ARRAYS[name][0]}
    rounds=[{'round':0,'subset_rows':512,'iterations':7,'cumulative_iterations':7,'objective':0.,'worst_full_violation':0.,'added_row_count':0 if not bad_rows else 1,'added_rows':[],'retried_witness':False}]
    return out,edges,rounds

def receipt_for(cmd,edges,rounds,accepted=True):
    mp=Path(cmd[cmd.index('--input-manifest')+1]);lib=Path(cmd[cmd.index('--library-receipt')+1]);exe=Path(cmd[cmd.index('--executable-receipt')+1]);manifest=json.loads(mp.read_text())
    d={'rounds':rounds,'round_count':len(rounds),'total_iterations':7,'full_rows':manifest['query_rows'],'selected_rows':512,'warm_hint_rows':0,'initial_selected_rows':512}
    base={'schema':m.RESULT_SCHEMA,'completed':True,'diagnostic_only':True,'execution_authority':False,'publication_authority':False,'input':m.edge(mp),'library_receipt':m.edge(lib),'executable_receipt':m.edge(exe),'request_sha256':manifest['request_sha256'],'diagnostics':d,'lifecycle':{'creates_attempted':1,'creates_succeeded':1,'destroys':1,'passes':1,'runs':1,'solutions':1,'option_sets':8,'callbacks_registered':1,'callbacks_started':1},'attempts':[{}]}
    if accepted:base.update(status='accepted',rollback_required=False,objective=0.,arrays=edges)
    else:base.update(status='terminal',rollback_required=True,terminal_reason='deadline_exhausted',arrays=None)
    return base

class FakeProcess:
    next_pid=200000
    def __init__(self,cmd,*args,terminal=False,bad_rows=False,timeout=False,**kwargs):
        self.cmd=cmd;self.pid=FakeProcess.next_pid;FakeProcess.next_pid+=1;self.returncode=None;self.timeout=timeout
        mp=Path(cmd[cmd.index('--input-manifest')+1]);a=np.fromfile(mp.parent/'matrix.f64le',dtype='<f8').reshape(json.loads(mp.read_text())['matrix']['shape'])
        out,edges,rounds=emit(mp.parent,a,bad_rows=bad_rows)
        (out/'result.json').write_text(json.dumps(receipt_for(cmd,edges,rounds,not terminal))+'\n')
    def wait(self,timeout=None):
        if self.timeout and self.returncode is None:raise m.subprocess.TimeoutExpired(self.cmd,timeout)
        if self.returncode is None:self.returncode=0
        return self.returncode
    def poll(self):return self.returncode
    def terminate(self):self.returncode=-15
    def kill(self):self.returncode=-9

def cfg(root):
    binary=root/'native';binary.write_text('fake')
    lib=root/'library.json';lib.write_text('{}')
    src=root/'source.json';src.write_text('{}')
    exe=root/'executable.json'
    le=m.edge(lib);be=m.edge(binary);exe.write_text(json.dumps({'completed':True,'binary':be,'library_receipt':le}))
    return {'binary':binary,'library':lib,'source':src,'executable':exe,'scratch':root,'edges':{'binary':be,'library':le,'source':m.edge(src),'executable':m.edge(exe)}}

def test_success_and_exact_rows():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);c=cfg(root);a=np.full((912,288),1.,np.float64)
        with patch.object(m,'config',return_value=c),patch.object(m.subprocess,'Popen',side_effect=lambda *x,**k:FakeProcess(*x,**k)),patch.object(m.os,'getpgid',return_value=os.getpgrp()):r=m.solve_mass_lp(a,10.)
        assert r['accepted'] and r['solution']['constraint_generation']['rounds'][0]['added_rows']==[]
        assert r['solution']['iterations']==7 and r['diagnostics']['subprocess_lifecycle']['worker_drained'] is True

def test_terminal_never_promotes_candidate():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);c=cfg(root);a=np.ones((912,288))
        with patch.object(m,'config',return_value=c),patch.object(m.subprocess,'Popen',side_effect=lambda *x,**k:FakeProcess(*x,terminal=True,**k)),patch.object(m.os,'getpgid',return_value=os.getpgrp()):r=m.solve_mass_lp(a,10.)
        assert r['accepted'] is False and r['solution'] is None and r['diagnostics']['termination']=='deadline_exhausted'

def test_row_count_mismatch_rejects():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);c=cfg(root);a=np.ones((912,288))
        try:
            with patch.object(m,'config',return_value=c),patch.object(m.subprocess,'Popen',side_effect=lambda *x,**k:FakeProcess(*x,bad_rows=True,**k)),patch.object(m.os,'getpgid',return_value=os.getpgrp()):m.solve_mass_lp(a,10.)
        except ValueError as e:assert 'added row identities' in str(e)
        else:raise AssertionError('row trace mismatch accepted')

def test_timeout_drains_and_rolls_back():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);c=cfg(root);a=np.ones((912,288))
        with patch.object(m,'config',return_value=c),patch.object(m.subprocess,'Popen',side_effect=lambda *x,**k:FakeProcess(*x,timeout=True,**k)),patch.object(m.os,'getpgid',return_value=os.getpgrp()):r=m.solve_mass_lp(a,.01)
        assert r['accepted'] is False and r['solution'] is None and r['diagnostics']['reason']=='worker_timeout' and r['diagnostics']['subprocess_lifecycle']['worker_drained'] is True

def test_request_digest_matches_real_manifest_builder():
    import argparse
    bp=HERE/'prepare_attention_mass_dynamic_cg_input_v1.py';bs=importlib.util.spec_from_file_location('builder',bp);b=importlib.util.module_from_spec(bs);bs.loader.exec_module(b)
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);matrix=root/'matrix.f64le';matrix.write_bytes(np.zeros((912,288),dtype='<f8').tobytes());warm=root/'warm.u64le';warm.write_bytes(np.array([0,17,911],dtype='<u8').tobytes());source=root/'source.json';source.write_text('{}');out=root/'input.json'
        value=b.build(argparse.Namespace(query_rows=912,retained_rows=288,matrix=matrix,warm_rows=warm,source=source,output=out))
        assert m.request_digest(value)==value['request_sha256']

if __name__=='__main__':
    for f in (test_success_and_exact_rows,test_terminal_never_promotes_candidate,test_row_count_mismatch_rejects,test_timeout_drains_and_rolls_back,test_request_digest_matches_real_manifest_builder):f()
    print('5/5')
