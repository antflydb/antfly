#!/usr/bin/env python3
"""Diagnostic native-CG subprocess adapter preserving the frozen Python call/result API."""
from __future__ import annotations
import hashlib, json, math, os, signal, subprocess, tempfile, time
from pathlib import Path
import numpy as np

SCHEMA='antfly.attention-matching.native-cg-subprocess-adapter.v1'
INPUT_SCHEMA='antfly.attention-matching.mass-dynamic-cg-input.v1'
RESULT_SCHEMA='antfly.attention-matching.mass-dynamic-cg-result.v2'
ARRAYS={'raw_weights':('float64-le',np.dtype('<f8')),'bias':('float32-le',np.dtype('<f4')),'stored_weights':('float64-le',np.dtype('<f8')),'mass':('float64-le',np.dtype('<f8')),'alpha':('float64-le',np.dtype('<f8')),'beta':('float64-le',np.dtype('<f8')),'selected_rows':('uint64-le',np.dtype('<u8'))}
SEED_ROWS=512; ADD_ROWS=256; MAX_ROUNDS=12
WEIGHT_MIN=1e-6; WEIGHT_MAX=1e6; WITNESS_REL_TOL=1e-6; COMPLEMENTARITY_REL_TOL=1e-5; STATIONARITY_REL_TOL=1e-5

def req(v,m):
    if not v: raise ValueError(m)
def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1<<20),b''): h.update(b)
    return h.hexdigest()
def edge(path):
    p=Path(path).resolve(strict=True);req(p.is_file() and not p.is_symlink(),'invalid file edge')
    return {'path':str(p),'sha256':sha(p),'bytes':p.stat().st_size}
def write_exclusive(path,data):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        v=memoryview(data)
        while v:
            n=os.write(fd,v);req(n>0,'short write');v=v[n:]
        os.fsync(fd)
    finally: os.close(fd)
def write_json(path,value): write_exclusive(path,(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode())
def owner(root,name):
    p=(root/name).resolve();req(p.parent==root.resolve() and p.name==name,'invalid owner-relative path');return p

def config():
    names={'binary':'ANTFLY_NATIVE_CG_BINARY','executable':'ANTFLY_NATIVE_CG_EXECUTABLE_RECEIPT','library':'ANTFLY_NATIVE_CG_LIBRARY_RECEIPT','source':'ANTFLY_NATIVE_CG_SOURCE_RECEIPT','scratch':'ANTFLY_NATIVE_CG_SCRATCH_ROOT'}
    c={k:Path(os.environ.get(v,'')) for k,v in names.items()}
    req(all(str(x) and x.is_absolute() for x in c.values()),'missing native CG configuration')
    for k in ('binary','executable','library','source'): req(c[k].is_file() and not c[k].is_symlink(),f'invalid {k}')
    req(c['scratch'].is_dir() and not c['scratch'].is_symlink(),'invalid scratch root')
    c['edges']={k:edge(c[k]) for k in ('binary','executable','library','source')}
    exe=json.loads(c['executable'].read_text())
    req(exe.get('completed') is True and exe.get('binary')==c['edges']['binary'] and exe.get('library_receipt')==c['edges']['library'],'executable closure mismatch')
    return c

def request_digest(m):
    value={'schema':m['schema'],'query_rows':m['query_rows'],'retained_rows':m['retained_rows'],'matrix':m['matrix'],'warm_rows':m['warm_rows'],'source':m['source']}
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def drain(p,grace=.5):
    if p.poll() is None:
        p.terminate()
        try:p.wait(timeout=grace)
        except subprocess.TimeoutExpired:p.kill();p.wait(timeout=grace)
    return p.returncode

def load_vector(root,spec,dtype,count):
    req(type(spec) is dict and spec.get('dtype')==ARRAYS[dtype][0] and spec.get('count')==count and spec.get('bytes')==count*ARRAYS[dtype][1].itemsize,'invalid vector metadata')
    p=owner(root,spec.get('path'));req(p.is_file() and not p.is_symlink() and sha(p)==''.join(spec.get('sha256')),'vector changed')
    raw=p.read_bytes();req(len(raw)==spec['bytes'],'vector size changed')
    v=np.frombuffer(raw,dtype=ARRAYS[dtype][1]).copy();req(np.isfinite(v).all(),'nonfinite vector');return v

def witness(a,raw,t,alpha,beta):
    fitted=a@raw;tol=WITNESS_REL_TOL*max(1.,float(np.max(np.abs(a))))
    upper=1.-fitted+t;lower=fitted+t-1.;reduced=a.T@(alpha-beta)
    lo=raw<=WEIGHT_MIN+tol;hi=raw>=WEIGHT_MAX-tol;interior=~(lo|hi)
    comp=float(max(np.max(np.abs(alpha*upper),initial=0.),np.max(np.abs(beta*lower),initial=0.)))
    dual=float(alpha.sum()+beta.sum())
    checks={'alpha_nonnegative':bool(np.all(alpha>=-tol)),'beta_nonnegative':bool(np.all(beta>=-tol)),'upper_primal_feasible':bool(np.all(upper>=-tol)),'lower_primal_feasible':bool(np.all(lower>=-tol)),'weight_lower_feasible':bool(np.all(raw>=WEIGHT_MIN-tol)),'weight_upper_feasible':bool(np.all(raw<=WEIGHT_MAX+tol)),'objective_matches_t':True,'complementarity':bool(comp<=COMPLEMENTARITY_REL_TOL*max(1.,abs(t))),'t_bound_dual_stationarity':bool((t>tol and abs(dual-1.)<=1e-5) or (t<=tol and dual<=1.+1e-5)),'interior_weight_stationarity':bool(not np.any(interior) or np.max(np.abs(reduced[interior]))<=STATIONARITY_REL_TOL*max(1.,float(np.max(np.abs(a))))),'lower_weight_stationarity':bool(not np.any(lo) or np.min(reduced[lo])>=-tol),'upper_weight_stationarity':bool(not np.any(hi) or np.max(reduced[hi])<=tol)}
    return {'witness_tolerance':tol,'checks':checks,'all_checks_passed':all(checks.values()),'alpha_min':float(alpha.min()),'beta_min':float(beta.min()),'upper_slack_min':float(upper.min()),'lower_slack_min':float(lower.min()),'weight_min':float(raw.min()),'weight_max':float(raw.max()),'weight_lower_violation':float(max(0.,WEIGHT_MIN-raw.min(initial=WEIGHT_MIN))),'weight_upper_violation':float(max(0.,raw.max(initial=WEIGHT_MAX)-WEIGHT_MAX)),'objective':float(t),'t':float(t),'objective_t_absolute_delta':0.,'primal_max_violation':float(max(0.,-upper.min(initial=0.),-lower.min(initial=0.))),'complementarity_max':comp,'dual_sum':dual,'dual_sum_minus_one':dual-1.,'interior_stationarity_max':float(np.max(np.abs(reduced[interior]),initial=0.)),'lower_stationarity_min':None if not np.any(lo) else float(np.min(reduced[lo])),'upper_stationarity_max':None if not np.any(hi) else float(np.max(reduced[hi])),'lower_bound_count':int(np.count_nonzero(lo)),'upper_bound_count':int(np.count_nonzero(hi)),'interior_count':int(np.count_nonzero(interior))}

def decode(root,r,a,remaining):
    req(r.get('schema')==RESULT_SCHEMA and r.get('completed') is True and r.get('diagnostic_only') is True and r.get('execution_authority') is False and r.get('publication_authority') is False,'invalid result authority')
    d=r.get('diagnostics');req(type(d) is dict and type(d.get('rounds')) is list and d.get('round_count')==len(d['rounds'])<=MAX_ROUNDS,'invalid rounds')
    previous_iterations=0
    for i,row in enumerate(d['rounds']):
        ids=row.get('added_rows');scalars=(row.get('objective'),row.get('worst_full_violation'))
        req(row.get('round')==i and type(ids) is list and row.get('added_row_count')==len(ids)<=ADD_ROWS and all(type(x) is int and 0<=x<a.shape[0] for x in ids) and len(ids)==len(set(ids)),'invalid added row identities')
        req(type(row.get('subset_rows')) is int and 0<row['subset_rows']<=a.shape[0] and type(row.get('iterations')) is int and row['iterations']>=0 and type(row.get('cumulative_iterations')) is int and row['cumulative_iterations']==previous_iterations+row['iterations'] and all(type(x) is float and math.isfinite(x) for x in scalars) and type(row.get('retried_witness')) is bool,'invalid round trace')
        previous_iterations=row['cumulative_iterations']
    req(d.get('total_iterations')==previous_iterations,'round iteration total changed')
    life=r.get('lifecycle');req(type(life) is dict and all(type(life.get(k)) is int and life[k]>=0 for k in ('creates_attempted','creates_succeeded','destroys','passes','runs','solutions','option_sets','callbacks_registered','callbacks_started')) and life['creates_attempted']>=life['creates_succeeded']==life['destroys'],'unbalanced native lifecycle')
    attempts=r.get('attempts');req(type(attempts) is list and len(attempts)==life['creates_succeeded'],'attempt lifecycle mismatch')
    if r.get('status')=='terminal':
        req(r.get('rollback_required') is True and r.get('arrays') is None,'terminal promoted arrays')
        return {'accepted':False,'solution':None,'diagnostics':{'termination':r.get('terminal_reason'),'rounds':d['rounds'],'total_iterations':d['total_iterations'],'full_rows':d['full_rows'],'selected_rows':d['selected_rows']}}
    req(r.get('status')=='accepted' and r.get('rollback_required') is False,'invalid accepted status')
    specs=r.get('arrays');req(type(specs) is dict and set(specs)==set(ARRAYS),'array schema changed')
    rows,cols=a.shape
    vals={k:load_vector(root,specs[k],k,rows if k in ('mass','alpha','beta') else (d['selected_rows'] if k=='selected_rows' else cols)) for k in ARRAYS}
    selected=vals.pop('selected_rows').astype(np.int64);req(np.array_equal(selected,np.sort(selected)) and len(np.unique(selected))==len(selected) and np.all(selected<rows),'invalid selected identities')
    raw=vals['raw_weights'];alpha=vals['alpha'];beta=vals['beta'];objective=float(r.get('objective'));req(math.isfinite(objective),'invalid objective')
    audit=witness(a,raw,objective,alpha,beta);req(audit['all_checks_passed'],'native result failed Python original-coordinate witness')
    bias=np.asarray(np.log(raw),np.float32);stored=np.exp(bias.astype(np.float64));mass=a@stored
    req(np.array_equal(bias,vals['bias']),'stored bias bytes changed')
    eps=np.finfo(np.float64).eps
    req(np.allclose(stored,vals['stored_weights'],rtol=4*eps,atol=0),'native stored weights changed')
    req(np.max(np.abs(mass-vals['mass']))<=1e-12*max(1.,float(np.max(np.abs(mass)))),'native stored mass changed')
    stored_audit={'available':bool(np.isfinite(mass).all() and np.all(mass>0)),'stored_training_mass_max':float(np.max(np.abs(mass-1.))),'stored_weight_min':float(stored.min()),'stored_weight_max':float(stored.max())}
    req(stored_audit['available'],'stored candidate unavailable')
    rounds=[{'round':x['round'],'subset_rows':x['subset_rows'],'iterations':x['iterations'],'cumulative_iterations':x['cumulative_iterations'],'objective':x['objective'],'worst_full_violation':x['worst_full_violation'],'added_rows':x['added_rows'],**({'retried_witness':True} if x.get('retried_witness') else {})} for x in d['rounds']]
    last=rounds[-1]['iterations'];cg={'seed_rows':min(SEED_ROWS,rows),'warm_hint_rows':d['warm_hint_rows'],'initial_selected_rows':d['initial_selected_rows'],'selected_row_indices':selected.tolist(),'add_rows':ADD_ROWS,'max_rounds':MAX_ROUNDS,'rounds':rounds,'total_iterations':d['total_iterations'],'last_restricted_iterations':last,'full_rows':rows,'selected_rows':len(selected),'full_witness':audit,'stored_f32_log_bias_mass':stored_audit,'optimality_argument':'restricted_optimum_plus_full_feasibility_and_full_kkt'}
    sol={'raw_weights':raw,'bias':bias,'stored_weights':stored,'mass':mass,'alpha':alpha,'beta':beta,'objective':objective,'iterations':d['total_iterations'],'kkt':{'primal_max_violation':audit['primal_max_violation'],'complementarity_max':audit['complementarity_max'],'dual_sum':audit['dual_sum'],'interior_stationarity_max':audit['interior_stationarity_max']},'dual_support':int(np.count_nonzero(alpha>1e-12)+np.count_nonzero(beta>1e-12)),'time_limit_seconds':float(remaining),'constraint_generation':cg}
    return {'accepted':True,'solution':sol,'diagnostics':cg}

def solve_mass_lp(matrix,remaining_seconds,now=time.monotonic,*,initial_indices=None):
    started=float(now());req(math.isfinite(started) and math.isfinite(remaining_seconds) and remaining_seconds>0,'no LP time remains');deadline=started+remaining_seconds
    a=np.ascontiguousarray(np.asarray(matrix,np.float64));req(a.ndim==2 and tuple(a.shape) in ((912,352),(912,288),(3000,288)) and np.isfinite(a).all() and np.all(a>=0),'invalid full mass matrix')
    hints=None if initial_indices is None else np.asarray(initial_indices)
    if hints is not None: req(hints.ndim==1 and np.issubdtype(hints.dtype,np.signedinteger) and len(hints)<=a.shape[0] and len(np.unique(hints))==len(hints) and np.all(hints>=0) and np.all(hints<a.shape[0]),'invalid warm constraint row identities')
    hints=None if hints is None else np.ascontiguousarray(hints,dtype='<u8')
    c=config();process=None;pid=None;pgid=None;request_sha=None;result_sha=None;reason=None;decoded=None
    with tempfile.TemporaryDirectory(prefix='antfly-native-cg-',dir=c['scratch']) as td:
        root=Path(td);matrix_path=root/'matrix.f64le';write_exclusive(matrix_path,a.astype('<f8',copy=False).tobytes(order='C'))
        warm_edge=None
        if hints is not None:
            hp=root/'warm-rows.u64le';write_exclusive(hp,hints.tobytes());warm_edge={'path':hp.name,'sha256':sha(hp),'bytes':hp.stat().st_size,'dtype':'uint64-le','order':'C','count':len(hints)}
        manifest={'schema':INPUT_SCHEMA,'completed':True,'diagnostic_only':True,'execution_authority':False,'publication_authority':False,'fit_invoked':False,'held_tensors_loaded':False,'query_rows':a.shape[0],'retained_rows':a.shape[1],'matrix':{'path':matrix_path.name,'sha256':sha(matrix_path),'bytes':matrix_path.stat().st_size,'dtype':'float64-le','order':'C','shape':list(a.shape)},'warm_rows':warm_edge,'source':c['edges']['source']}
        manifest['request_sha256']=request_digest(manifest);mp=root/'input.json';write_json(mp,manifest);request_sha=sha(mp)
        current=float(now());req(math.isfinite(current) and current>=started,'clock regression')
        if current>=deadline: reason='deadline_before_spawn'
        if reason is None:
            out=root/'output';cmd=[str(c['binary']),'--input-manifest',str(mp),'--input-sha256',request_sha,'--library-receipt',str(c['library']),'--library-sha256',c['edges']['library']['sha256'],'--executable-receipt',str(c['executable']),'--executable-sha256',c['edges']['executable']['sha256'],'--output-dir',str(out),'--deadline-ms',str(max(1,min(560000,int((deadline-current)*1000))))]
            process=subprocess.Popen(cmd,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=False);pid=process.pid
            try:
                pgid=os.getpgid(pid);req(pgid==os.getpgrp(),'native CG escaped parent process group')
                try:process.wait(timeout=max(0.,deadline-float(now())))
                except subprocess.TimeoutExpired:reason='worker_timeout'
            finally: drain(process)
            req(process.poll() is not None,'native CG not reaped')
            current=float(now());req(math.isfinite(current) and current>=started,'clock regression')
            if reason is None and current>=deadline:reason='deadline_after_worker'
            if reason is None:
                req(all(edge(c[k])==c['edges'][k] for k in ('binary','executable','library','source')),'native CG closure changed')
                rp=out/'result.json';req(process.returncode==0 and rp.is_file(),'native CG failed without result');result_sha=sha(rp);receipt=json.loads(rp.read_text())
                req(receipt.get('input')==edge(mp) and receipt.get('library_receipt')==c['edges']['library'] and receipt.get('executable_receipt')==c['edges']['executable'] and receipt.get('request_sha256')==manifest['request_sha256'],'native receipt identity mismatch')
                decoded=decode(out,receipt,a,remaining_seconds)
                if float(now())>=deadline:reason='deadline_after_result_decode'
        final=float(now());req(math.isfinite(final) and final>=started,'clock regression')
    life={'schema':SCHEMA,'worker_pid':pid,'worker_pgid':pgid,'worker_same_process_group':pgid is not None and pgid==os.getpgrp(),'worker_drained':True,'request_sha256':request_sha,'response_sha256':result_sha,'expected_executable':c['edges']['executable'],'elapsed_seconds':final-started}
    if reason is not None or final>=deadline:return {'accepted':False,'solution':None,'diagnostics':{'termination':'shared_deadline_exhausted_subprocess','reason':reason or 'deadline_after_cleanup','elapsed_seconds':final-started,'subprocess_lifecycle':life}}
    req(decoded is not None,'missing decoded result');decoded['diagnostics']['subprocess_lifecycle']=life
    if decoded['accepted']:decoded['solution']['constraint_generation']['subprocess_lifecycle']=life
    audited=float(now());req(math.isfinite(audited) and audited>=final,'clock regression')
    if audited>=deadline:return {'accepted':False,'solution':None,'diagnostics':{'termination':'shared_deadline_exhausted_subprocess','reason':'deadline_after_numerical_audit','elapsed_seconds':audited-started,'subprocess_lifecycle':{**life,'elapsed_seconds':audited-started}}}
    life['elapsed_seconds']=audited-started;return decoded
