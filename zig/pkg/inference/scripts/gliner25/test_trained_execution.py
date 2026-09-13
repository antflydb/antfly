from __future__ import annotations

import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import check_trained_execution as check
import test_training_merge as fixtures


def sample_source(request):
    """Authored protocol data, never a model prediction or quality fixture."""
    schema=check.schema_for(request);text=request["text"]
    end=text.index(" ") if " " in text else len(text)
    value={"text":text[:end],"confidence":.75,"start":0,"end":end}
    output={"entities":{name:[copy.deepcopy(value)] for name in schema.get("entities",[])}}
    if request["kind"]=="joint_ie":
        types=request["schema"]["entities"]
        output={"entities":[dict(value,id="e0",type=types[0])],"relations":[]}
    for task in schema.get("classifications",[]):
        name=task["name"];label=task["labels"][0]
        output[name]={"label":label,"confidence":.75}
    if request["kind"]=="classification":output["_meta"]={"feasible":True,"exact":True,"decoder":"exact","objective":1.,"violations":[]}
    for name,spec in schema.get("structures",{}).items():
        record={}
        for field,rule in spec["fields"].items():
            field_value={"text":rule["choices"][0],"confidence":.8} if "choices" in rule else copy.deepcopy(value)
            record[field]=[field_value] if rule["type"]=="list" else field_value
        output[name]=[record]
    return output


def native_output(request,raw):
    result=check.bench.canonical_python(request,raw);schema=check.schema_for(request)
    def value(item):
        item["derived"]=item["source"] is None;item["token_span"]=None
        if item["source"] is not None:
            item["source"].update(unit="unicode_codepoints",byte_start=len(request["text"][:item["source"]["start"]].encode()),
                                  byte_end=len(request["text"][:item["source"]["end"]].encode()))
        for group in item["attributes"]:group["multi_label"]=False
    for group in result["entities"]:
        group["dtype"]="list"
        for item in group["values"]:value(item)
    for group in result["classifications"]:group["multi_label"]=False
    for group in result["structures"]:
        for record in group["instances"]:
            record.update(confidence=None,anchor=None)
            for field in record["fields"]:
                field["dtype"]=schema["structures"][group["name"]]["fields"][field["name"]]["type"]
                for item in field["values"]:value(item)
    for relation in result["relations"]:
        value(relation["head"]);value(relation["tail"])
    result.update(classification_solver=None,joint_solver=None,record_solver=None,long_document=None)
    if request["kind"]=="classification":result["classification_solver"]={"status":"optimal","exhausted":False,"objective":1.,"work":3}
    if request["kind"]=="joint_ie":result["joint_solver"]={"status":"optimal","exhausted":False,"objective":1.,"work":3}
    return result


def oracle_report(audit,requests):
    captures={phase:[{"id":r["id"],"request_sha256":check.merge.object_digest(r),"input_ids":[1,2,index],
                     "output":sample_source(r)} for index,r in enumerate(requests)] for phase in check.merge.PHASES}
    tensors=[]
    for name,item in sorted(audit["merged_tensors"].items()):
        count=item["size_bytes"]//4
        if name in audit["adapted_names"]:
            tensors.append({"name":name,"kind":"numerical",**check.merge.numeric_comparison([0.]*count,[0.]*count)})
        else:tensors.append({"name":name,"kind":"exact","elements":count,"bytes_equal":True})
    runtime={"status":"verified","qualification":False,"tolerances":check.merge.TOLERANCES,
        "loader_profile":"peft-0.18.0-export-v1","one_resident_model_owner":True,"owners_released_before_next_load":True,
        "network_allowed":False,"missing_weight_fallback":False,"quality_evaluation":False,"requests":check.merge.REQUEST_PIN,
        "private_copy_bytes":10,"captures":captures,"comparisons":check.merge.compare_phases(captures,requests),
        "tensor_comparisons":tensors,"tensor_parity_pass":True}
    return {"scope":check.merge.SCOPE,"qualification":False,"status":"verified","numerical_runtime_executed":True,
        "numerical_runtime_requested":True,"helpers":check.merge.helper_identity(),"contract":check.merge.load_contract()[1],
        "tolerances":check.merge.TOLERANCES,"audit":audit,"runtime":runtime}


def admission(env):
    l=env["limits"];w=env["merge"]["merged"]["weight"]["size_bytes"];d=l["encoder_device_bytes"]+l["head_device_bytes"] if env["backend"]=="metal" else 0
    copied=w if env["backend"]=="metal" else 0;request=l["request_scratch_bytes"]+copied
    host=8*check.MIB+w+l["loader_host_bytes"]+request
    return dict(setup_bytes=8*check.MIB,mapped_weight_bytes=w,loader_host_bytes=l["loader_host_bytes"],
        request_weight_copy_bytes=copied,request_scratch_bytes=l["request_scratch_bytes"],request_host_bytes=request,
        device_context_bytes=d,host_total_bytes=host,backend_total_bytes=d,combined_total_bytes=host+d)


def metal_stats():
    owner=dict(peak_device_bytes=1000,charged_weight_bytes=100,metadata_upload_bytes=4,proposal_download_bytes=0,
        result_download_bytes=12,proposal_download_calls=0,result_download_calls=1,device_dispatches=5)
    return dict(encoder=owner,head=None,result_download_bytes=12,proposal_download_bytes=0,peak_device_upper_bound_bytes=1000)


def events_for(env,fixture_pin,inputs,requests,report):
    ready={"event":"ready","scope":check.SCOPE,"version":1,"qualification":False,"backend":env["backend"],
        "math_policy":check.MATH_POLICY,"weight_precision":"fp32","activation_precision":"f32","accumulation_precision":"f32",
        "head_precision":"f32","fixture_digest":fixture_pin,"inputs_digest":check.INPUT_PIN,"oracle_report":env["oracle_report"],
        "merge_receipt":env["merge_receipt"],"identity":env["merge"]["merged"],"limits":env["limits"],
        "build_mode":"ReleaseFast","zig_version":"0.16.0-test","admission":admission(env)}
    events=[ready]
    for index,case in enumerate(inputs["cases"]):
        events.append({"event":"result","case_id":case["id"],"canonical_request_sha256":check.canonical_request_pin(case),
            "input_ids":[1,2,index],"output":native_output(requests[index],report["runtime"]["captures"]["native_merged"][index]["output"]),
            "request_host_peak_bytes":10,"metal":metal_stats() if env["backend"]=="metal" else None})
    complete={"event":"complete","cases":10,"errors":0,"qualification":False,"fixture_digest":fixture_pin,
        "inputs_digest":check.INPUT_PIN,"merge_receipt":env["merge_receipt"],"identity":env["merge"]["merged"],
        "output_bytes_before_complete":sum(len(check.encoded(x))+1 for x in events),"loader_host_peak_bytes":10}
    return events+[complete]


def sizes(events):return [len(check.encoded(x))+1 for x in events]


class Prepared:
    def __enter__(self):
        self.owner=fixtures.prepared();self.f=self.owner.__enter__();self.audit=fixtures.audit(self.f)
        self.contract,self.contract_pin,self.inputs,self.requests,_=check.load_contract()
        self.report=oracle_report(self.audit,self.requests);self.report_path=self.f.root/"oracle.json"
        self.report_path.write_bytes(check.encoded(self.report)+b"\n");self.report_pin=check.pin(self.report_path,check.MAX_REPORT)
        self.binary=self.f.root/"worker";self.binary.write_text('#!/bin/sh\nexit 0\n');self.binary.chmod(0o700)
        self.args=check.parser().parse_args(['--variant','small','--backend','native','--source-dir',str(self.f.source),
            '--adapter-dir',str(self.f.adapter),'--run-dir',str(self.f.training),'--model-dir',str(self.f.merged),
            '--merge-job-config',str(self.f.config),'--merge-report',str(self.report_path),'--merge-report-sha256',self.report_pin['sha256'],
            '--binary',str(self.binary),'--output-dir',str(self.f.root/'output')])
        self.env=check.envelope('native',check.LIMITS,self.report_pin,self.audit)
        self.fixture_pin=check.exports.digest_bytes(check.encoded(self.env)+b'\n')
        self.events=events_for(self.env,self.fixture_pin,self.inputs,self.requests,self.report)
        return self
    def __exit__(self,*args):return self.owner.__exit__(*args)


class TrainedContractTests(unittest.TestCase):
    def test_input_fixture_is_only_exact_ten_frozen_ordered_schemas(self):
        contract,pin,value,requests,raw=check.load_contract()
        self.assertEqual(check.INPUT_PIN,check.exports.digest_bytes(raw));self.assertEqual(10,len(value['cases']))
        for item,request in zip(value['cases'],requests):
            self.assertEqual({'id','kind','text','schema'},set(item));self.assertEqual(check.schema_for(request),item['schema'])
        changed=copy.deepcopy(value);changed['cases'][0]['schema']['entities'].reverse()
        self.assertNotEqual(check.canonical_request_pin(value['cases'][0]),check.canonical_request_pin(changed['cases'][0]))
        self.assertFalse(any(x in sys.modules for x in ('torch','gliner2','peft')))

    def test_closed_integer_resource_profile_rejects_bool_unknown_missing_and_overflow(self):
        self.assertEqual(check.LIMITS,check.limits(check.LIMITS))
        for edit in (lambda x:x.update(extra=1),lambda x:x.pop('version'),lambda x:x.update(version=True),
                     lambda x:x.update(loader_host_bytes='134217728'),lambda x:x.update(head_device_bytes=2**80),
                     lambda x:x.update(event_bytes=0),lambda x:x.update(total_output_bytes=1),
                     lambda x:x.update(total_timeout_ms=100)):
            value=copy.deepcopy(check.LIMITS);edit(value)
            with self.assertRaises(ValueError):check.limits(value)

    def test_approved_merge_report_requires_real_audit_numerical_success_and_frozen_tolerances(self):
        with Prepared() as p:
            self.assertEqual(p.audit,check.admitted_oracle(p.args,p.report_pin['sha256'],p.requests)[2])
            with self.assertRaisesRegex(ValueError,'SHA'):check.admitted_oracle(p.args,'0'*64,p.requests)
            for key,value in [('status','static_verified'),('qualification',True),('numerical_runtime_executed',False),('tolerances',{})]:
                changed=copy.deepcopy(p.report);changed[key]=value;p.report_path.write_bytes(check.encoded(changed))
                with self.assertRaises(ValueError):check.admitted_oracle(p.args,check.pin(p.report_path,check.MAX_REPORT)['sha256'],p.requests)

    def test_exact_full_protocol_and_all_three_source_comparisons(self):
        with Prepared() as p:
            rows=check.evaluate_events(p.events,sizes(p.events),p.env,p.fixture_pin,p.inputs,p.requests,p.report)
            self.assertEqual(10,len(rows));self.assertTrue(all(x['pass'] and len(x['python_comparisons'])==3 for x in rows))
            for edit in (lambda e:e.pop(2),lambda e:e.insert(2,e[1]),lambda e:e[2].update(case_id=e[1]['case_id']),
                         lambda e:e[0].update(identity={}),lambda e:e[-1].update(errors=1),lambda e:e[-1].update(output_bytes_before_complete=1)):
                events=copy.deepcopy(p.events);edit(events)
                with self.assertRaises(ValueError):check.evaluate_events(events,sizes(events),p.env,p.fixture_pin,p.inputs,p.requests,p.report)

    def test_decisions_tokens_and_confidence_use_existing_tolerance_without_hiding_mismatch(self):
        with Prepared() as p:
            for change in (lambda x:x['input_ids'].__setitem__(0,7),
                           lambda x:x['output']['entities'][0]['values'][0].update(confidence=.751)):
                event=copy.deepcopy(p.events[1]);change(event)
                row=check.compare_case(event,p.inputs['cases'][0],p.requests[0],p.report,p.env,p.events[0])
                self.assertFalse(row['pass'])
            event=copy.deepcopy(p.events[1]);event['output']['entities'][0]['values'][0]['confidence']+=1e-5
            self.assertTrue(check.compare_case(event,p.inputs['cases'][0],p.requests[0],p.report,p.env,p.events[0])['pass'])

    def test_original_unicode_byte_coordinates_and_synthetic_suffix_never_clipped(self):
        with Prepared() as p:
            original=p.events[2]['output'];canonical=check.bench.canonical_result(original)
            check.validate_sources(p.requests[1]['text'],original,canonical,True)
            for edit in (lambda x:x['source'].update(byte_end=x['source']['byte_end']-1),
                         lambda x:x['source'].update(end=500),lambda x:x.update(text='wrong'),
                         lambda x:x['source'].update(start=True)):
                altered=copy.deepcopy(original);edit(altered['entities'][0]['values'][0])
                with self.assertRaises(ValueError):check.validate_sources(p.requests[1]['text'],altered,check.bench.canonical_result(altered),True)
            canonical={'entities':[{'name':'time_zone','values':[{'text':'cpt.','source':{'start':16,'end':20},'confidence':.6,'attributes':[]}]}],
                       'classifications':[],'structures':[],'relations':[]}
            with self.assertRaisesRegex(ValueError,'original'):check.validate_sources('current time in cpt',{},canonical,False)

    def test_strict_metal_owner_readback_admission_and_status_metadata(self):
        with Prepared() as p:
            env=check.envelope('metal',check.LIMITS,p.report_pin,p.audit);digest=check.exports.digest_bytes(check.encoded(env)+b'\n')
            events=events_for(env,digest,p.inputs,p.requests,p.report)
            self.assertTrue(all(x['pass'] for x in check.evaluate_events(events,sizes(events),env,digest,p.inputs,p.requests,p.report,p.events)))
            for edit in (lambda x:x.update(metal=None),lambda x:x['metal']['encoder'].update(device_dispatches=0),
                         lambda x:x['metal']['encoder'].update(proposal_download_calls=1),
                         lambda x:x['metal'].update(result_download_bytes=13),lambda x:x.update(request_host_peak_bytes=2**40)):
                altered=copy.deepcopy(events[1]);edit(altered)
                with self.assertRaises(ValueError):check.compare_case(altered,p.inputs['cases'][0],p.requests[0],p.report,env,events[0],p.events[1])
            altered=copy.deepcopy(events[-2]);altered['output']['joint_solver']['exhausted']=True
            with self.assertRaisesRegex(ValueError,'strict'):check.compare_case(altered,p.inputs['cases'][-1],p.requests[-1],p.report,env,events[0],p.events[-2])


class TrainedProcessTests(unittest.TestCase):
    def fake_worker(self,p,mode=None):
        owners=[]
        class Worker:
            def __init__(self,arm,command,environment,output,guard):
                self.buffer=bytearray();self.calls=0;self.closed=False
                self.process=SimpleNamespace(poll=lambda:0,returncode=0,stdout=io.BytesIO())
                env=json.loads((output/'execution.json').read_bytes());fp=check.pin(output/'execution.json',check.MAX_INPUTS)
                self.events=events_for(env,fp,p.inputs,p.requests,p.report)
                if mode=='comparison':self.events[1]['output']['entities'][0]['values'][0]['confidence']=.751
                if mode=='metadata':self.events[1]['output']['entities'][0]['dtype']='str'
                if mode=='missing':self.events.pop(3)
                if mode=='duplicate':self.events[2]=self.events[1]
                if mode=='stderr':raise OSError('worker launch failed')
                if mode=='extra':self.process.stdout=io.BytesIO(b'unsolicited')
                self.events[-1]['output_bytes_before_complete']=sum(sizes(self.events[:-1]));owners.append(self)
            def receive(self,_):
                self.calls+=1
                if mode=='cancel' and self.calls==3:raise KeyboardInterrupt()
                if mode=='deadline' and self.calls==3:raise TimeoutError('deadline')
                event=self.events[self.calls-1];self.last_raw=check.encoded(event)+b'\n';return event
            def close(self):self.closed=True
        return Worker,owners

    def test_fake_complete_and_failed_numeric_reports_are_retained_and_copy_reclaimed(self):
        for mode in (None,'comparison'):
            with self.subTest(mode=mode),Prepared() as p:
                worker,owners=self.fake_worker(p,mode)
                with mock.patch.object(check,'RawWorker',worker):report=check.run(p.args)
                self.assertEqual('complete',report['status']);self.assertEqual(mode is None,report['parity_pass'])
                self.assertTrue(owners[0].closed);self.assertFalse((p.args.output_dir/'.artifact-scratch').exists())
                self.assertTrue((p.args.output_dir/'report.json').exists());self.assertFalse((p.args.output_dir/'failure.json').exists())
                with self.assertRaises(FileExistsError):check.run(p.args)

    def test_fake_cancel_deadline_missing_duplicate_extra_and_launch_failure_keep_evidence(self):
        for mode in ('cancel','deadline','missing','duplicate','extra','stderr'):
            with self.subTest(mode=mode),Prepared() as p:
                worker,owners=self.fake_worker(p,mode)
                with mock.patch.object(check,'RawWorker',worker),self.assertRaises((KeyboardInterrupt,TimeoutError,ValueError,IndexError,OSError)):
                    check.run(p.args)
                self.assertTrue(all(x.closed for x in owners));self.assertFalse((p.args.output_dir/'.artifact-scratch').exists())
                self.assertTrue((p.args.output_dir/'failure.json').exists());self.assertFalse((p.args.output_dir/'report.json').exists())
                self.assertEqual(False,json.loads((p.args.output_dir/'process.json').read_text())['complete_protocol'])

    def test_native_reference_requires_exact_report_binary_inputs_events_and_exit_receipt(self):
        with Prepared() as p:
            worker,_=self.fake_worker(p)
            with mock.patch.object(check,'RawWorker',worker):report=check.run(p.args)
            path=p.args.output_dir/'report.json';digest=check.pin(path,check.MAX_REPORT)
            env=check.envelope('metal',check.LIMITS,p.report_pin,p.audit)
            helpers=check.helper_identity(p.contract,p.contract_pin);binary_pin=check.pin(p.binary,check.MIB)
            events,_=check.native_reference(path,digest['sha256'],env,p.report,p.inputs,p.requests,helpers,binary_pin)
            self.assertEqual(12,len(events))
            for changed in ({**binary_pin,'sha256':'0'*64},{**binary_pin,'size_bytes':1}):
                with self.assertRaises(ValueError):check.native_reference(path,digest['sha256'],env,p.report,p.inputs,p.requests,helpers,changed)
            raw=(p.args.output_dir/'events.jsonl').read_bytes();(p.args.output_dir/'events.jsonl').write_bytes(raw.replace(b'0.75',b'0.76',1))
            with self.assertRaisesRegex(ValueError,'event'):check.native_reference(path,digest['sha256'],env,p.report,p.inputs,p.requests,helpers,binary_pin)

    def test_complete_native_then_metal_requires_public_metadata_and_reclaims_both_copies(self):
        for mode in (None,'metadata'):
            with self.subTest(mode=mode),Prepared() as p:
                worker,_=self.fake_worker(p)
                with mock.patch.object(check,'RawWorker',worker):native=check.run(p.args)
                source=p.args.output_dir/'report.json'
                metal_args=copy.copy(p.args);metal_args.backend='metal';metal_args.output_dir=p.f.root/'metal'
                metal_args.native_reference_report=source;metal_args.native_reference_sha256=check.pin(source,check.MAX_REPORT)['sha256']
                worker,owners=self.fake_worker(p,mode)
                with mock.patch.object(check,'RawWorker',worker):result=check.run(metal_args)
                self.assertTrue(native['parity_pass']);self.assertEqual(mode is None,result['parity_pass'])
                self.assertTrue(all(row['pass'] for row in result['comparisons'][0]['python_comparisons']))
                self.assertEqual(mode is None,result['comparisons'][0]['backend_comparison']['parity_pass'])
                self.assertTrue(all(x.closed for x in owners));self.assertFalse((metal_args.output_dir/'.artifact-scratch').exists())

    def test_same_descriptor_copy_refuses_tamper_before_dispatch_and_preserves_originals(self):
        with Prepared() as p:
            before=(p.f.merged/'tokenizer.json').read_bytes()
            altered=copy.deepcopy(p.audit['merged_files']);altered['tokenizer.json']['sha256']='0'*64
            destination=p.f.root/'private'
            with self.assertRaises(ValueError):check.copy_artifact(p.f.merged,destination,altered,SimpleNamespace(check=lambda:None))
            self.assertEqual(before,(p.f.merged/'tokenizer.json').read_bytes())
            self.assertFalse((p.f.source/'private').exists())

    def test_real_stdlib_child_raw_float_spelling_is_preserved_and_worker_reaped(self):
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp);guard=check.Guard(check.time.monotonic()+5)
            worker=check.RawWorker('raw',[sys.executable,'-c','print(\'{"value":1e-8}\',flush=True)'],os.environ.copy(),output,guard)
            try:
                self.assertEqual({'value':1e-8},worker.receive(3));self.assertEqual(b'{"value":1e-8}\n',worker.last_raw)
                self.assertNotEqual(check.encoded({'value':1e-8})+b'\n',worker.last_raw)
            finally:worker.close()
            self.assertIsNotNone(worker.process.poll())

    def test_real_unresponsive_child_times_out_and_is_reaped(self):
        with tempfile.TemporaryDirectory() as temp:
            guard=check.Guard(check.time.monotonic()+5)
            worker=check.RawWorker('sleep',[sys.executable,'-c','import time;time.sleep(30)'],os.environ.copy(),Path(temp),guard)
            try:
                with self.assertRaisesRegex(ValueError,'deadline'):worker.receive(.1)
            finally:worker.close()
            self.assertIsNotNone(worker.process.poll())

    def test_constructor_selector_failures_reap_children_before_private_copy_removal(self):
        real_popen=check.subprocess.Popen;real_selector=check.selectors.DefaultSelector
        real_remove=check.shutil.rmtree
        for mode in ('allocation','registration','registration_interrupt'):
            with self.subTest(mode=mode),Prepared() as p:
                children=[];selectors=[]
                def launch(*_args,**kwargs):
                    child=real_popen([sys.executable,'-c','import time;time.sleep(30)'],**kwargs)
                    children.append(child);return child
                class FailedSelector:
                    def __init__(self):self.owner=real_selector();self.closed=False;selectors.append(self)
                    def register(self,*_):
                        if mode=='registration_interrupt':raise KeyboardInterrupt('registration interrupted')
                        raise OSError('selector registration failed')
                    def close(self):self.owner.close();self.closed=True
                def removed(path,*args,**kwargs):
                    self.assertTrue(all(child.poll() is not None for child in children))
                    self.assertTrue(all(child.stdin.closed and child.stdout.closed for child in children))
                    self.assertTrue(all(owner.closed for owner in selectors))
                    return real_remove(path,*args,**kwargs)
                factory=mock.Mock(side_effect=MemoryError('selector allocation failed')) if mode=='allocation' else FailedSelector
                with mock.patch.object(check.subprocess,'Popen',side_effect=launch),\
                     mock.patch.object(check.selectors,'DefaultSelector',side_effect=factory),\
                     mock.patch.object(check.shutil,'rmtree',side_effect=removed),\
                     self.assertRaises((MemoryError,OSError,KeyboardInterrupt)):
                    check.run(p.args)
                self.assertEqual(0 if mode=='allocation' else 1,len(children))
                receipt=json.loads((p.args.output_dir/'process.json').read_bytes())
                self.assertTrue(receipt['scratch_cleaned']);self.assertFalse(receipt['complete_protocol'])
                self.assertIsNone(receipt['cleanup_error']);self.assertEqual(0,receipt['events'])
                if children:self.assertEqual(children[0].returncode,receipt['worker_exit_code'])
                self.assertTrue((p.args.output_dir/'failure.json').exists())
                self.assertFalse((p.args.output_dir/'report.json').exists())

    def test_unreturned_constructor_owner_retries_cleanup_before_removing_scratch(self):
        with Prepared() as p:
            real_popen=check.subprocess.Popen;real_close=check.RawWorker.close;children=[];attempts=[]
            def launch(*_args,**kwargs):
                child=real_popen([sys.executable,'-c','import time;time.sleep(30)'],**kwargs)
                children.append(child);return child
            def close(owner):
                attempts.append(owner)
                if len(attempts)==1:raise OSError('first cleanup failed')
                real_close(owner)
            selector=mock.Mock();selector.register.side_effect=OSError('registration failed')
            with mock.patch.object(check.subprocess,'Popen',side_effect=launch),\
                 mock.patch.object(check.selectors,'DefaultSelector',return_value=selector),\
                 mock.patch.object(check.RawWorker,'close',close),\
                 self.assertRaisesRegex(OSError,'first cleanup failed'):
                check.run(p.args)
            self.assertEqual(2,len(attempts));self.assertIs(attempts[0],attempts[1])
            self.assertEqual(1,len(children));self.assertIsNotNone(children[0].poll())
            self.assertTrue(children[0].stdin.closed and children[0].stdout.closed)
            self.assertTrue(attempts[0].log.closed);selector.close.assert_called_once()
            receipt=json.loads((p.args.output_dir/'process.json').read_bytes())
            self.assertTrue(receipt['scratch_cleaned']);self.assertFalse(receipt['complete_protocol'])
            self.assertEqual(children[0].returncode,receipt['worker_exit_code'])

    def test_unreaped_constructor_owner_keeps_private_artifacts_and_failure_receipt(self):
        with Prepared() as p:
            real_popen=check.subprocess.Popen;real_close=check.RawWorker.close;owners=[]
            def launch(*_args,**kwargs):
                return real_popen([sys.executable,'-c','import time;time.sleep(30)'],**kwargs)
            def unavailable_cleanup(owner):
                owners.append(owner);raise OSError('reaping unavailable')
            selector=mock.Mock();selector.register.side_effect=OSError('registration failed')
            try:
                with mock.patch.object(check.subprocess,'Popen',side_effect=launch),\
                     mock.patch.object(check.selectors,'DefaultSelector',return_value=selector),\
                     mock.patch.object(check.RawWorker,'close',unavailable_cleanup),\
                     self.assertRaisesRegex(OSError,'reaping unavailable'):
                    check.run(p.args)
                self.assertEqual(2,len(owners));self.assertIs(owners[0],owners[1])
                self.assertIsNone(owners[0].process.poll())
                self.assertTrue((p.args.output_dir/'.artifact-scratch/model/model.safetensors').exists())
                receipt=json.loads((p.args.output_dir/'process.json').read_bytes())
                self.assertFalse(receipt['scratch_cleaned']);self.assertFalse(receipt['complete_protocol'])
                self.assertIsNone(receipt['worker_exit_code']);self.assertEqual('OSError',receipt['cleanup_error']['type'])
                self.assertTrue((p.args.output_dir/'failure.json').exists());self.assertFalse((p.args.output_dir/'report.json').exists())
            finally:
                for owner in owners:real_close(owner)


if __name__=='__main__':unittest.main()
