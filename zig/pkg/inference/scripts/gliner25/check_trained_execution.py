#!/usr/bin/env python3
"""Bounded Zig CPU/Metal execution of independently checked trained FP32 exports.

Additive proof only: no model promotion, training, threshold tuning or quality
evaluation. Python imports are metadata/contract code; only the supervised Zig
worker executes a model. Existing oracle/checker bytes are immutable inputs.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import time

import check_training_merge as merge
from generate_pipeline_cases import schema_for

bench, compare, exports, oracle = merge.bench, merge.comparison, merge.exports, merge.oracle
HERE = Path(__file__).resolve().parent
CONTRACT = HERE / "trained_execution_contract_v1.json"
INPUTS = oracle.FIXTURES / "trained_execution_inputs_v1.json"
SCOPE = "gliner25_trained_artifact_execution/v1"
REPORT_SCOPE = "gliner25_trained_artifact_execution_report/v1"
INPUT_SCOPE = "gliner25_trained_artifact_inputs/v1"
MATH_POLICY = "strict_f32_activations_v1"
MIB = 1024**2
MAX_INPUTS = 65536
MAX_REPORT = 8 * MIB
MAX_EVENT = 4 * MIB
MAX_OUTPUT = 64 * MIB
MAX_COPY = 2 * 1024 * MIB
MAX_STDERR = 8 * MIB
RSS_LIMIT = 6144 * MIB
INPUT_PIN = {"size_bytes": 5965, "sha256": "d9414f1d7a2138ed0a9211285bcee5f662667747a31b255ca1708f48a7966abd"}
LIMITS = {"version": 1, "loader_host_bytes": 128*MIB, "request_scratch_bytes": 128*MIB,
    "encoder_device_bytes": 512*MIB, "head_device_bytes": 128*MIB,
    "proposal_download_bytes": 8*MIB, "result_download_bytes": 8*MIB,
    "combined_bytes": 3*1024*MIB, "event_bytes": MAX_EVENT, "total_output_bytes": MAX_OUTPUT,
    "startup_timeout_ms": 180000, "request_timeout_ms": 120000, "total_timeout_ms": 600000}
HARD_LIMITS = {"loader_host_bytes": (MIB, 1024*MIB), "request_scratch_bytes": (MIB, 2*1024*MIB),
    "encoder_device_bytes": (MIB, 4*1024*MIB), "head_device_bytes": (MIB, 1024*MIB),
    "proposal_download_bytes": (1,64*MIB), "result_download_bytes": (1,64*MIB),
    "combined_bytes": (MIB,12*1024*MIB), "event_bytes": (1,MAX_EVENT),
    "total_output_bytes": (1,MAX_OUTPUT), "startup_timeout_ms": (1,180000),
    "request_timeout_ms": (1,120000), "total_timeout_ms": (1,1800000)}
HELPERS = (*merge.HELPERS, "check_training_merge.py", "training_merge_contract.json", "extract_token_evidence.py")

checked, encoded, pin = merge.checked, merge.encoded, merge.pin


def read(path, maximum):
    with exports.Opened(path, maximum) as owner:
        raw = b"".join(owner.chunks())
    return raw, exports.digest_bytes(raw)


def limits(value):
    checked(isinstance(value, dict) and set(value) == set(LIMITS) and
            type(value["version"]) is int and value["version"] == 1, "invalid trained execution limits")
    for name, (minimum, maximum) in HARD_LIMITS.items():
        checked(type(value[name]) is int and minimum <= value[name] <= maximum, "invalid trained limit: " + name)
    checked(value["event_bytes"] <= value["total_output_bytes"], "event limit exceeds total output")
    checked(value["total_timeout_ms"]>=max(value["startup_timeout_ms"],value["request_timeout_ms"]),
            "total timeout is smaller than a declared phase timeout")
    return dict(value)


def build_inputs(requests):
    return {"version": 1, "scope": INPUT_SCOPE, "source_commit": oracle.UPSTREAM_COMMIT,
        "requests": merge.REQUEST_PIN, "offset_unit": "unicode_codepoints", "word_splitter": "whitespace",
        "threshold": .5, "cases": [{"id": item["id"], "kind": item["kind"], "text": item["text"],
                                    "schema": schema_for(item)} for item in requests]}


def load_contract():
    raw, contract_pin = read(CONTRACT, MAX_INPUTS)
    value = exports.decode(raw)
    checked(value.get("scope") == SCOPE and type(value.get("version")) is int and value["version"] == 1 and
        value.get("qualification") is False and value.get("input_scope") == INPUT_SCOPE and
        value.get("report_scope") == REPORT_SCOPE and value.get("inputs") == INPUT_PIN and
        value.get("requests") == merge.REQUEST_PIN and value.get("math_policy") == MATH_POLICY and
        value.get("confidence_absolute_tolerance") == 5e-4 and value.get("default_limits") == LIMITS and
        value.get("hard_limits") == {name:list(pair) for name,pair in HARD_LIMITS.items()} and
        value.get("rss_limit_bytes") == RSS_LIMIT and value.get("max_private_copy_bytes") == MAX_COPY and
        value.get("max_stderr_bytes") == MAX_STDERR and value.get("request_count") == 10 and
        value.get("event_count") == 12 and value.get("max_report_bytes") == MAX_REPORT and
        value.get("max_words") == 128 and value.get("max_encoded_tokens") == 512 and value.get("max_queries") == 64 and
        value.get("native_probability_map_parity_claimed") is False and
        value.get("source_coordinate_contract") == "original_codepoints_and_exact_utf8_bytes_no_synthetic_clipping" and
        value.get("metal_requires_native") == "identical_binary_artifact_inputs_math_policy_and_approved_report_hash" and
        set(value.get("frozen_helpers", {})) == set(HELPERS), "trained execution contract differs")
    for name in HELPERS:
        exports.verify_pin(pin(HERE/name, 2*MIB), value["frozen_helpers"][name], "frozen helper "+name)
    merge_contract, _ = merge.load_contract()
    requests, _ = merge.request_fixture(merge_contract)
    fixture, fixture_pin = read(INPUTS, MAX_INPUTS)
    exports.verify_pin(fixture_pin, INPUT_PIN, "input-only fixture")
    generated = build_inputs(requests)
    checked(fixture == (json.dumps(generated, ensure_ascii=False, allow_nan=False, indent=2)+"\n").encode(),
            "input-only fixture no longer derives from the frozen schemas")
    checked(not any(name in sys.modules for name in ("torch", "gliner2", "peft")), "driver imported a numerical runtime")
    return value, contract_pin, generated, requests, fixture


def helper_identity(contract, contract_pin):
    return {"driver": pin(Path(__file__), MIB), "contract": contract_pin,
            "frozen_helpers": contract["frozen_helpers"]}


def driver_runtime():
    return {"python":merge.python_identity(),"python_version":sys.version,
            "psutil_version":importlib.metadata.version("psutil")}


def canonical_request_pin(case):
    return hashlib.sha256(encoded({"text":case["text"], "schema":case["schema"]})).hexdigest()


def admitted_oracle(args, expected_report_sha, requests):
    raw, report_pin = read(args.merge_report, MAX_REPORT)
    checked(compare.is_digest(expected_report_sha) and report_pin["sha256"] == expected_report_sha,
            "merge oracle report SHA differs")
    report = exports.decode(raw)
    checked(report.get("scope") == merge.SCOPE and report.get("status") == "verified" and
        report.get("qualification") is False and report.get("numerical_runtime_executed") is True and
        report.get("numerical_runtime_requested") is True and report.get("helpers") == merge.helper_identity(),
        "merge oracle is not a completed matching numerical check")
    contract, contract_pin = merge.load_contract()
    checked(report.get("contract") == contract_pin and report.get("tolerances") == merge.TOLERANCES,
            "merge oracle contract or tolerances differ")
    audit = merge.audit_merge(args.variant,args.source_dir,args.adapter_dir,args.run_dir,args.model_dir,args.merge_job_config,contract)
    checked(report.get("audit") == audit, "merge oracle does not bind the actual trained artifacts")
    runtime = report.get("runtime", {})
    checked(merge.validate_runtime_result(runtime,audit,requests,runtime.get("captures",{})),
            "merge oracle numerical comparisons did not pass")
    return report, report_pin, audit


def envelope(backend, resource_limits, report_pin, audit):
    checked(backend in ("native", "metal"), "invalid trained backend")
    return {"version":1,"scope":SCOPE,"qualification":False,"backend":backend,"inputs":INPUT_PIN,
        "oracle_report":report_pin,"merge_receipt":audit["merged_files"][merge.RECEIPT],
        "merge":audit["merge_receipt"],"limits":limits(resource_limits)}


def integer(value, name, maximum=2**63-1):
    checked(type(value) is int and 0 <= value <= maximum, "invalid nonnegative integer: "+name)
    return value


def validate_admission(value, env):
    names = {"setup_bytes","mapped_weight_bytes","loader_host_bytes","request_weight_copy_bytes",
             "request_scratch_bytes","request_host_bytes","device_context_bytes","host_total_bytes",
             "backend_total_bytes","combined_total_bytes"}
    checked(isinstance(value,dict) and set(value)==names, "admission field inventory differs")
    for name,item in value.items():integer(item,name)
    lim=env["limits"];weight=env["merge"]["merged"]["weight"]["size_bytes"]
    copy_bytes=weight if env["backend"]=="metal" else 0
    checked(value["setup_bytes"]==8*MIB and value["mapped_weight_bytes"]==weight and
        value["loader_host_bytes"]==lim["loader_host_bytes"] and value["request_weight_copy_bytes"]==copy_bytes and
        value["request_scratch_bytes"]==lim["request_scratch_bytes"] and
        value["request_host_bytes"]==lim["request_scratch_bytes"]+copy_bytes,
        "admission source/host ownership differs")
    checked(value["host_total_bytes"]==sum(value[key] for key in ("setup_bytes","mapped_weight_bytes","loader_host_bytes","request_host_bytes")) and
        value["combined_total_bytes"]==value["host_total_bytes"]+value["backend_total_bytes"] and
        value["combined_total_bytes"]<=lim["combined_bytes"], "admission total differs")
    if env["backend"]=="native":
        checked(value["device_context_bytes"]==value["backend_total_bytes"]==0,"native admission has device state")
    else:
        checked(value["device_context_bytes"]==value["backend_total_bytes"]==lim["encoder_device_bytes"]+lim["head_device_bytes"],
                "device admission differs")


def validate_ready(value, env, fixture_pin):
    fixed={"event":"ready","scope":SCOPE,"version":1,"qualification":False,"backend":env["backend"],
        "math_policy":MATH_POLICY,"weight_precision":"fp32","activation_precision":"f32",
        "accumulation_precision":"f32","head_precision":"f32","fixture_digest":fixture_pin,
        "inputs_digest":INPUT_PIN,"oracle_report":env["oracle_report"],"merge_receipt":env["merge_receipt"],
        "identity":env["merge"]["merged"],"limits":env["limits"]}
    checked(isinstance(value,dict) and set(value)==set(fixed)|{"build_mode","zig_version","admission"} and
            all(type(value[key]) is type(item) and value[key]==item for key,item in fixed.items()),
            "trained worker ready identity differs")
    checked(value["build_mode"] in ("Debug","ReleaseSafe","ReleaseFast","ReleaseSmall") and
        isinstance(value["zig_version"],str) and 0<len(value["zig_version"])<=128,"invalid worker build identity")
    validate_admission(value["admission"],env)


def validate_metal(value, env):
    if env["backend"]=="native":
        checked(value is None,"native result has Metal claims");return
    fields={"encoder","head","result_download_bytes","proposal_download_bytes","peak_device_upper_bound_bytes"}
    checked(isinstance(value,dict) and set(value)==fields,"missing strict Metal request statistics")
    owner_keys={"peak_device_bytes","charged_weight_bytes","metadata_upload_bytes","proposal_download_bytes",
                "result_download_bytes","proposal_download_calls","result_download_calls","device_dispatches"}
    owners=[]
    for kind in ("encoder","head"):
        owner=value[kind]
        if kind=="head" and owner is None:continue
        checked(isinstance(owner,dict) and set(owner)==owner_keys,"invalid Metal owner statistics")
        for name,item in owner.items():integer(item,kind+"."+name)
        checked(owner["peak_device_bytes"]<=env["limits"][kind+"_device_bytes"] and
                owner["charged_weight_bytes"]<=owner["peak_device_bytes"],"Metal owner exceeded declared device ceiling")
        owners.append(owner)
    checked(value["encoder"]["device_dispatches"]>0 and value["encoder"]["charged_weight_bytes"]>0,
            "Metal result has no encoder dispatch/weight evidence")
    checked(value["encoder"]["proposal_download_bytes"]==value["encoder"]["proposal_download_calls"]==0,
            "encoder owner downloaded proposal data")
    for field in ("result_download_bytes","proposal_download_bytes","peak_device_upper_bound_bytes"):
        integer(value[field],field)
    checked(value["result_download_bytes"]==sum(x["result_download_bytes"] for x in owners)<=env["limits"]["result_download_bytes"] and
        value["proposal_download_bytes"]==sum(x["proposal_download_bytes"] for x in owners)<=env["limits"]["proposal_download_bytes"] and
        value["peak_device_upper_bound_bytes"]==sum(x["peak_device_bytes"] for x in owners), "Metal readback/peak accounting differs")


def coordinates(text, value, native):
    checked(isinstance(value,dict),"source span must be an object")
    start,end=value.get("start"),value.get("end")
    checked(type(start) is int and type(end) is int and 0<=start<end<=len(text),"invalid original codepoint source span")
    if native:
        checked(set(value)=={"start","end","unit","byte_start","byte_end"} and value["unit"]=="unicode_codepoints" and
            type(value["byte_start"]) is int and type(value["byte_end"]) is int and
            value["byte_start"]==len(text[:start].encode()) and value["byte_end"]==len(text[:end].encode()),
            "native UTF-8/codepoint coordinates differ")
    return text[start:end].strip()


def validate_sources(text, raw, canonical, native):
    def walk(value):
        if isinstance(value,dict):
            if {"unit","byte_start","byte_end"} & set(value):coordinates(text,value,True)
            for child in value.values():walk(child)
        elif isinstance(value,list):
            for child in value:walk(child)
    if native:walk(raw)
    def item(value):
        checked(isinstance(value.get("text"),str),"invalid extracted text")
        source=value["source"]
        if source is not None:checked(coordinates(text,source,False)==value["text"],"emitted text does not match original source")
    for group in canonical["entities"]:
        for value in group["values"]:
            checked(value["source"] is not None,"entity has no original source");item(value)
    for group in canonical["structures"]:
        for record in group["instances"]:
            for field in record["fields"]:
                for value in field["values"]:item(value)
    for edge in canonical["relations"]:
        for side in ("head","tail"):
            checked(edge[side]["source"] is not None,"relation endpoint has no original source");item(edge[side])
    # Unmatched branches must not hide NaN or Boolean confidence values.
    compare.compare(canonical,canonical)


def compare_case(result, case, request, report, env, ready, native_result=None):
    fields={"event","case_id","canonical_request_sha256","input_ids","output","request_host_peak_bytes","metal"}
    checked(isinstance(result,dict) and set(result)==fields and result["event"]=="result" and result["case_id"]==case["id"] and
            result["canonical_request_sha256"]==canonical_request_pin(case),"trained case identity differs")
    ids=result["input_ids"]
    checked(isinstance(ids,list) and 0<len(ids)<=512 and all(type(x) is int and 0<=x<2**32 for x in ids),"invalid trained encoder IDs")
    checked(integer(result["request_host_peak_bytes"],"request_host_peak")<=ready["admission"]["request_host_bytes"],"request host ceiling exceeded")
    validate_metal(result["metal"],env)
    actual=bench.canonical_result(result["output"])
    validate_sources(case["text"],result["output"],actual,True)
    for name in ("classification_solver","joint_solver","record_solver"):
        diagnostic=result["output"].get(name)
        if diagnostic is not None:
            checked(isinstance(diagnostic,dict) and diagnostic.get("status") in ("optimal","feasible") and
                    diagnostic.get("exhausted") is False,"native output has no strict solver witness")
    if request["kind"]=="joint_ie":checked(result["output"].get("joint_solver") is not None,"native JointIE omitted solver evidence")
    comparisons=[]
    for phase in merge.PHASES:
        source=next((x for x in report["runtime"]["captures"][phase] if x["id"]==case["id"]),None)
        checked(source is not None,"missing source request")
        expected=bench.canonical_python(request,source["output"])
        validate_sources(case["text"],source["output"],expected,False)
        row=compare.compare(expected,actual)
        row.update(phase=phase,token_ids_equal=source["input_ids"]==ids)
        row["pass"]=row["token_ids_equal"] and row["fp32_reference_tolerance_pass"]
        if request["kind"]=="classification":
            metadata=source["output"].get("_meta",{})
            diagnostic=result["output"].get("classification_solver")
            checked(metadata.get("feasible") is True and metadata.get("violations")==[],"invalid source classification witness")
            valid=isinstance(diagnostic,dict) and diagnostic.get("exhausted") is False and diagnostic.get("status") in ("optimal","feasible")
            exact=valid and (diagnostic["status"]=="optimal")==metadata.get("exact")
            row["classification_solver_equal"]=bool(exact);row["pass"]=row["pass"] and bool(exact)
        comparisons.append(row)
    metadata=compare.backend_result(result["output"])
    backend_comparison=None
    if env["backend"]=="metal":
        checked(native_result is not None,"Metal requires a matching native result")
        backend_comparison=compare.compare_backends(compare.backend_result(native_result["output"]),metadata)
        backend_comparison["token_ids_equal"]=native_result["input_ids"]==ids
        backend_comparison["parity_pass"]=backend_comparison["parity_pass"] and backend_comparison["token_ids_equal"]
    return {"id":case["id"],"canonical_request_sha256":canonical_request_pin(case),"source_span_validity":True,
            "python_comparisons":comparisons,"backend_comparison":backend_comparison,
            "pass":all(row["pass"] for row in comparisons) and (backend_comparison is None or backend_comparison["parity_pass"])}


def validate_complete(value, env, fixture_pin, bytes_before):
    expected={"event":"complete","cases":10,"errors":0,"qualification":False,"fixture_digest":fixture_pin,
        "inputs_digest":INPUT_PIN,"merge_receipt":env["merge_receipt"],"identity":env["merge"]["merged"],
        "output_bytes_before_complete":bytes_before}
    checked(isinstance(value,dict) and set(value)==set(expected)|{"loader_host_peak_bytes"} and
            all(type(value[key]) is type(item) and value[key]==item for key,item in expected.items()),"incomplete trained execution protocol")
    checked(integer(value["loader_host_peak_bytes"],"loader_host_peak")<=env["limits"]["loader_host_bytes"],"loader ceiling exceeded")


def evaluate_events(events, raw_sizes, env, fixture_pin, inputs, requests, oracle_report, native_events=None):
    checked(len(events)==len(raw_sizes)==12,"trained execution needs exactly twelve events")
    checked(sum(raw_sizes)<=env["limits"]["total_output_bytes"] and all(0<x<=env["limits"]["event_bytes"] for x in raw_sizes),"trained event bytes exceed ceiling")
    validate_ready(events[0],env,fixture_pin)
    if native_events is not None:checked(len(native_events)==12,"incomplete native reference events")
    rows=[compare_case(events[index+1],case,requests[index],oracle_report,env,events[0],
            None if native_events is None else native_events[index+1]) for index,case in enumerate(inputs["cases"])]
    validate_complete(events[-1],env,fixture_pin,sum(raw_sizes[:-1]))
    return rows


def event_bytes(path, maximum):
    raw,digest=read(path,maximum)
    lines=raw.splitlines(keepends=True)
    checked(len(lines)==12 and all(line.endswith(b"\n") and 0<len(line)<=MAX_EVENT for line in lines),"invalid persisted event framing")
    return [exports.decode(line) for line in lines],[len(line) for line in lines],digest


def native_reference(path, expected_sha, current, oracle_report, inputs, requests, helpers, binary_pin):
    raw,digest=read(path,MAX_REPORT);checked(compare.is_digest(expected_sha) and digest["sha256"]==expected_sha,"native report SHA differs")
    value=exports.decode(raw)
    checked(value.get("scope")==REPORT_SCOPE and value.get("version")==1 and value.get("status")=="complete" and
        value.get("qualification") is False and value.get("backend")=="native" and value.get("parity_pass") is True and
        value.get("helpers")==helpers and value.get("binary")==binary_pin and value.get("inputs")==INPUT_PIN and
        value.get("driver_runtime")==driver_runtime() and
        value.get("oracle_report")==current["oracle_report"] and value.get("merge_receipt")==current["merge_receipt"] and
        value.get("identity")==current["merge"]["merged"] and value.get("math_policy")==MATH_POLICY,
        "native reference is not a complete matching trained-artifact proof")
    env=envelope("native",value["limits"],current["oracle_report"],{"merged_files":{merge.RECEIPT:current["merge_receipt"]},"merge_receipt":current["merge"]})
    fixture_pin=exports.digest_bytes(encoded(env)+b"\n")
    checked(value.get("execution_fixture")==fixture_pin,"native execution envelope differs")
    events,sizes,events_pin=event_bytes(path.parent/"events.jsonl",MAX_OUTPUT)
    checked(events_pin==value["events"],"native raw event bytes differ")
    process_raw,process_pin=read(path.parent/"process.json",MAX_REPORT);process=exports.decode(process_raw)
    checked(process_pin==value.get("process") and process.get("complete_protocol") is True and
            process.get("events")==12 and process.get("worker_exit_code")==0 and process.get("scratch_cleaned") is True and
            process.get("cleanup_error") is None and process.get("rss_limit_bytes")==RSS_LIMIT and
            type(process.get("peak_worker_rss_bytes")) is int and 0<=process["peak_worker_rss_bytes"]<=RSS_LIMIT,
            "native reference has no complete process/cleanup receipt")
    rows=evaluate_events(events,sizes,env,fixture_pin,inputs,requests,oracle_report)
    checked(rows==value["comparisons"] and all(x["pass"] for x in rows),"native comparison evidence was substituted")
    return events,digest


class Guard(bench.ResourceGuard):
    def __init__(self,deadline):
        super().__init__(RSS_LIMIT);self.deadline=deadline

    def check(self):
        total=0
        for worker in self.workers:
            checked(worker.log_path.stat().st_size<=MAX_STDERR,"trained worker stderr ceiling exceeded")
            # A completed/reaped child no longer owns its former PID. Do not
            # count an unrelated process if that PID is subsequently reused.
            if worker.process is not None and worker.process.poll() is None:
                try:total+=self.psutil.Process(worker.process.pid).memory_info().rss
                except self.psutil.NoSuchProcess:pass
        self.peak_rss_bytes=max(self.peak_rss_bytes,total)
        checked(total<=RSS_LIMIT,"trained worker RSS ceiling exceeded")
        checked(time.monotonic()<=self.deadline,"trained execution absolute deadline exceeded")


def copy_artifact(source,destination,files,guard):
    checked(set(files)=={*exports.SIDECARS,"model.safetensors",merge.RECEIPT},"private copy needs exactly six model files")
    total=sum(item["size_bytes"] for item in files.values())
    checked(total<=MAX_COPY and shutil.disk_usage(destination.parent).free>=total+256*MIB,"trained artifact copy/disk ceiling exceeded")
    destination.mkdir(mode=0o700)
    for name,expected in files.items():
        target=destination/name;target.parent.mkdir(parents=True,exist_ok=True)
        with exports.Opened(source/name,MAX_COPY) as owner,target.open("xb") as output:
            h=hashlib.sha256();size=0
            for block in owner.chunks():
                guard.check();output.write(block);h.update(block);size+=len(block)
            exports.verify_pin({"size_bytes":size,"sha256":h.hexdigest()},expected,"owned trained artifact "+name)
            output.flush();os.fsync(output.fileno())
    return total


def check_copy(directory,files,guard):
    for name,expected in files.items():
        guard.check();exports.verify_pin(pin(directory/name,MAX_COPY),expected,"consumed trained artifact "+name)


class RawWorker(bench.Worker):
    """Preserve consumed bytes; decimal serialization is not a wire-byte oracle."""
    def __init__(self,arm,command,environment,directory,guard):
        self.arm=arm;self.guard=guard;self.log_path=directory/(arm+".stderr.log")
        self.process=None;self.selector=None;self.log=None
        self.buffer=bytearray();self.sequence=0
        # Register ownership before spawning. A failed constructor does not
        # return its object to supervise, but its child must still be reaped
        # before that caller is allowed to remove the private model copy.
        guard.workers.append(self)
        try:
            self.log=self.log_path.open("xb")
            self.selector=selectors.DefaultSelector()
            self.process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,
                env=environment,bufsize=0,start_new_session=True)
            self.selector.register(self.process.stdout,selectors.EVENT_READ)
        except BaseException:
            self.close()
            raise

    def close(self):
        failure=None
        try:
            if self.process is not None and self.process.poll() is None:
                try:os.killpg(self.process.pid,signal.SIGTERM)
                except ProcessLookupError:pass
                try:self.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    try:os.killpg(self.process.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    self.process.wait(timeout=2)
        except BaseException as error:
            failure=error
        # Close every initialized descriptor even when another close fails.
        # Keep process ownership on the object so supervise can retry a failed
        # reap and retain scratch if that second cleanup cannot establish exit.
        streams=() if self.process is None else (self.process.stdin,self.process.stdout)
        for owner in (self.selector,*streams,self.log):
            if owner is not None:
                try:owner.close()
                except BaseException as error:
                    if failure is None:failure=error
        if failure is not None:raise failure

    def receive(self,timeout):
        deadline=time.monotonic()+timeout
        while b"\n" not in self.buffer:
            self.guard.check()
            checked(time.monotonic()<deadline,"trained worker response deadline exceeded")
            if self.selector.select(min(.1,max(0,deadline-time.monotonic()))):
                block=os.read(self.process.stdout.fileno(),65536)
                checked(bool(block),"trained worker exited before a complete response")
                self.buffer.extend(block)
                newline=self.buffer.find(b"\n")
                checked((len(self.buffer) if newline<0 else newline+1)<=MAX_EVENT,"trained worker oversized response")
        line,_,remaining=self.buffer.partition(b"\n")
        self.buffer=bytearray(remaining);self.last_raw=bytes(line)+b"\n"
        return exports.decode(self.last_raw)


def supervise(args,env,fixture_pin,inputs,requests,oracle_report,audit,output,guard,native_events):
    scratch=output/".artifact-scratch";scratch.mkdir(mode=0o700)
    worker=None;events=[];sizes=[];complete=False;copied=0;rows=None
    first_owner=len(guard.workers)
    try:
        copied=copy_artifact(args.model_dir,scratch/"model",audit["merged_files"],guard)
        command=[str(args.binary.resolve()),"--model-dir",str(scratch/"model"),"--fixture",str(output/"execution.json"),
                 "--inputs",str(output/"inputs.json"),"--backend",args.backend]
        environment=os.environ.copy();environment.update({key:"1" for key in bench.THREAD_ENV})
        environment.update(HF_HUB_OFFLINE="1",TRANSFORMERS_OFFLINE="1",TOKENIZERS_PARALLELISM="false")
        worker=RawWorker(args.backend,command,environment,output,guard)
        with (output/"events.jsonl").open("xb") as sink:
            for index in range(12):
                timeout=env["limits"]["startup_timeout_ms" if index==0 else "request_timeout_ms"]/1000
                timeout=min(timeout,guard.deadline-time.monotonic());checked(timeout>0,"trained execution deadline exceeded")
                event=worker.receive(timeout);raw=worker.last_raw
                checked(len(raw)<=env["limits"]["event_bytes"] and sink.tell()+len(raw)<=env["limits"]["total_output_bytes"],"trained output ceiling exceeded")
                sink.write(raw);sink.flush();events.append(event);sizes.append(len(raw))
                if index==0:validate_ready(event,env,fixture_pin)
                elif index<11:compare_case(event,inputs["cases"][index-1],requests[index-1],oracle_report,env,events[0],None if native_events is None else native_events[index])
                else:validate_complete(event,env,fixture_pin,sum(sizes[:-1]))
            os.fsync(sink.fileno())
        until=min(guard.deadline,time.monotonic()+5)
        while worker.process.poll() is None:
            guard.check();checked(time.monotonic()<until,"trained worker did not exit after complete");time.sleep(.05)
        checked(worker.process.returncode==0 and not worker.buffer and not worker.process.stdout.read(1),"trained worker trailing output or failed exit")
        check_copy(scratch/"model",audit["merged_files"],guard)
        rows=evaluate_events(events,sizes,env,fixture_pin,inputs,requests,oracle_report,native_events)
        complete=True
    finally:
        cleanup=None
        try:
            if worker is not None:worker.close()
            # Also covers a child retained by an unfinished RawWorker.__init__.
            # close is idempotent; a constructor cleanup failure gets a retry.
            for index in range(first_owner,len(guard.workers)):
                owned=guard.workers[index];owned.close()
                checked(owned.process is None or owned.process.poll() is not None,
                        "trained worker remains alive during artifact cleanup")
            shutil.rmtree(scratch)
        except BaseException as error:
            cleanup={"type":type(error).__name__,"message":str(error)[:2048]};raise
        finally:
            attempted=worker if worker is not None else (guard.workers[first_owner] if len(guard.workers)>first_owner else None)
            process=None if attempted is None else attempted.process
            merge.atomic_json(output/"process.json",{"scope":REPORT_SCOPE,"qualification":False,"complete_protocol":complete,
                "events":len(events),"copied_bytes":copied,"rss_limit_bytes":RSS_LIMIT,"peak_worker_rss_bytes":guard.peak_rss_bytes,
                "worker_exit_code":None if process is None else process.returncode,
                "scratch_cleaned":not scratch.exists(),"cleanup_error":cleanup},MAX_REPORT)
    return rows


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--variant",choices=("small","base","multi"),required=True)
    p.add_argument("--backend",choices=("native","metal"),required=True)
    for name in ("source-dir","adapter-dir","run-dir","model-dir","merge-job-config","merge-report","binary","output-dir"):
        p.add_argument("--"+name,type=Path,required=True)
    p.add_argument("--merge-report-sha256",required=True)
    p.add_argument("--native-reference-report",type=Path)
    p.add_argument("--native-reference-sha256")
    p.add_argument("--limits",type=Path)
    return p


def run(args):
    contract,contract_pin,inputs,requests,input_bytes=load_contract();helpers=helper_identity(contract,contract_pin)
    limits_raw,limits_pin=read(args.limits,MAX_INPUTS) if args.limits else (None,None)
    resource_limits=limits(exports.decode(limits_raw) if limits_raw is not None else LIMITS)
    checked((args.backend=="metal")==(args.native_reference_report is not None and args.native_reference_sha256 is not None),
            "Metal requires exactly one approved native reference")
    checked(args.backend=="metal" or (args.native_reference_report is None and args.native_reference_sha256 is None),"native execution cannot substitute a reference")
    output=args.output_dir.resolve()
    for directory in (args.source_dir,args.adapter_dir,args.run_dir,args.model_dir):
        checked(not output.is_relative_to(directory.resolve()),"trained evidence must be outside input artifacts")
    checked(not args.output_dir.is_symlink(),"trained output must not be a symlink")
    output.mkdir(mode=0o700,parents=True,exist_ok=False)
    result={"scope":REPORT_SCOPE,"version":1,"qualification":False,"status":"incomplete","backend":args.backend,
        "helpers":helpers,"inputs":INPUT_PIN,"math_policy":MATH_POLICY,"limits":resource_limits,
        "limits_file":limits_pin,"driver_runtime":driver_runtime(),
        "native_probability_map_parity_claimed":False,"quality_evaluation":False}
    guard=Guard(time.monotonic()+resource_limits["total_timeout_ms"]/1000)
    try:
        report,report_pin,audit=admitted_oracle(args,args.merge_report_sha256,requests);guard.check()
        binary_pin=pin(args.binary,256*MIB);checked(os.access(args.binary,os.X_OK),"trained worker is not executable")
        env=envelope(args.backend,resource_limits,report_pin,audit);fixture_pin=exports.digest_bytes(encoded(env)+b"\n")
        result.update(binary=binary_pin,oracle_report=report_pin,merge_receipt=env["merge_receipt"],identity=env["merge"]["merged"],execution_fixture=fixture_pin)
        native_events=native_pin=None
        if args.backend=="metal":native_events,native_pin=native_reference(args.native_reference_report,args.native_reference_sha256,env,report,inputs,requests,helpers,binary_pin)
        result["native_reference"]=native_pin
        merge.atomic_json(output/"execution.json",env,MAX_INPUTS)
        with (output/"inputs.json").open("xb") as destination:
            destination.write(input_bytes);destination.flush();os.fsync(destination.fileno())
        rows=supervise(args,env,fixture_pin,inputs,requests,report,audit,output,guard,native_events)
        guard.check();checked(pin(args.binary,256*MIB)==binary_pin,"trained binary changed during execution")
        checked(admitted_oracle(args,args.merge_report_sha256,requests)==(report,report_pin,audit),"trained input artifacts changed during execution")
        checked(helper_identity(contract,contract_pin)==helpers,"trained helper closure changed during execution")
        checked(result["driver_runtime"]==driver_runtime(),"trained driver interpreter/environment changed")
        refreshed,refreshed_pin,*_=load_contract()
        checked(refreshed==contract and refreshed_pin==contract_pin,"trained contract changed during execution")
        if args.limits:checked(pin(args.limits,MAX_INPUTS)==limits_pin,"explicit resource configuration changed")
        exports.verify_pin(pin(output/"execution.json",MAX_INPUTS),fixture_pin,"consumed execution envelope")
        exports.verify_pin(pin(output/"inputs.json",MAX_INPUTS),INPUT_PIN,"consumed input fixture")
        if native_pin is not None:checked(pin(args.native_reference_report,MAX_REPORT)==native_pin,"native reference changed during Metal execution")
        guard.check()
        result.update(status="complete",comparisons=rows,parity_pass=all(x["pass"] for x in rows),events=pin(output/"events.jsonl",MAX_OUTPUT),process=pin(output/"process.json",MAX_REPORT))
        merge.atomic_json(output/"report.json",result,MAX_REPORT)
    except (Exception,KeyboardInterrupt) as error:
        result.update(status="incomplete",error={"type":type(error).__name__,"message":str(error)[:8192]})
        merge.atomic_json(output/"failure.json",result,MAX_REPORT);raise
    return result


def main():
    def interrupted(signum,_frame):raise KeyboardInterrupt("trained execution interrupted by signal "+str(signum))
    previous=signal.signal(signal.SIGTERM,interrupted)
    try:
        result=run(parser().parse_args())
        print(encoded({"scope":REPORT_SCOPE,"status":result["status"],"parity_pass":result["parity_pass"],"qualification":False}).decode())
        if not result["parity_pass"]:raise SystemExit(1)
    finally:signal.signal(signal.SIGTERM,previous)


if __name__=="__main__":main()
