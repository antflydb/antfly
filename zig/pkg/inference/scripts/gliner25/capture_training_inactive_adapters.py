#!/usr/bin/env python3
"""Actual pinned trainer semantics for task adapters absent from a microbatch.

No published checkpoint or corpus is loaded. This additive oracle reuses an
immutable tiny baseline, executes the source trainer methods, and distinguishes
missing gradients from explicit zero gradients through accumulation and AdamW.
"""
from __future__ import annotations

import argparse
import ast
import contextlib
import copy
import hashlib
import json
import logging
import math
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import oracle
import capture_training_step as step_capture

HERE = Path(__file__).resolve().parent
CONTRACT = HERE / "training_inactive_adapters_contract_v1.json"
BASELINE = oracle.FIXTURES / "training_step"
SCOPE = "gliner25_inactive_adapter_training/v1"
METHODS = ("_gold_injection_probability", "_soft_iou_anneal_scale", "_backward_one",
           "_install_finite_grad_hooks", "_create_optimizer", "_renormalize_partial_accumulation", "_optimizer_step")
MAX_TENSORS = 8 * 1024**2
MAX_METADATA = 4 * 1024**2
MAX_RSS = 2 * 1024**3
MAX_SECONDS = 120
PROFILES = {
    "classifier_only": {"targets": ["classification_head"],
                        "sequence": ["active_good", "inactive", "active_bad", "inactive", "inactive"],
                        "flush_after": [2, 3, 5], "resume_after": 1},
    "record_only": {"targets": ["record_head"], "sequence": ["inactive"], "flush_after": [1], "resume_after": None},
    "relation_only": {"targets": ["relation_head"], "sequence": ["inactive"], "flush_after": [1], "resume_after": None},
    "encoder_classifier": {"targets": ["encoder", "classification_head"],
                           "sequence": ["active_good", "inactive"], "flush_after": [2], "resume_after": None},
}


def checked(condition, message):
    if not condition:
        raise oracle.ContractError(message)


def digest(path):
    return {"size_bytes": path.stat().st_size, "sha256": oracle.sha256_file(path)}


def trainer_nodes(path):
    text = path.read_text()
    trainer = next(node for node in ast.parse(text).body
                   if isinstance(node, ast.ClassDef) and node.name == "ExtractorTrainer")
    selected = [node for node in trainer.body if isinstance(node, ast.FunctionDef) and node.name in METHODS]
    checked({node.name for node in selected} == set(METHODS), "source trainer method inventory differs")
    pins = {node.name: {"lines": [node.lineno, node.end_lineno],
                       "sha256": hashlib.sha256(ast.get_source_segment(text, node).encode()).hexdigest()}
            for node in selected}
    return selected, pins


def preflight(source):
    checked(not sys.flags.optimize, "upstream assertions must remain enabled")
    contract = oracle.read_json(CONTRACT)
    checked(contract["scope"] == SCOPE and contract["version"] == 1 and contract["qualification"] is False and
            contract["source_commit"] == oracle.UPSTREAM_COMMIT and contract["profiles"] == PROFILES and
            contract["modes"] == ["lora", "dora"] and contract["accumulation_steps"] == 2 and
            contract["max_tensor_bytes"] == MAX_TENSORS and contract["max_metadata_bytes"] == MAX_METADATA and
            contract["max_rss_bytes"] == MAX_RSS and contract["max_seconds"] == MAX_SECONDS,
            "inactive adapter contract differs")
    oracle.verify_upstream_checkout(source)
    checked(digest(Path(oracle.__file__)) == contract["oracle"], "oracle helper changed")
    checked(digest(Path(step_capture.__file__)) == contract["tensor_storage_helper"],
            "tensor storage helper changed")
    for name, expected in contract["baseline"].items():
        checked(digest(BASELINE / name) == expected, "tiny baseline changed: " + name)
    for name, expected in contract["source_files"].items():
        checked(digest(source / name) == expected, "pinned source file changed: " + name)
    nodes, pins = trainer_nodes(source / "gliner2/training/trainer.py")
    checked(pins == contract["trainer_methods"], "pinned trainer methods changed")
    return contract, nodes


def examples():
    def span(start, end):
        return {"start": start, "end": end, "unit": "utf8_bytes"}
    result = {}
    for name, label in (("active_good", "good"), ("inactive", None), ("active_bad", "bad")):
        schema = {"entities": ["person"]}
        upstream = {"entities": {"person": ["Ada"]}}
        annotations = {"schema_fingerprint": [0]*32, "entities": [{"entity_type": 0, "source": span(0, 3)}]}
        if label is not None:
            schema["classifications"] = [{"name": "sentiment", "labels": ["good", "bad"]}]
            upstream["classifications"] = [{"task": "sentiment", "labels": ["good", "bad"], "true_label": [label]}]
            annotations["classifications"] = [{"task": 0, "labels": [0 if label == "good" else 1]}]
        result[name] = {"text": "Ada.", "schema_version": 2,
                        "schema_json": json.dumps(schema, ensure_ascii=False, separators=(",", ":")),
                        "upstream": upstream, "annotations": annotations}
    return result


def capture(source, destination):
    contract, nodes = preflight(source)
    checked(not destination.exists(), "refusing to overwrite inactive adapter capture")
    started = time.monotonic()
    provenance, torch = oracle.prepare_runtime(source)
    import psutil
    from gliner2 import BoundaryExtractor, ExtractorConfig
    from safetensors import safe_open
    from transformers import DebertaV2Config, PreTrainedTokenizerFast

    process = psutil.Process()
    peak_rss = 0

    def guard():
        nonlocal peak_rss
        peak_rss = max(peak_rss, process.memory_info().rss)
        checked(peak_rss <= MAX_RSS, "tiny inactive adapter RSS ceiling exceeded")
        checked(time.monotonic()-started <= MAX_SECONDS, "tiny inactive adapter capture deadline exceeded")

    # Compile the actual methods as a class, preserving their staticmethod
    # decorators and method calls. No fallback or optimizer math is rewritten.
    tree = ast.parse("class SourceTrainer:\n    pass\n")
    tree.body[0].body = copy.deepcopy(nodes)
    namespace = {"torch": torch, "AdamW": torch.optim.AdamW, "contextlib": contextlib,
                 "logger": logging.getLogger(__name__), "dist": torch.distributed}
    exec(compile(tree, str(source / "gliner2/training/trainer.py"), "exec"), namespace)
    SourceTrainer = namespace["SourceTrainer"]
    baseline = step_capture.expand_metadata(oracle.read_json(BASELINE / "capture.json"))
    baseline_full = next(profile for profile in baseline["profiles"] if profile["mode"] == "full")
    with safe_open(str(BASELINE / "tensors.safetensors"), framework="pt", device="cpu") as owner:
        base_weights = {name: owner.get_tensor(key).clone() for name, key in baseline_full["initial"].items()}
    token_options = oracle.read_json(BASELINE / "tokenizer_config.json")
    tensors = {}
    tensor_bytes = 0
    cases = examples()
    fragments = {}

    def save(name, value):
        nonlocal tensor_bytes
        checked(name not in tensors, "duplicate captured tensor: " + name)
        value = value.detach().cpu().contiguous().clone()
        checked(not value.is_floating_point() or bool(torch.isfinite(value).all()), "nonfinite tensor: " + name)
        tensor_bytes += value.numel()*value.element_size()
        checked(tensor_bytes <= MAX_TENSORS, "inactive adapter tensor ceiling exceeded")
        tensors[name] = value
        return name

    def values(prefix, mapping):
        return {name: None if tensor is None else save(prefix+"."+name, tensor) for name, tensor in mapping.items()}

    def scalar(value):
        value = float(value.detach())
        checked(math.isfinite(value), "nonfinite trainer objective")
        return value

    def state_hash(value):
        hashed = hashlib.sha256()
        def visit(item):
            if isinstance(item, torch.Tensor):
                data = item.detach().cpu().contiguous()
                hashed.update(json.dumps([str(data.dtype), list(data.shape)], separators=(",", ":")).encode()+b"\0")
                hashed.update(data.reshape(-1).view(torch.uint8).numpy().tobytes())
            elif isinstance(item, dict):
                hashed.update(b"{")
                for key in sorted(item, key=lambda k: (type(k).__name__, str(k))):
                    visit(key);visit(item[key])
                hashed.update(b"}")
            elif isinstance(item, (list, tuple)):
                hashed.update(b"[")
                for child in item:visit(child)
                hashed.update(b"]")
            else:
                hashed.update(json.dumps(item, allow_nan=False, separators=(",", ":")).encode()+b"\0")
        visit(value)
        return hashed.hexdigest()

    def construct(mode, profile):
        guard();torch.manual_seed(257713)
        tokenizer = PreTrainedTokenizerFast(tokenizer_file=str(BASELINE/"tokenizer.json"), **copy.deepcopy(token_options["special_tokens"]))
        config = ExtractorConfig(**copy.deepcopy(baseline_full["config"]))
        encoder = DebertaV2Config(**copy.deepcopy(baseline_full["encoder_config"]))
        base = BoundaryExtractor(config, encoder_config=encoder, tokenizer=tokenizer, use_flashdeberta=False).float().cpu().train()
        base.load_state_dict(base_weights, strict=True)
        # The source apply_lora path freezes the backbone through PEFT.
        model = base.apply_lora(r=2, alpha=3, dropout=0, targets=profile["targets"], use_dora=mode == "dora")
        model.train()
        trainable = {name: parameter for name, parameter in model.named_parameters() if parameter.requires_grad}
        checked(bool(trainable), "no selected adapter parameters")
        owner = SourceTrainer()
        owner.model=model;owner.device=torch.device("cpu");owner.is_distributed=False
        owner.global_step=0;owner._planned_max_steps=len(profile["flush_after"])
        owner._skip_counter=None;owner._loss_accum=None;owner._loss_finite_flag=None
        owner.config=SimpleNamespace(adam_beta1=.9,adam_beta2=.999,adam_epsilon=1e-8,
            encoder_lr=1e-5,task_lr=5e-4,weight_decay=.01,use_lora=True,fused_optimizer=False,fp16=False,
            gradient_accumulation_steps=2,max_grad_norm=.7,gold_injection_start=1.,gold_injection_end=1.,
            gold_injection_hold_frac=0.,diagnostics_every_n_steps=1,logging_steps=1,log_proposal_metrics=False,
            ddp_consensus_check=True)
        owner.optimizer=owner._create_optimizer()
        owner.scheduler=torch.optim.lr_scheduler.LambdaLR(owner.optimizer, lr_lambda=lambda _step: 1.)
        owner._install_finite_grad_hooks()
        owner.window=0
        return owner,base,trainable

    def snapshot(owner):
        return copy.deepcopy({"model": owner.model.state_dict(), "optimizer": owner.optimizer.state_dict(),
            "gradients": {name: parameter.grad for name, parameter in owner.model.named_parameters()},
            "scheduler": owner.scheduler.state_dict(), "global_step": owner.global_step, "window": owner.window,
            "skip_counter": owner._skip_counter, "loss_accum": owner._loss_accum, "finite_flag": owner._loss_finite_flag,
            "rng_state": torch.get_rng_state()})

    def restore(owner, state):
        owner.model.load_state_dict(state["model"], strict=True);owner.optimizer.load_state_dict(copy.deepcopy(state["optimizer"]))
        owner.scheduler.load_state_dict(copy.deepcopy(state["scheduler"]))
        for name, parameter in owner.model.named_parameters():
            grad = state["gradients"][name];parameter.grad=None if grad is None else grad.clone()
        owner.global_step=state["global_step"];owner.window=state["window"]
        owner._skip_counter=state["skip_counter"].clone();owner._loss_accum=state["loss_accum"].clone()
        owner._loss_finite_flag=state["finite_flag"].clone();torch.set_rng_state(state["rng_state"])

    def release(owner):
        for hook in owner._finite_grad_hook_handles:hook.remove()
        owner._finite_grad_hook_handles=[]
        # Drop the last graph; no model owner is retained by a source output.
        if hasattr(owner, "_last_train_outputs"):del owner._last_train_outputs

    reports = []
    warnings = []
    class Warnings(logging.Handler):
        def emit(self, record):
            if record.levelno >= logging.WARNING:warnings.append(record.getMessage())
    logger=logging.getLogger("gliner2.models.boundary.model");handler=Warnings();logger.addHandler(handler)
    try:
        for mode in ("lora", "dora"):
            for label, profile in PROFILES.items():
                identity=mode+"."+label
                owner,base,trainable=construct(mode,profile)
                frozen_before=state_hash({name:p for name,p in owner.model.named_parameters() if not p.requires_grad})
                report={"id":identity,"mode":mode,"targets":profile["targets"],"sequence":profile["sequence"],
                    "flush_after":profile["flush_after"],"parameters":[{"name":name,"shape":list(p.shape)} for name,p in trainable.items()],
                    "initial":values(identity+".initial",trainable),"microbatches":[],"flushes":[]}
                checkpoint=None

                def execute(index, recording):
                    guard();case=cases[profile["sequence"][index]]
                    batch=base.processor.collate_fn_inference([(case["text"],case["upstream"])],architecture="boundary",
                        build_targets=True,max_len=None,error_policy="raise",max_gold_per_query=8,on_capacity_exceeded="raise")
                    checked(batch.input_ids.shape[1]<=128 and batch.text_word_mask.shape[1]<=8 and batch.query_marker_mask.shape[1]<=8,
                            "inactive adapter request exceeds tiny geometry")
                    seen={};observed={};hooks=[]
                    def output_hook(_module,_inputs,result):
                        observed["model_loss"]=None if result.total_loss is None else scalar(result.total_loss)
                        observed["model_loss_requires_grad"]=result.total_loss is not None and result.total_loss.requires_grad
                        observed["losses"]={name:scalar(value) for name,value in result.losses.items()}
                    hooks.append(base.register_forward_hook(output_hook))
                    for name,parameter in trainable.items():
                        def watch(gradient,name=name):
                            checked(name not in seen,"adapter gradient hook called twice")
                            seen[name]=gradient.detach().clone()
                        hooks.append(parameter.register_hook(watch))
                    try:
                        reported=owner._backward_one(batch,index,False,torch.float32,
                            is_last_micro=index+1 in profile["flush_after"])
                    finally:
                        for hook in hooks:hook.remove()
                    checked(not warnings,"source dropped an auxiliary loss: "+repr(warnings))
                    checked(bool(owner._loss_finite_flag),"source zeroed a nonfinite objective")
                    owner.window+=1
                    current={name:seen.get(name) for name in trainable}
                    inactive=profile["sequence"][index]=="inactive"
                    fallback=not observed["model_loss_requires_grad"]
                    expected_fallback=label=="classifier_only" and inactive
                    checked(fallback==expected_fallback,"unexpected no-gradient fallback predicate")
                    checked(observed["model_loss"] is not None and observed["model_loss"]>0,"missing positive frozen-task objective")
                    checked(scalar(reported)==(0. if fallback else observed["model_loss"]),"reported source objective changed")
                    for name,gradient in current.items():
                        expected_none=label=="encoder_classifier" and inactive and ".classifier." in name
                        checked((gradient is None)==expected_none,"unexpected gradient presence: "+name)
                        if gradient is not None:
                            checked(bool(torch.isfinite(gradient).all()),"nonfinite adapter gradient")
                            if inactive and label!="encoder_classifier":
                                checked(not bool(torch.count_nonzero(gradient)),"inactive touched adapter acquired a nonzero gradient")
                    if recording:
                        prefix=identity+f".micro{index}"
                        report["microbatches"].append({"index":index,"case":profile["sequence"][index],
                            "global_step_before":owner.global_step,"window_after_backward":owner.window,
                            **observed,"reported_loss":scalar(reported),"fallback":fallback,
                            "gradients_scaled":values(prefix+".scaled",current),
                            "gradients_unscaled":values(prefix+".unscaled",{name:None if g is None else g*2 for name,g in current.items()}),
                            "accumulated_gradients":values(prefix+".accumulated",{name:p.grad for name,p in trainable.items()}),
                            "inputs":values(prefix+".input",{name:getattr(batch,name) for name in
                                ("input_ids","attention_mask","text_word_indices","text_word_mask","query_marker_indices",
                                 "query_marker_mask","cls_marker_indices","cls_marker_mask")}),
                            "schema_tokens_list":batch.schema_tokens_list,"text_tokens":batch.text_tokens,
                            "task_types":batch.task_types,"structure_labels":batch.structure_labels})
                        for group in batch.schema_tokens_list[0]:
                            for fragment in group:fragments[fragment]=base.processor.tokenizer.encode(fragment,add_special_tokens=False)
                        for fragment in batch.text_tokens[0]:fragments[fragment]=base.processor.tokenizer.encode(fragment,add_special_tokens=False)
                    if index+1 in profile["flush_after"]:
                        micro=owner.window;owner._renormalize_partial_accumulation(micro)
                        before={name:None if p.grad is None else p.grad.detach().clone() for name,p in trainable.items()}
                        checked(owner._optimizer_step() is True,"source failed to advance optimizer")
                        owner.global_step+=1;owner.window=0
                        if recording:
                            prefix=identity+f".flush{owner.global_step}"
                            report["flushes"].append({"after_microbatch":index+1,"microbatches":micro,
                                "partial_renormalization":2/micro,"global_step":owner.global_step,
                                "scheduler_last_epoch":owner.scheduler.last_epoch,"grad_norm":scalar(owner._last_grad_norm),
                                "gradients_before_clip":values(prefix+".before_clip",before),
                                "parameters":{name:{"weight":save(prefix+".weight."+name,p),
                                    "state_present":p in owner.optimizer.state,
                                    "step":int(owner.optimizer.state.get(p,{}).get("step",0)),
                                    "exp_avg":None if p not in owner.optimizer.state else save(prefix+".m."+name,owner.optimizer.state[p]["exp_avg"]),
                                    "exp_avg_sq":None if p not in owner.optimizer.state else save(prefix+".v."+name,owner.optimizer.state[p]["exp_avg_sq"])}
                                    for name,p in trainable.items()}})
                    guard()

                for index in range(len(profile["sequence"])):
                    execute(index,True)
                    if index+1==profile["resume_after"]:checkpoint=snapshot(owner)
                expected=snapshot(owner)
                checked(frozen_before==state_hash({name:p for name,p in owner.model.named_parameters() if not p.requires_grad}),
                        "frozen source parameter changed")
                report["frozen_parameters_unchanged"]=True
                report["final_state_sha256"]=state_hash(expected)
                release(owner)
                if checkpoint is not None:
                    owner=None;base=None;trainable=None
                    owner,base,trainable=construct(mode,profile)
                    restore(owner,checkpoint)
                    report["resume_snapshot_sha256"]=state_hash(checkpoint)
                    checked(state_hash(snapshot(owner))==state_hash(checkpoint),"fresh-owner restore state differs")
                    for index in range(profile["resume_after"],len(profile["sequence"])):execute(index,False)
                    checked(state_hash(snapshot(owner))==state_hash(expected),"fresh-owner resumed final state differs")
                    report["fresh_owner_mid_window_resume_exact"]=True
                    release(owner)
                else:report["fresh_owner_mid_window_resume_exact"]=None
                reports.append(report)
                owner=None;base=None;trainable=None;checkpoint=None;expected=None
    finally:logger.removeHandler(handler)
    guard();preflight(source)
    with oracle.atomic_output_directory(destination) as output:
        initial=values("base.initial",base_weights)
        retained,aliases=step_capture.deduplicate_tensors(tensors)
        tensor_report=oracle.save_tensors(output/"tensors.safetensors",retained,torch)
        tensor_report.pop("tensors")  # Shapes and dtypes live in the pinned SafeTensors header.
        report={"version":1,"scope":SCOPE,"qualification":False,"source_commit":oracle.UPSTREAM_COMMIT,
            "provenance":provenance,"contract":digest(CONTRACT),"generator":digest(Path(__file__)),
            "tensor_storage_helper":digest(Path(step_capture.__file__)),
            "baseline":contract["baseline"],"trainer_methods":contract["trainer_methods"],
            "config":baseline_full["config"],"encoder_config":baseline_full["encoder_config"],
            "base_parameters":initial,"cases":cases,"tokenizer_fragments":fragments,
            "optimizer":{"accumulation_steps":2,"max_grad_norm":.7,"betas":[.9,.999],"eps":1e-8,
                "task_lr":5e-4,"weight_decay":.01,"foreach":True,"scheduler":"constant_LambdaLR",
                "rank":2,"alpha":3,"dropout":0.,"gradient_unscale":"exact_factor_two_of_observed_source_backward_hook"},
            "profiles":reports,"tensors":tensor_report,
            "notes":["Synthetic H16 baseline only; no published checkpoint or corpus is loaded.",
                "Exact pinned Trainer._backward_one executes the no-grad fallback, finite masking and accumulation division.",
                "Classifier-only wholly inactive batches report zero and zero-touch every selected adapter slot.",
                "Record/relation optional-head touches retain the positive frozen-task objective and zero-present slot gradients.",
                "When another selected path is live, the absent classifier remains None rather than zero.",
                "Zero-present slots still decay weights/moments and advance AdamW steps; None remains absent.",
                "The first three classifier microbatches form an epoch with a partial flush; two later inactive batches test an all-zero window.",
                "Fresh-owner in-memory resume compares full parameters, gradient presence, moments, counters and RNG bytes exactly. Durable native publication is separate.",
                "No native training, optimizer, quality, convergence or release qualification is implied by source capture."]}
        report=step_capture.consolidate_metadata(step_capture.resolve_tensor_aliases(report,aliases))
        raw=(json.dumps(report,ensure_ascii=False,sort_keys=True,indent=2,allow_nan=False)+"\n").encode()
        checked(len(raw)<=MAX_METADATA,"inactive adapter metadata ceiling exceeded")
        (output/"capture.json").write_bytes(raw)
    return {"path":str(destination),"profiles":len(reports),"tensors":digest(destination/"tensors.safetensors"),
            "capture":digest(destination/"capture.json"),"peak_rss_bytes":peak_rss,"qualification":False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream",type=Path,default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output-dir",type=Path)
    parser.add_argument("--preflight-only",action="store_true")
    args=parser.parse_args()
    if args.preflight_only:
        contract,_=preflight(args.upstream)
        checked(not any(name in sys.modules for name in ("torch","gliner2","peft")),"preflight imported a numerical runtime")
        print(json.dumps({"scope":SCOPE,"qualification":False,"status":"preflight_only","profiles":len(contract["profiles"])*2}))
    else:
        checked(args.output_dir is not None,"--output-dir is required for capture")
        print(json.dumps(capture(args.upstream,args.output_dir),sort_keys=True))


if __name__ == "__main__":main()
