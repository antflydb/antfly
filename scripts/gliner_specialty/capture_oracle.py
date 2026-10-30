#!/usr/bin/env python3
"""Capture exact pinned GLiNER2 specialty preprocessing, heads, and outputs."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.metadata
import importlib.util
import inspect
import json
import platform
import sys
from pathlib import Path

UPSTREAM_COMMIT = "3c913c7369301133d3b7699252074c4303ada50e"
SOURCE_TREE_SHA256 = "042ea6064ddb439ccff6c6ac5eb7850dac6d9cc74bc33cbef40928e8f653b020"
UPSTREAM_VERSION = "2.0.0"
LOCKED_PACKAGES = {
    "PyYAML":"6.0.3", "accelerate":"1.10.1", "huggingface-hub":"0.36.0",
    "numpy":"2.3.5", "packaging":"25.0", "peft":"0.17.1",
    "psutil":"7.1.3", "pydantic":"2.12.3", "requests":"2.32.5",
    "safetensors":"0.7.0", "scipy":"1.16.3", "tokenizers":"0.21.4",
    "torch":"2.9.1", "transformers":"4.55.4", "urllib3":"2.5.0",
}
MODEL_PINS = {
    "pii_multi": {"directory":"gliner2-privacy-filter-PII-multi","repo":"fastino/gliner2-privacy-filter-PII-multi","revision":"1cb4166094dc58fa8d836429f060d6c95f62b495","size":1228421964,"sha256":"0280f6f39f6012da50b6640bad438d9b7e763a1b0102094115d1b710c4dd79b6","marker_id":250106},
    "gliguard": {"directory":"gliguard-LLMGuardrails-300M","repo":"fastino/gliguard-LLMGuardrails-300M","revision":"7776816b54581f37b90dd2de12dbe13123229a9b","size":833938108,"sha256":"03132cef1def4bb172b586e662ffc3c30f76034411fc21cd309dd8f1b627c466","marker_id":128007},
}

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""): h.update(chunk)
    return h.hexdigest()

def tree_sha256(root: Path) -> str:
    h = hashlib.sha256()
    for path in sorted((root / "gliner2").rglob("*.py")):
        h.update(path.relative_to(root).as_posix().encode()); h.update(b"\0")
        h.update(path.read_bytes()); h.update(b"\0")
    return h.hexdigest()

def stable(value):
    return float(f"{float(value):.9g}")

def tensor_record(tensor):
    value = tensor.detach().cpu().float().contiguous()
    return {"shape":list(value.shape),"values":[stable(x) for x in value.flatten().tolist()]}

def verify_environment(source_root: Path, model_root: Path, profile: str, verify_weight: bool = True) -> dict:
    if sys.version_info[:3] != (3, 12, 3): raise RuntimeError(f"oracle Python mismatch: {platform.python_version()}")
    actual = {name:importlib.metadata.version(name) for name in LOCKED_PACKAGES}
    if actual != LOCKED_PACKAGES: raise RuntimeError(f"oracle dependency mismatch: {actual}")
    source_digest = tree_sha256(source_root)
    if source_digest != SOURCE_TREE_SHA256: raise RuntimeError(f"GLiNER2 source mismatch: {source_digest}")
    pin = MODEL_PINS[profile]; model_dir = model_root / pin["directory"]
    if verify_weight:
        weight = model_dir / "model.safetensors"
        if weight.stat().st_size != pin["size"] or sha256(weight) != pin["sha256"]: raise RuntimeError(f"{profile} weight identity mismatch")
    return {"python":platform.python_version(),"packages":actual,"source_tree_sha256":source_digest}

def verify_local_oracle_modules() -> dict:
    import accelerate
    import peft
    prefix = Path(sys.prefix).resolve()
    result = {}
    for name, module in (("accelerate", accelerate), ("peft", peft)):
        path = Path(module.__file__).resolve()
        if prefix not in path.parents: raise RuntimeError(f"{name} was not imported from the worktree-local oracle environment: {path}")
        result[name] = path.relative_to(prefix).as_posix()
    return result

def render_document(document: dict) -> str:
    root = Path(__file__).resolve().parents[2]
    formatter_path = root / "zig/pkg/inference/scripts/gliner25/format_family_captures.py"
    spec = importlib.util.spec_from_file_location("gliner25_capture_formatter", formatter_path)
    if spec is None or spec.loader is None: raise RuntimeError(f"cannot load capture formatter: {formatter_path}")
    formatter = importlib.util.module_from_spec(spec); spec.loader.exec_module(formatter)
    return formatter.render(document) + "\n"

def preprocessing(batch, marker_id: int) -> dict:
    active = int(batch.attention_mask[0].sum().item()); ids = batch.input_ids[0,:active].tolist()
    return {"active_input_ids":ids,"marker_token_id":marker_id,"marker_positions":[i for i,x in enumerate(ids) if x == marker_id],"query_marker_positions":batch.query_marker_indices[0,batch.query_marker_mask[0]].tolist(),"classification_marker_positions":batch.cls_marker_indices[0,batch.cls_marker_mask[0]].tolist(),"text_word_positions":batch.text_word_indices[0,batch.text_word_mask[0]].tolist(),"text_tokens":batch.text_tokens[0],"offset_unit":"unicode_codepoints","offset_end":"exclusive","start_offsets":batch.start_mappings[0],"end_offsets":batch.end_mappings[0],"schema_tokens":batch.schema_tokens_list[0]}

def capture_pii(model, cases):
    import torch
    canonical_labels = cases["pii_labels"]; output = []
    for case in cases["pii_cases"]:
        labels = case.get("labels", canonical_labels)
        entity_thresholds = case.get("entity_thresholds", {})
        unknown_thresholds = set(entity_thresholds) - set(labels)
        if unknown_thresholds:
            raise RuntimeError(f"{case['id']}: thresholds for labels outside the effective schema: {sorted(unknown_thresholds)}")
        entity_types = {
            label: ({"threshold": entity_thresholds[label]} if label in entity_thresholds else {})
            for label in labels
        }
        request_options = case.get("request_options", {})
        allowed_options = {"threshold", "overlap", "offset_unit"}
        unknown_options = set(request_options) - allowed_options
        if unknown_options:
            raise RuntimeError(f"{case['id']}: unsupported public request options: {sorted(unknown_options)}")
        threshold = float(request_options.get("threshold", 0.5))
        overlap_policy = request_options.get("overlap")
        offset_unit = request_options.get("offset_unit", "unicode_codepoints")
        if offset_unit != "unicode_codepoints":
            raise RuntimeError(f"{case['id']}: Python oracle offsets are Unicode code points, not {offset_unit!r}")
        schema = model.create_schema().entities(entity_types); built = schema.build()
        batch = model.processor.collate_fn_inference([(case["text"],copy.deepcopy(built))],architecture="span",error_policy="raise")
        observed = []; original = model._extract_span_result
        def observe(*args, **kwargs):
            bound = inspect.signature(original).bind(*args, **kwargs).arguments
            embs = bound["embs"]; count_logits = model.count_pred(embs[0].unsqueeze(0)); predicted_count = int(count_logits.argmax(1).item())
            head = {"task_type":bound["task_type"],"schema_name":bound["schema_name"],"count_logits":tensor_record(count_logits),"predicted_count":predicted_count}
            if predicted_count > 0 and bound["span_info"] is not None:
                projected = model.count_embed(embs[1:], predicted_count)
                raw = torch.einsum("lkd,bpd->bplk",bound["span_info"]["span_rep"],projected)
                head["raw_span_logits"] = tensor_record(raw)
            observed.append(head); return original(*args, **kwargs)
        model._extract_span_result = observe
        try:
            presented = model.extract_entities(
                case["text"], entity_types, threshold=threshold,
                include_confidence=True, include_spans=True,
                overlap_policy=overlap_policy,
            )
        finally: model._extract_span_result = original
        captured_case = {**case,"threshold":threshold,"labels":labels,"preprocessing":preprocessing(batch,MODEL_PINS["pii_multi"]["marker_id"]),"heads":observed,"presented_output":presented}
        if not entity_thresholds: captured_case.pop("entity_thresholds", None)
        if not request_options: captured_case.pop("request_options", None)
        output.append(captured_case)
        nonempty = sum(bool(values) for values in presented.get("entities", {}).values())
        print(f"pii {case['id']}: tokens={len(output[-1]['preprocessing']['active_input_ids'])} count={observed[0]['predicted_count']} nonempty_labels={nonempty}",flush=True)
    return output

def normalized_tasks(all_tasks, names):
    return {name:copy.deepcopy(all_tasks[name]) for name in names}

def task_config(value):
    return value if isinstance(value,dict) else {"labels":value,"multi_label":False}

def capture_gliguard(model, cases):
    import torch
    output = []
    for case in cases["gliguard_cases"]:
        tasks = normalized_tasks(cases["gliguard_tasks"],case["tasks"]); schema = model._classification_schema(tasks); built = schema.build()
        batch = model.processor.collate_fn_inference([(case["text"],copy.deepcopy(built))],architecture="span",error_policy="raise")
        raw = []
        handle = model.classifier.register_forward_hook(lambda _m,_a,result: raw.append(result.detach().cpu()))
        try: presented = model.classify_text(case["text"],tasks,threshold=0.5)
        finally: handle.remove()
        if len(raw) != len(tasks): raise RuntimeError(f"{case['id']}: classifier head count mismatch")
        heads = []
        for (name,value),logits_tensor in zip(tasks.items(),raw):
            config = task_config(value); logits = logits_tensor.squeeze(-1).float(); multi = bool(config.get("multi_label",False)); activation = config.get("class_act","auto")
            probs = torch.sigmoid(logits) if activation == "sigmoid" or (activation == "auto" and multi) else torch.softmax(logits,dim=-1)
            heads.append({"task":name,"labels":config["labels"],"multi_label":multi,"activation":"sigmoid" if multi and activation == "auto" else ("softmax" if activation == "auto" else activation),"threshold":config.get("cls_threshold",0.5),"raw_logits":tensor_record(logits),"probabilities":tensor_record(probs)})
        output.append({**case,"threshold":0.5,"tasks_config":tasks,"preprocessing":preprocessing(batch,MODEL_PINS["gliguard"]["marker_id"]),"heads":heads,"presented_output":presented})
        print(f"gliguard {case['id']}: tokens={len(output[-1]['preprocessing']['active_input_ids'])} tasks={len(tasks)}",flush=True)
    return output

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--profile",choices=tuple(MODEL_PINS),required=True); parser.add_argument("--source-root",type=Path,required=True); parser.add_argument("--model-root",type=Path,required=True); parser.add_argument("--cases",type=Path); parser.add_argument("--output",type=Path); parser.add_argument("--check",action="store_true"); parser.add_argument("--verify-environment-only",action="store_true"); args = parser.parse_args()
    if not args.verify_environment_only and (args.cases is None or args.output is None): parser.error("--cases and --output are required for capture")
    receipt = verify_environment(args.source_root,args.model_root,args.profile,verify_weight=not args.verify_environment_only)
    sys.path.insert(0,str(args.source_root.resolve()))
    import torch
    torch.set_num_threads(2); torch.set_num_interop_threads(1); torch.manual_seed(0); torch.use_deterministic_algorithms(True)
    import gliner2
    from gliner2 import AutoExtractor
    imported = Path(sys.modules["gliner2"].__file__).resolve()
    if args.source_root.resolve() not in imported.parents: raise RuntimeError(f"wrong gliner2 imported: {imported}")
    if gliner2.__version__ != UPSTREAM_VERSION: raise RuntimeError(f"wrong GLiNER2 source version: {gliner2.__version__}")
    local_modules = verify_local_oracle_modules()
    if args.verify_environment_only:
        print(json.dumps({**receipt,"gliner2_version":gliner2.__version__,"gliner2_module":imported.relative_to(args.source_root.resolve()).as_posix(),"worktree_local_modules":local_modules},sort_keys=True))
        return 0
    cases = json.loads(args.cases.read_text()); pin = MODEL_PINS[args.profile]; model_dir = args.model_root / pin["directory"]
    model = AutoExtractor.from_pretrained(str(model_dir),map_location="cpu").eval()
    captured = capture_pii(model,cases) if args.profile == "pii_multi" else capture_gliguard(model,cases)
    document = {"format_version":1,"profile":args.profile,"model":{k:v for k,v in pin.items() if k != "marker_id"},"upstream_commit":UPSTREAM_COMMIT,"runtime":{**receipt,"device":"cpu","dtype":"float32","torch_threads":2,"deterministic_algorithms":True,"gliner2_version":gliner2.__version__,"gliner2_module":imported.relative_to(args.source_root.resolve()).as_posix(),"worktree_local_modules":local_modules},"cases":captured}
    serialized = render_document(document)
    if args.check:
        if args.output.read_text() != serialized: raise RuntimeError(f"oracle fixture drift: {args.output}")
        print(f"verified {args.output} sha256={sha256(args.output)}",flush=True)
    else:
        args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(serialized); print(f"wrote {args.output} sha256={sha256(args.output)}",flush=True)
    return 0

if __name__ == "__main__": raise SystemExit(main())
