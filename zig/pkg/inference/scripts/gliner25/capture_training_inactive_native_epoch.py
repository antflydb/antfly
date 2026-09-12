#!/usr/bin/env python3
"""Additive inactive-adapter oracle with an immutable native five-row epoch.

The control profile and captured tensor values remain unchanged.
Only classifier flush positions differ: [2, 4, 5] instead of [2, 3, 5].
No trainer, model, backward, optimizer, or source resume equation is replaced.
"""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
from pathlib import Path
import sys

import oracle

HERE = Path(__file__).resolve().parent
CONTROL_GENERATOR = HERE / "capture_training_inactive_adapters.py"
CONTROL_CONTRACT = HERE / "training_inactive_adapters_contract_v1.json"
CONTRACT = HERE / "training_inactive_native_epoch_contract_v1.json"
SCOPE = "gliner25_inactive_adapter_native_epoch/v1"
CONTROL_PINS = {
    "generator": {"size_bytes": 25119, "sha256": "8f63e2ccb46fcf2ee00033ea3577c02615d59c07b233da7ed9303de0fea45380"},
    "contract": {"size_bytes": 4680, "sha256": "d88ece37ce0873baf481d957a4e30c53b5fb35e5780ea542dc360931d5e810cd"},
}
NATIVE_EPOCH = {
    "version": 1,
    "control_generator": CONTROL_PINS["generator"],
    "control_contract": CONTROL_PINS["contract"],
    "only_numerical_profile_change": "classifier_only.flush_after=[2,4,5]",
    "classifier_window_sizes": [2, 2, 1],
    "epochs": 1,
    "batch_size": 1,
    "shuffle": False,
    "dataset_format": "gliner_boundary_dataset.Row/version1",
    "row_identity": "profile_name + '-' + zero_based_sequence_index",
    "source_resume": "fresh_owner_after_first_microbatch_with_no_post_update_weight_injection",
}
OLD_EPOCH_NOTE = (
    "The first three classifier microbatches form an epoch with a partial flush; "
    "two later inactive batches test an all-zero window."
)
NEW_EPOCH_NOTE = (
    "The five immutable classifier rows form one unshuffled epoch: flush after "
    "rows 2, 4, and 5, with a wholly inactive final partial window."
)


def checked(condition, message):
    if not condition:
        raise oracle.ContractError(message)


def digest(path):
    return {"size_bytes": path.stat().st_size, "sha256": oracle.sha256_file(path)}


def expected_contract():
    checked(digest(CONTROL_GENERATOR) == CONTROL_PINS["generator"], "frozen control generator changed")
    checked(digest(CONTROL_CONTRACT) == CONTROL_PINS["contract"], "frozen control contract changed")
    value = oracle.read_json(CONTROL_CONTRACT)
    checked(value["profiles"]["classifier_only"]["flush_after"] == [2, 3, 5], "unexpected control epoch")
    value["scope"] = SCOPE
    value["profiles"]["classifier_only"]["flush_after"] = [2, 4, 5]
    value["native_epoch"] = copy.deepcopy(NATIVE_EPOCH)
    return value


def load_control():
    """Use a private module owner; never mutate the imported control module."""
    expected = expected_contract()
    checked(oracle.read_json(CONTRACT) == expected, "native epoch contract differs")
    spec = importlib.util.spec_from_file_location("_gliner25_inactive_native_epoch_control", CONTROL_GENERATOR)
    checked(spec is not None and spec.loader is not None, "control loader unavailable")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    # These are declared capture configuration, not replacements of source math.
    helper.CONTRACT = CONTRACT
    helper.SCOPE = SCOPE
    helper.PROFILES = copy.deepcopy(expected["profiles"])
    return helper


def preflight(source):
    helper = load_control()
    helper.preflight(source)
    return helper


def jsonl_rows(helper, family):
    cases = helper.examples()
    rows = []
    for index, name in enumerate(helper.PROFILES[family]["sequence"]):
        case = cases[name]
        row = {"version": 1, "id": family + "-" + str(index), "text": case["text"],
               "schema": json.loads(case["schema_json"]),
               "entities": [{"id": "ada", "type": "person", "span": {
                   "start": 0, "end": 3, "unit": "utf8_bytes"}}]}
        if "classifications" in case["upstream"]:
            task = case["upstream"]["classifications"][0]
            row["classifications"] = [{"task": task["task"], "labels": task["true_label"]}]
        rows.append(row)
    return rows


def input_files(helper):
    return {family + ".jsonl": b"".join(
        (json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()
        for row in jsonl_rows(helper, family)) for family in helper.PROFILES}


def native_settings(helper):
    return {
        "run": {"epochs": 1, "batch_size": 1, "accumulation": 2, "shuffle": False,
                "encoder_lr": 1e-5, "task_lr": 5e-4, "weight_decay": .01,
                "beta1": .9, "beta2": .999, "epsilon": 1e-8, "max_grad_norm": .7,
                "scheduler": "constant", "warmup_steps": 0, "seed": 257713},
        "peft": {"rank": 2, "alpha": 3, "dropout": 0., "mode": "each_of_lora_and_dora",
                 "targets_by_profile": {name: value["targets"] for name, value in helper.PROFILES.items()}},
        "source_step": {"max_gold_per_query": 8, "gold_injection_start": 1., "gold_injection_end": 1.,
                        "gold_injection_hold_frac": 0., "consistency_warmup_steps": 2000,
                        "soft_iou_anneal_steps": 20000, "explicit_dropout": False},
        "source_initialization": "strict_original_training_step_full_weights_then_source_peft_seed257713",
        "native_initialization": "copy_captured_initial_A_B_m_only_before_microbatch_zero",
        "model_config": "capture.config_and_capture.encoder_config_unchanged_from_control_baseline",
        "tokenizer": "training_step/tokenizer.json_and_tokenizer_config.json_bound_by_contract",
        "qualification": False,
    }


def write_inputs(directory, helper):
    files = input_files(helper)
    settings = (json.dumps(native_settings(helper), sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    files["native_settings.json"] = settings
    checked(sum(map(len, files.values())) <= 64 * 1024, "native epoch inputs exceed bounded metadata")
    for name, raw in files.items():
        (directory / name).write_bytes(raw)
    return {name: digest(directory / name) for name in files}


def prepare_inputs(source, destination):
    helper = preflight(source)
    with oracle.atomic_output_directory(destination) as output:
        files = write_inputs(output, helper)
    return {"scope": SCOPE, "status": "inputs_only", "qualification": False, "files": files}


def capture(source, destination):
    helper = preflight(source)
    wrapper_pin = digest(Path(__file__))
    with oracle.atomic_output_directory(destination) as output:
        # The helper publishes into a still-private outer transaction. Annotate
        # the declared profile before any final directory becomes visible.
        intermediate = output / "control_capture"
        result = helper.capture(source, intermediate)
        report = oracle.read_json(intermediate / "capture.json")
        checked(report["scope"] == SCOPE and report["generator"] == CONTROL_PINS["generator"],
                "captured control identity differs")
        checked(report["notes"].count(OLD_EPOCH_NOTE) == 1, "control epoch description changed")
        report["notes"] = [NEW_EPOCH_NOTE if value == OLD_EPOCH_NOTE else value for value in report["notes"]]
        report["capture_wrapper"] = wrapper_pin
        report["profile_adapter"] = copy.deepcopy(NATIVE_EPOCH)
        report["native_inputs"] = write_inputs(output, helper)
        report["native_settings"] = native_settings(helper)
        raw = (json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
        checked(len(raw) <= helper.MAX_METADATA, "native epoch capture metadata ceiling exceeded")
        checked(digest(Path(__file__)) == wrapper_pin, "native epoch wrapper changed during capture")
        preflight(source)
        (output / "capture.json").write_bytes(raw)
        (intermediate / "tensors.safetensors").rename(output / "tensors.safetensors")
        (intermediate / "capture.json").unlink()
        intermediate.rmdir()
    return {"scope": SCOPE, "path": str(destination), "profiles": result["profiles"],
            "peak_rss_bytes": result["peak_rss_bytes"], "qualification": False,
            "files": {path.name: digest(path) for path in sorted(destination.iterdir())}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output-dir", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preflight-only", action="store_true")
    mode.add_argument("--inputs-only", action="store_true")
    args = parser.parse_args()
    if args.preflight_only:
        helper = preflight(args.upstream)
        result = {"scope": SCOPE, "status": "preflight_only", "qualification": False,
                  "profiles": len(helper.PROFILES) * 2, "classifier_flush_after": [2, 4, 5]}
    else:
        checked(args.output_dir is not None, "--output-dir is required")
        result = (prepare_inputs if args.inputs_only else capture)(args.upstream, args.output_dir)
    if args.preflight_only or args.inputs_only:
        checked(not any(name in sys.modules for name in ("torch", "gliner2", "peft")),
                "metadata preparation imported a numerical runtime")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
