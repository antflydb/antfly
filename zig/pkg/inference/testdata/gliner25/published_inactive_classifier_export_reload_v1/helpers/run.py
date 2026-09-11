#!/usr/bin/env python3
"""One frozen classifier export reload; --preflight-only imports no model runtime."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import sys
from types import ModuleType

ROOT = Path(__file__).resolve().parent
PLAN_PIN = {"size_bytes": 26716, "sha256": "bb2a3e805ebf1fdb2ac79304c91b438a782f3c5dd9120f949bf04e7dae0b38b1"}
SCOPE = "gliner25_published_inactive_classifier_export_reload_supervision/v1"
MiB = 1024**2
FAMILIES = ("torch", "gliner2", "peft")
THREAD_ENV = {
    "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1", "PYTHONDONTWRITEBYTECODE": "1",
    "TOKENIZERS_PARALLELISM": "false", "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1", "VECLIB_MAXIMUM_THREADS": "1", "BLIS_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1", "PYTHONPATH": "",
}


def require(value, reason):
    if not value:
        raise ValueError(reason)


def pin(raw):
    return {"size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def file_pin(path, maximum=512 * MiB, *, contents=False):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= maximum, "not a bounded regular file: " + str(path))
        digest, count, chunks = hashlib.sha256(), 0, []
        while chunk := stream.read(MiB):
            count += len(chunk)
            require(count <= maximum, "input grew beyond bound: " + str(path))
            digest.update(chunk)
            if contents:
                chunks.append(chunk)
        after = os.fstat(stream.fileno())
    require(count == before.st_size and all(getattr(before, k) == getattr(after, k)
            for k in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")), "input changed during read: " + str(path))
    identity = {"size_bytes": count, "sha256": digest.hexdigest()}
    return (identity, b"".join(chunks)) if contents else identity


def exact(path, expected, *, contents=False, maximum=512 * MiB):
    result = file_pin(path, maximum, contents=contents)
    observed = result[0] if contents else result
    require(observed == {key: expected[key] for key in ("size_bytes", "sha256")}, "frozen input differs: " + str(path))
    return result[1] if contents else observed


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result
    def nonfinite(value):
        raise ValueError("nonfinite JSON: " + value)
    return json.loads(raw, object_pairs_hook=unique, parse_constant=nonfinite)


def read_json(path, maximum=4 * MiB):
    return decode(file_pin(path, maximum, contents=True)[1])


def write_json(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    require(len(raw) <= 4 * MiB, "receipt output exceeds bound")
    with Path(path).open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return pin(raw)


def load_plan():
    plan = decode(exact(ROOT / "plan.json", PLAN_PIN, contents=True, maximum=MiB))
    require(plan["version"] == 1 and plan["qualification"] is False and plan["runtime_executed"] is False,
            "not the approved preparation-only plan")
    return plan


def input_pins(plan, entry):
    actual = {}
    for row in plan["helper_closure"].values():
        for name in ("original", "snapshot"):
            actual[row[name]] = exact(row[name], row, maximum=4 * MiB)
    for name in ("original", "snapshot"):
        row = plan["supervisor"]
        actual[row[name]] = exact(row[name], row, maximum=MiB)
    for name in ("wheel", "python_executable", "pyvenv_cfg", "requests"):
        row = plan[name]
        actual[row["path"]] = exact(row["path"], row)
    for name, expected in plan["source_files"].items():
        path = str(Path(plan["source_dir"]) / name)
        actual[path] = exact(path, expected)
    for path, expected in entry["original_files"].items():
        actual[path] = exact(path, expected, maximum=MiB)
    for name in ("static_audit", "training_ledger"):
        row = entry[name]
        actual[row["path"]] = exact(row["path"], row, maximum=MiB)
    for row in plan["prior_all_target_export_proofs"].values():
        actual[row["path"]] = exact(row["path"], row, maximum=MiB)
    return actual


def modules(plan):
    path = str(ROOT / "checker/scripts/gliner25")
    if path not in sys.path:
        sys.path.insert(0, path)
    import oracle
    import training_export_runtime
    for module in (oracle, training_export_runtime):
        require(Path(module.__file__).resolve().parent == Path(path), "helper imported outside frozen owner")
    return oracle, training_export_runtime


def preflight(name):
    plan = load_plan()
    require(name in plan["profiles"], "unknown fixed profile")
    require(os.path.abspath(sys.executable) == plan["python_invocation"], "use the pinned lexical virtualenv invocation")
    entry = plan["profiles"][name]
    identities = input_pins(plan, entry)
    oracle, runtime = modules(plan)
    require(oracle.verify_dependencies() == plan["original_runtime"], "original numerical dependencies differ")
    require(oracle.verify_upstream_checkout(Path(plan["upstream"]["checkout"])) == plan["upstream"], "upstream source differs")
    profile = runtime.load_profile()
    runtime.wheel_bytes(Path(plan["wheel"]["path"]), profile)
    require(len(profile["wheel_files"]) == plan["wheel"]["entries"] and
            sum(value["size_bytes"] for value in profile["wheel_files"].values()) == plan["wheel"]["uncompressed_bytes"],
            "isolated wheel inventory differs")
    require(not any(name == family or name.startswith(family + ".") for name in sys.modules for family in FAMILIES),
            "preflight imported a numerical runtime")
    static = read_json(entry["static_audit"]["path"])
    require(static["job"]["result_state_sha256"] == entry["state_sha256"] and
            static["provenance"]["optimizer_identity"] == entry["expected_optimizer_identity"], "final job identity differs")
    require(entry["runtime_copy_bytes"] + entry["additional_request_copy_bytes"] <= plan["max_runtime_copy_bytes"], "copy ceiling exceeded")
    require(shutil.disk_usage(ROOT).free >= plan["max_artifact_bytes"] + 256 * MiB, "insufficient disk headroom")
    return plan, entry, static, identities


def tree_sizes(root, maximum, *, max_entries=512):
    """Inspect only a private output tree; never follow symlinks or open FIFOs."""
    sizes, entries = {}, [0]
    def walk(fd, prefix, depth):
        require(depth <= 12, "artifact nesting exceeds bound")
        with os.scandir(fd) as children:
            for child in children:
                entries[0] += 1
                require(entries[0] <= max_entries, "artifact entry count exceeds bound")
                name = prefix + child.name
                try:
                    info = child.stat(follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        nested = os.open(child.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                        try:
                            walk(nested, name + "/", depth + 1)
                        finally:
                            os.close(nested)
                    else:
                        require(stat.S_ISREG(info.st_mode), "nonregular artifact: " + name)
                        sizes[name] = info.st_size
                        require(0 <= info.st_size <= maximum and sum(sizes.values()) <= maximum, "artifact byte ceiling exceeded")
                except FileNotFoundError:
                    # The checker atomically renames/removes its own staging.
                    continue
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        walk(descriptor, "", 0)
    finally:
        os.close(descriptor)
    return sizes


def artifact_tracker(supervisor, directory, maximum):
    class ArtifactTree(supervisor.ProcessTree):
        def __init__(self, *args):
            super().__init__(*args)
            self.inspect_artifacts = True
            self.artifact_peak = 0

        def sample(self):
            rss = super().sample()
            if self.inspect_artifacts:
                self.artifact_peak = max(self.artifact_peak, sum(tree_sizes(directory, maximum).values()))
            return rss

        def cleanup_sample(self):
            self.inspect_artifacts = False
            return super().cleanup_sample()

        def receipt(self):
            return {**super().receipt(), "sampled_artifact_peak_bytes": self.artifact_peak,
                    "artifact_byte_ceiling": maximum}
    return ArtifactTree


def cleanup_unpublished_copies(directory, process, maximum):
    require(process["cleanup"]["complete"], "cannot clean scratch before complete owned-process reaping")
    require(directory.parent == ROOT / "execution" and directory.name in load_plan()["profiles"], "not a helper-owned execution directory")
    tree_sizes(directory, maximum)
    deleted = []
    # Never delete the published report, top-level partial captures, source,
    # adapters, checkpoints, logs, or any pre-existing evidence directory.
    for outer in directory.iterdir():
        if outer.name != "report" and not re.fullmatch(r"\.report-[A-Za-z0-9_-]+", outer.name):
            continue
        require(outer.is_dir() and not outer.is_symlink(), "invalid owned report stage")
        for candidate in outer.iterdir():
            if not (re.fullmatch(r"\.training-export-runtime-[A-Za-z0-9_-]+", candidate.name) or
                    re.fullmatch(r"\.peft018-wheel-[A-Za-z0-9_-]+", candidate.name)):
                continue
            require(candidate.is_dir() and not candidate.is_symlink(), "invalid owned runtime scratch")
            sizes = tree_sizes(candidate, maximum)
            deleted.append({"path": str(candidate), "bytes": sum(sizes.values()), "files": len(sizes)})
            shutil.rmtree(candidate)
    remaining = tree_sizes(directory, maximum)
    require(not any(part.startswith((".training-export-runtime-", ".peft018-wheel-"))
                    for path in remaining for part in Path(path).parts), "private runtime copy remains")
    return deleted


def validate_report_content(report, static, entry, plan):
    base = dict(report)
    runtime = base.pop("runtime")
    require(base.pop("numerical_runtime_executed") is True, "runtime was not executed")
    base["numerical_runtime_executed"] = False
    require(base == static, "runtime static/source/export/checkpoint proof differs from admitted bytes")
    require(runtime["status"] == "loaded" and runtime["all_loaded_tensor_bytes_equal"] is True and
            runtime["missing_weight_fallback"] is False and runtime["network_allowed"] is False and
            runtime["quality_evaluation"] is False, "runtime loader guarantee failed")
    require(runtime["loader_profile"] == plan["loader_profile"] and
            runtime["private_copy_bytes"] == entry["runtime_copy_bytes"], "loader/copy profile differs")
    require(runtime["requests"] == {k: plan["requests"][k] for k in ("size_bytes", "sha256")}, "request bytes differ")
    provenance = runtime["provenance"]
    require(provenance["commit"] == plan["upstream"]["commit"] and provenance["checkout"] == plan["upstream"]["checkout"] and
            provenance["runtime"] == plan["isolated_runtime"] and provenance["device"] == "cpu" and
            provenance["dtype"] == "float32" and provenance["threads"] == 1 and
            provenance["training_oracle_runtime_unchanged"] is True and provenance["export_loader_profile"] == plan["loader_profile"],
            "actual upstream/PEFT runtime identity differs")
    for key in ("size_bytes", "sha256"):
        require(provenance["peft_wheel"][key] == plan["wheel"][key], "actual PEFT wheel differs")
    require(provenance["export_loader_contract_sha256"] == plan["helper_closure"]["scripts/gliner25/training_export_peft018.json"]["sha256"] and
            provenance["export_loader_helper_sha256"] == plan["helper_closure"]["scripts/gliner25/training_export_runtime.py"]["sha256"], "actual loader helper differs")
    requests = read_json(plan["requests"]["path"])["requests"]
    require([(value["id"], value["kind"]) for value in runtime["outputs"]] == [(value["id"], value["kind"]) for value in requests] and
            len(runtime["outputs"]) == 10, "runtime request coverage/order differs")
    expected = {"report.json"}
    captures = {}
    for result in runtime["outputs"]:
        if result["kind"] == "extract":
            capture = result["tensor_capture"]
            name = result["id"] + ".safetensors"
            require(capture["file"] == name and 0 < capture["size_bytes"] <= 32 * MiB, "invalid request capture")
            expected.add(name)
            captures[name] = {key: capture[key] for key in ("size_bytes", "sha256")}
    return expected, captures


def validate_success(directory, static, entry, plan):
    report_path = directory / "report/report.json"
    report = read_json(report_path)
    expected, captures = validate_report_content(report, static, entry, plan)
    sizes = tree_sizes(directory / "report", plan["max_final_artifact_bytes"])
    require(set(sizes) == expected, "published report has extra, missing, or private scratch files")
    for name, expected_pin in captures.items():
        exact(directory / "report" / name, expected_pin, maximum=32 * MiB)
    stdout = file_pin(directory / "stdout.jsonl", 4 * MiB, contents=True)[1].splitlines()
    require(len(stdout) == 1 and decode(stdout[0]) == {
        "scope": static["scope"], "status": "verified", "mode": entry["mode"], "runtime": True,
        "qualification": False, "report_sha256": file_pin(report_path, 4 * MiB)["sha256"],
    }, "checker completion event differs")
    return {"report": file_pin(report_path, 4 * MiB), "output_files": {name: file_pin(directory / "report" / name, 32 * MiB) for name in sorted(sizes)},
            "source_tensors": len(report["source_tensors"]), "adapter_tensors": len(report["export_tensors"]),
            "requests": len(report["runtime"]["outputs"]), "all_loaded_tensor_bytes_equal": True,
            "runtime_private_copy_bytes": report["runtime"]["private_copy_bytes"]}


def execute(name):
    plan, entry, static, originals = preflight(name)
    directory = Path(entry["execution_dir"])
    require(directory.parent == ROOT / "execution" and directory.name == name and not os.path.lexists(directory), "refusing an existing or foreign output")
    for prior in directory.parent.iterdir():
        require(prior.is_dir() and not prior.is_symlink() and prior.name in plan["profiles"], "unexpected prior execution owner")
        receipt = read_json(prior / "process.json")
        require(receipt["process"]["cleanup"]["complete"] and receipt["private_copies_cleaned"],
                "a prior execution has unresolved process or model-copy ownership")
    directory.mkdir(mode=0o700)
    start = {"scope": SCOPE, "version": 1, "qualification": False, "profile": name, "training_backend": entry["training_backend"],
             "runtime_backend": "pinned upstream CPU", "plan": PLAN_PIN, "wrapper": file_pin(Path(__file__), MiB),
             "command": entry["command"], "environment": THREAD_ENV, "limits": plan["limits"],
             "max_runtime_copy_bytes": plan["max_runtime_copy_bytes"], "max_artifact_bytes": plan["max_artifact_bytes"],
             "original_pins": originals, "numerical_runtime_executed": False}
    write_json(directory / "start.json", start)
    for key, value in THREAD_ENV.items():
        os.environ[key] = value
    os.environ.pop("USE_FLASHDEBERTA", None)
    raw = exact(plan["supervisor"]["snapshot"], plan["supervisor"], contents=True, maximum=MiB)
    supervisor = ModuleType("pinned_classifier_export_supervision")
    supervisor.__file__ = plan["supervisor"]["snapshot"]
    exec(compile(raw, supervisor.__file__, "exec"), supervisor.__dict__)
    result = supervisor.run(entry["command"], directory / "stdout.jsonl", directory / "stderr.log", **plan["limits"],
        tree_factory=artifact_tracker(supervisor, directory, plan["max_artifact_bytes"]))
    final = {**start, "status": "failed", "process": result, "private_copies_cleaned": False, "original_inputs_unchanged": False}
    try:
        final["removed_unpublished_copies"] = cleanup_unpublished_copies(directory, result, plan["max_artifact_bytes"])
        final["private_copies_cleaned"] = True
        require(input_pins(plan, entry) == originals, "original inputs changed during child execution")
        oracle, _ = modules(plan)
        require(oracle.verify_upstream_checkout(Path(plan["upstream"]["checkout"])) == plan["upstream"], "source checkout changed")
        final["original_inputs_unchanged"] = True
        require(result["failure"] is None and result["returncode"] == 0 and result["cleanup"]["complete"], "child execution or cleanup failed")
        final.update(validate_success(directory, static, entry, plan))
        final.update(status="verified", numerical_runtime_executed=True)
    except BaseException as error:
        final["validation_failure"] = f"{type(error).__name__}: {error}"
    for name in ("stdout.jsonl", "stderr.log"):
        if (directory / name).exists():
            final[name] = file_pin(directory / name, plan["limits"]["output_limit_bytes"])
    write_json(directory / "process.json", final)
    return final


def stop(_number, _frame):
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, signal.SIG_IGN)
    raise KeyboardInterrupt("export supervisor interrupted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, choices=("cpu-lora", "cpu-dora", "metal-lora", "metal-dora"))
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    if args.preflight_only:
        plan, entry, _, identities = preflight(args.profile)
        print(json.dumps({"scope": SCOPE, "profile": args.profile, "status": "prepared", "qualification": False,
                          "numerical_runtime_executed": False, "files_verified": len(identities),
                          "plan": PLAN_PIN, "runtime_copy_bytes": entry["runtime_copy_bytes"], "limits": plan["limits"]}))
        return
    with (ROOT / "execution.lock").open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        for number in (signal.SIGINT, signal.SIGTERM):
            signal.signal(number, stop)
        result = execute(args.profile)
    print(json.dumps({"profile": args.profile, "status": result["status"], "qualification": False,
                      "evidence": str(ROOT / "execution" / args.profile / "process.json")}))
    if result["status"] != "verified":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
