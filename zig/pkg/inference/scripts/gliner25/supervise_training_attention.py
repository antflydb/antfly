#!/usr/bin/env python3
"""One bounded attention-oracle child using the unchanged published supervisor.

Preflight never imports Torch. Execution snapshots the exact small Python source
closure, preserves the selected virtualenv invocation, and retains all failures.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import sys
from types import ModuleType

HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parent.parent / "testdata/gliner25"
SUPERVISOR = FIXTURES / "published_inactive_classifier_cpu_v1/helpers/v2/supervision.py"
PYTHON = Path("/private/tmp/antfly-gliner25-oracle-venv/bin/python")
UPSTREAM = Path("/private/tmp/antfly-gliner25-upstream")
DEFAULT_OUTPUT = Path("/private/tmp/gliner25-training-attention-v1")
DEFAULT_EVIDENCE = Path("/private/tmp/gliner25-training-attention-probe-v1")
SCOPE = "gliner25_training_attention_supervision/v1"
MAX_ARTIFACT_BYTES = 64 * 1024**2
PINS = {
    "capture_training_attention.py": {"size_bytes": 20879, "sha256": "c67ee517899871b4ad36a11408ff321028ad10ab88f0941aa0c09ef48f6a1a6f"},
    "training_attention_contract_v1.json": {"size_bytes": 12057, "sha256": "7e512fe4418d006619088093ed7410ddd9a209dcb421954c9fbb14287bbecfcc"},
    "test_training_attention.py": {"size_bytes": 5899, "sha256": "8c0e7baa07646b2e2b8e3cb2a5dce0ab5dbc467af72f94df359ee2fe555bbcae"},
    "oracle.py": {"size_bytes": 28555, "sha256": "61bbf4544a21111dbfbd643ed2392b1d9723498493b3fa6832154d6e1dc42752"},
    "oracle_manifest.json": {"size_bytes": 4097, "sha256": "918005508a57d2c558c18a78167637f39c54e7c2c6d67814be15fc6714c4152f"},
}
SUPERVISOR_PIN = {"size_bytes": 12474, "sha256": "a937237975be2ed879f62afd285494ccb51a7c1d7cc1dff7160621fb26b87332"}
PROOF_PINS = {
    "/private/tmp/gliner25-training-attention-preflight-v3.log": {"size_bytes": 118, "sha256": "4fb33fa67f62f8f2a25df60b90fc4d71ce840efe810d3e453b237b817692ddc1"},
    "/private/tmp/gliner25-training-attention-contract-tests-v2.log": {"size_bytes": 1339, "sha256": "c80a5175af28c38a51dd57e8e86ff5c64939cbd8a6e3ad3635daaeb6b2e4548f"},
}
LIMITS = {"timeout_seconds": 180, "rss_limit_bytes": 2 * 1024**3,
          "output_limit_bytes": 4 * 1024**2, "grace_seconds": 2.0,
          "kill_seconds": 5.0, "worker_grace_seconds": 2.0, "tick": 0.05}
ARTIFACT_NAMES = {"capture.json", "tensors.safetensors", "weights.safetensors"}


def checked(value, message):
    if not value:
        raise ValueError(message)


def read(path, maximum=4 * 1024**2):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
        before = os.fstat(source.fileno())
        checked(stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= maximum, "not a bounded regular file: " + str(path))
        data = source.read(maximum + 1)
        after = os.fstat(source.fileno())
    checked(len(data) == before.st_size and all(getattr(before, key) == getattr(after, key)
        for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")), "input changed during read")
    return data


def pin(data):
    return {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def require_pin(path, expected):
    raw = read(path)
    checked(pin(raw) == expected, "frozen attention input differs: " + str(path))
    return raw


def module_from_bytes(name, path, raw):
    module = ModuleType(name)
    module.__file__ = str(path)
    exec(compile(raw, str(path), "exec"), module.__dict__)
    return module


def artifact_sizes(root, limit=MAX_ARTIFACT_BYTES):
    try:
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return {}
    sizes = {}
    try:
        with os.scandir(descriptor) as entries:
            for entry in entries:
                checked(entry.name in ARTIFACT_NAMES and len(sizes) < 3, "unexpected attention artifact entry")
                with os.fdopen(os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor), "rb") as source:
                    info = os.fstat(source.fileno())
                checked(stat.S_ISREG(info.st_mode) and 0 <= info.st_size <= limit, "unbounded or nonregular attention artifact")
                sizes[entry.name] = info.st_size
                checked(sum(sizes.values()) <= limit, "attention artifact byte ceiling exceeded")
    finally:
        os.close(descriptor)
    return sizes


def tree_factory(supervisor, output):
    class ArtifactTree(supervisor.ProcessTree):
        def __init__(self, *args):
            super().__init__(*args)
            self.artifact_peak_bytes = 0
            self.check_artifacts = True

        def sample(self):
            result = super().sample()
            if self.check_artifacts:
                sizes = artifact_sizes(output)
                self.artifact_peak_bytes = max(self.artifact_peak_bytes, sum(sizes.values()))
            return result

        def cleanup_sample(self):
            # A failed artifact guard must not obstruct process-only cleanup.
            self.check_artifacts = False
            return super().cleanup_sample()

        def receipt(self):
            return {**super().receipt(), "sampled_artifact_peak_bytes": self.artifact_peak_bytes,
                    "artifact_byte_ceiling": MAX_ARTIFACT_BYTES}
    return ArtifactTree


def publish(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    checked(len(raw) <= 1024**2, "supervision receipt ceiling exceeded")
    with path.open("xb") as destination:
        destination.write(raw)
        destination.flush()
        os.fsync(destination.fileno())


def fresh_path(path):
    checked(path.is_absolute() and path.parent.resolve() == path.parent and path.parent.is_dir(), "output parent must be an existing canonical absolute directory")
    checked(not os.path.lexists(path), "refusing to overwrite attention output: " + str(path))


def preflight():
    checked(Path(os.path.abspath(sys.executable)) == PYTHON, "invoke the supervisor through the pinned virtualenv Python")
    inputs = {name: require_pin(HERE / name, expected) for name, expected in PINS.items()}
    supervisor_raw = require_pin(SUPERVISOR, SUPERVISOR_PIN)
    proofs = {name: pin(require_pin(Path(name), expected)) for name, expected in PROOF_PINS.items()}
    generator = module_from_bytes("attention_source_contract", HERE / "capture_training_attention.py", inputs["capture_training_attention.py"])
    contract, source = generator.preflight(UPSTREAM)
    manifest_raw = inputs["oracle_manifest.json"]
    checked(json.loads(manifest_raw)["runtime"] == contract["runtime"], "oracle manifest dependency closure differs")
    inputs["oracle_manifest.json"] = manifest_raw
    checked(not any(name in sys.modules for name in ("torch", "gliner2", "peft")), "supervisor preflight imported a numerical runtime")
    executable = read(PYTHON.resolve(), 32 * 1024**2)
    checked(pin(executable)["sha256"] == "80ee2dd97bc26259d4e30853336f72ad38aa4aa0531bb196cc444d899422689d", "Python executable changed")
    cfg = read(PYTHON.parent.parent / "pyvenv.cfg")
    checked(pin(cfg) == {"size_bytes": 339, "sha256": "b5575d239986adf44b66e0f3c032be806b508ec16f37cdb548653c301e9c6793"}, "virtualenv configuration changed")
    return inputs, supervisor_raw, {"scope": SCOPE, "qualification": False, "inputs": {name: pin(raw) for name, raw in inputs.items()},
        "supervisor": SUPERVISOR_PIN, "prior_checks": proofs, "transformers_source": generator.digest(source),
        "runtime": contract["runtime"], "python_invocation": str(PYTHON), "python_executable": pin(executable),
        "pyvenv_cfg": pin(cfg), "limits": {**LIMITS, "artifact_bytes": MAX_ARTIFACT_BYTES},
        "source_commit": contract["source_commit"], "wrapper": pin(read(Path(__file__)))}


def execute(output, evidence):
    inputs, supervisor_raw, admitted = preflight()
    fresh_path(output)
    fresh_path(evidence)
    checked(output != evidence, "capture and supervision owners must differ")
    evidence.mkdir(mode=0o700)
    snapshots = evidence / "helpers"
    snapshots.mkdir(mode=0o700)
    for name in ("capture_training_attention.py", "training_attention_contract_v1.json", "oracle.py", "oracle_manifest.json"):
        with (snapshots / name).open("xb") as destination:
            destination.write(inputs[name])
    command = [str(PYTHON), "-B", str(snapshots / "capture_training_attention.py"),
               "--upstream", str(UPSTREAM), "--output-dir", str(output)]
    admitted.update(command=command, output_dir=str(output), evidence_dir=str(evidence), status="admitted")
    publish(evidence / "start.json", admitted)
    supervisor = module_from_bytes("pinned_attention_supervision", SUPERVISOR, supervisor_raw)
    result = supervisor.run(command, evidence / "stdout.jsonl", evidence / "stderr.log",
        **LIMITS, tree_factory=tree_factory(supervisor, output))
    final = {**admitted, "status": "failed", "process": result, "artifacts": {}}
    try:
        checked(result["failure"] is None and result["returncode"] == 0 and result["cleanup"]["complete"], "attention child or owned-tree cleanup failed")
        sizes = artifact_sizes(output)
        checked(set(sizes) == ARTIFACT_NAMES, "attention capture is incomplete")
        for name in sorted(sizes):
            final["artifacts"][name] = pin(read(output / name, MAX_ARTIFACT_BYTES))
        manifest = json.loads(read(output / "capture.json", 2 * 1024**2))
        checked(manifest["scope"] == "gliner25_projected_training_attention/v1" and manifest["qualification"] is False and
                len(manifest["cases"]) == 9 and manifest["generator"] == PINS["capture_training_attention.py"] and
                manifest["contract"] == PINS["training_attention_contract_v1.json"], "attention artifact source identity differs")
        checked(manifest["files"] == {name: final["artifacts"][name] for name in ("weights.safetensors", "tensors.safetensors")}, "attention file receipt differs")
        for name, raw in inputs.items():
            checked(read(HERE / name) == raw, "original attention helper changed during child execution")
        for name in ("capture_training_attention.py", "training_attention_contract_v1.json", "oracle.py", "oracle_manifest.json"):
            checked(read(snapshots / name) == inputs[name], "private attention helper changed")
        require_pin(SUPERVISOR, SUPERVISOR_PIN)
        final["status"] = "captured"
    except BaseException as error:
        final["validation_failure"] = f"{type(error).__name__}: {error}"
    for name in ("stdout.jsonl", "stderr.log"):
        path = evidence / name
        if path.exists():
            final[name] = pin(read(path))
    publish(evidence / "process.json", final)
    return final


def stop_signal(_number, _frame):
    # Let the published supervisor's BaseException cleanup retain ownership.
    # Subsequent cooperative interrupts must not interrupt its bounded reaping.
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, signal.SIG_IGN)
    raise KeyboardInterrupt("attention supervisor interrupted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE)
    args = parser.parse_args()
    if args.preflight_only:
        _, _, admitted = preflight()
        fresh_path(args.output_dir)
        fresh_path(args.evidence_dir)
        print(json.dumps({**admitted, "status": "preflight_only", "model_execution": False}, sort_keys=True))
    else:
        previous = {number: signal.signal(number, stop_signal) for number in (signal.SIGINT, signal.SIGTERM)}
        try:
            result = execute(args.output_dir, args.evidence_dir)
        finally:
            for number, handler in previous.items():
                signal.signal(number, handler)
        print(json.dumps({"status": result["status"], "qualification": False, "process_receipt": str(args.evidence_dir / "process.json")}, sort_keys=True))
        raise SystemExit(0 if result["status"] == "captured" else 1)


if __name__ == "__main__":
    main()
