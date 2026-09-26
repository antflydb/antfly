"""Loaded-model upstream GLiNER2.5-Decide request latency on CPU or CUDA."""

import argparse
import hashlib
import json
import statistics
import sys
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--source-root", type=Path, required=True)
parser.add_argument("--model-dir", type=Path, required=True)
parser.add_argument("--device", choices=["cpu", "cuda"], required=True)
parser.add_argument("--warmup", type=int, default=3)
parser.add_argument("--reps", type=int, default=20)
args = parser.parse_args()
weight = args.model_dir / "model.safetensors"
if weight.stat().st_size != 1_945_828_140:
    raise RuntimeError("Decide checkpoint size mismatch")
digest = hashlib.sha256()
with weight.open("rb") as source:
    for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
        digest.update(chunk)
if (
    digest.hexdigest()
    != "40a5a23ff860dc3dff426cecd1048cacdd29c648c96db209dad818e9686dc997"
):
    raise RuntimeError("Decide checkpoint hash mismatch")
sys.path.insert(0, str(args.source_root))
import torch
from gliner2 import AutoExtractor
from gliner2.classification.compiler import compile_schema
from gliner2.classification.schema import ClassificationSchema
from gliner2.classification.scoring import ClassificationScorer

if args.device == "cuda" and not torch.cuda.is_available():
    raise RuntimeError("CUDA unavailable")
torch.set_num_threads(2)
model = AutoExtractor.from_pretrained(str(args.model_dir), map_location=args.device)
model.eval()
scorer = ClassificationScorer(model, device=args.device, dtype=torch.float32).eval()
schema = ClassificationSchema()
schema.single("intent", ("refund", "technical_support", "sales"))
schema.single("urgency", ("low", "medium", "high"))
compiled = compile_schema(schema)
text = "Please refund the duplicate charge. I do not need technical help."


def run():
    with torch.inference_mode():
        scores = scorer.score(text, compiled, max_len=512)
        if args.device == "cuda":
            torch.cuda.synchronize()
        if (
            max(
                ("refund", "technical_support", "sales"),
                key=lambda label: scores.logit("intent", label),
            )
            != "refund"
        ):
            raise RuntimeError("winner mismatch")


for _ in range(args.warmup):
    run()
samples = []
for _ in range(args.reps):
    start = time.perf_counter_ns()
    run()
    samples.append((time.perf_counter_ns() - start) / 1e6)
ordered = sorted(samples)
result = {
    "implementation": "upstream_gliner2_python",
    "device": args.device,
    "torch": torch.__version__,
    "transformers": __import__("transformers").__version__,
    "gpu": torch.cuda.get_device_name(0) if args.device == "cuda" else None,
    "threads": torch.get_num_threads(),
    "warmup": args.warmup,
    "reps": args.reps,
    "model_safetensors_sha256": digest.hexdigest(),
    "median_ms": statistics.median(samples),
    "mean_ms": statistics.mean(samples),
    "p95_ms": ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))],
    "min_ms": min(samples),
    "max_ms": max(samples),
    "samples_ms": samples,
}
print(json.dumps(result, sort_keys=True))
