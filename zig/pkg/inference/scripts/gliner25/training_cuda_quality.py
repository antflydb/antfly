"""Held-out evaluation of both trained weight sets in one pinned evaluator.

This tests learned-weight quality, not native inference execution. Reuse the
existing annotated validation fixture and public extraction normalization.
"""
from collections import Counter
from pathlib import Path
import json

import numpy as np
import benchmark_cpu as common


def read_rows(path):
    if not 0 < path.stat().st_size <= 2 * 1024**2:
        raise ValueError("qualification dataset exceeds two MiB")
    rows = [common.strict_json(line) for line in path.read_bytes().splitlines()]
    if not 1 <= len(rows) <= 256:
        raise ValueError("qualification dataset requires 1–256 examples")
    from training_cuda_worker import adapt_row
    ids = set()
    for row in rows:
        if row.get("version") != 1 or not isinstance(row.get("id"), str) or row["id"] in ids:
            raise ValueError("invalid or duplicate qualification example identity")
        ids.add(row["id"])
        adapt_row(row)
    return rows


def require_disjoint(training, validation):
    import unicodedata
    def texts(rows):
        return {" ".join(unicodedata.normalize("NFKC", row["text"]).casefold().split()) for row in rows}
    if ({row["id"] for row in training} & {row["id"] for row in validation}
            or texts(training) & texts(validation)):
        raise ValueError("training and validation examples overlap")


def canonical_parameters(model, inventory=None):
    """Keep pre-compilation names without accepting replaced or reordered tensors.

    Fastino compiles individual submodules, adding `_orig_mod` name components.
    Match the actual parameter objects, not a textual prefix rewrite that could
    conflate distinct names. Keep the registration order used by the optimizer.
    """
    current = tuple(model.named_parameters())
    canonical = current if inventory is None else tuple(inventory)
    if (len({name for name, _ in canonical}) != len(canonical)
            or len({id(value) for _, value in canonical}) != len(canonical)
            or [id(value) for _, value in current] != [id(value) for _, value in canonical]):
        raise ValueError("compiled parameter identity or registration order differs")
    return canonical


def load_weights(model, receipt, torch, inventory=None):
    """Validate the complete snapshot inventory before changing any parameter."""
    path = Path(receipt["snapshot"])
    if path.stat().st_size != receipt["size_bytes"] or not 0 < receipt["size_bytes"] <= 4 * 1024**3:
        raise ValueError("invalid evaluation snapshot size")
    selected = {name: value for name, value in canonical_parameters(model, inventory) if value.requires_grad}
    slots = receipt["slots"]
    if len(slots) != len(selected) or {s["canonical_name"] for s in slots} != selected.keys():
        raise ValueError("evaluation parameter inventory differs")
    with path.open("rb") as source:
        data = np.memmap(source, dtype="<f4", mode="r")
        offset = 0
        for slot in slots:
            parameter = selected[slot["canonical_name"]]
            n = parameter.numel()
            if (slot["shape"] != list(parameter.shape) or slot["elements"] != n
                    or slot["offset"] != offset or offset + n * 16 > receipt["size_bytes"]):
                raise ValueError("invalid evaluation snapshot layout")
            for start in range(offset // 4, offset // 4 + n, 262144):
                if not np.isfinite(data[start:min(start + 262144, offset // 4 + n)]).all():
                    raise ValueError("non-finite evaluation weight")
            offset += n * 16
        if offset != receipt["size_bytes"]:
            raise ValueError("evaluation snapshot has trailing data")
        with torch.no_grad():
            for slot in slots:
                values = np.array(data[slot["offset"] // 4:slot["offset"] // 4 + slot["elements"]], copy=True)
                selected[slot["canonical_name"]].copy_(torch.from_numpy(values).reshape(slot["shape"]))
        del data


def gold_facts(row):
    text = row["text"].encode()
    def surface(span):
        return text[span["start"]:span["end"]].decode()
    entities = {item["id"]: surface(item["span"]) for item in row.get("entities", [])}
    result = {name: Counter() for name in ("entities", "classifications", "records", "relations")}
    for item in row.get("entities", []):
        span = item["span"]
        start, end = (len(text[:span[key]].decode()) for key in ("start", "end"))
        result["entities"][(item["type"], entities[item["id"]], start, end)] += 1
    for item in row.get("classifications", []):
        for label in item["labels"]:
            result["classifications"][(item["task"], label)] += 1
    for item in row.get("records", []):
        fields = tuple(sorted((field["name"], tuple(sorted(surface(span) for value in field["values"] for span in value["occurrences"]))) for field in item["fields"]))
        result["records"][(item["type"], fields)] += 1
    for item in row.get("relations", []):
        result["relations"][(item["type"], entities[item["head"]["entity"]], entities[item["tail"]["entity"]])] += 1
    return result


def predicted_facts(output):
    result = {name: Counter() for name in ("entities", "classifications", "records", "relations")}
    for group in output["entities"]:
        for value in group["values"]:
            result["entities"][(group["name"], value["text"], value["source"]["start"], value["source"]["end"])] += 1
    for group in output["classifications"]:
        for label in group["labels"]:
            result["classifications"][(group["name"], label["label"])] += 1
    for group in output["structures"]:
        for record in group["instances"]:
            fields = tuple(sorted((field["name"], tuple(sorted(value["text"] for value in field["values"]))) for field in record["fields"]))
            result["records"][(group["name"], fields)] += 1
    for edge in output["relations"]:
        result["relations"][(edge["name"], edge["head"]["text"], edge["tail"]["text"])] += 1
    return result


def evaluation_request(row):
    schema = row["schema"]
    return {"kind": "extract", "text": row["text"], "schema": {
        "entities": schema.get("entities", []),
        "classifications": [{"task": item["name"], "labels": item["labels"], "multi_label": item["mode"] == "multi"} for item in schema.get("classifications", [])],
        "relations": [item["type"] for item in schema.get("relations", [])],
        "structures": {name: {**spec, "fields": [{"name": field, **settings} for field, settings in spec["fields"].items()]} for name, spec in schema.get("structures", {}).items()},
    }}


def evaluate(model, rows, torch):
    counts = {name: {"correct": 0, "predicted": 0, "gold": 0} for name in ("entities", "classifications", "records", "relations")}
    outputs = []
    training = model.training
    try:
        model.eval()
        with torch.inference_mode():
            for row in rows:
                request = evaluation_request(row)
                output = common.canonical_python(request, common.execute_python(model, request, json.dumps(request["schema"])))
                outputs.append({"id": row["id"], "output": output})
                gold, predicted = gold_facts(row), predicted_facts(output)
                for name, values in counts.items():
                    values["correct"] += sum((gold[name] & predicted[name]).values())
                    values["predicted"] += sum(predicted[name].values())
                    values["gold"] += sum(gold[name].values())
    finally:
        model.train(training)
    for values in counts.values():
        denominator = values["predicted"] + values["gold"]
        values["f1"] = 2 * values["correct"] / denominator if denominator else 1.0
    return {"evaluator": "pinned_python_cuda", "metrics": counts, "outputs": outputs}
