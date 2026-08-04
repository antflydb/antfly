#!/usr/bin/env python3
"""Score a standard PEFT adapter with the frozen upstream GLiNER2 decoder."""

from __future__ import annotations

import argparse
import json
import math
import platform
import sys
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator

from gliner2_release_contract import (
    CANONICAL_GLINER2_VERSION,
    CANONICAL_NORMALIZATION,
    CANONICAL_UNICODE_VERSION,
    UPSTREAM_COMMIT,
    canonical_text,
    clean_text as contract_clean_text,
    path_fingerprint,
    verify_import_source,
    verify_canonical_oracle_packages,
    verify_canonical_python_runtime,
    verify_upstream_checkout,
)
from validate_gliner2_release_data import (
    adapter_bundle_fingerprint,
    base_model_fingerprint,
    peft_adapter_fingerprint,
)


EVALUATION_CONTRACT = "gliner2_full_task_evaluation/v1"

REQUIRED_MINIMA = frozenset(
    {
        "entities.micro_f1",
        "entities.exact_match",
        "classifications.micro_f1",
        "classifications.exact_match",
        "json_structures.micro_f1",
        "json_structures.exact_match",
        "relations.micro_f1",
        "relations.exact_match",
        "count.accuracy",
    }
)


def clean_text(value: Any) -> str:
    return contract_clean_text(value)


def normalized_text(value: Any) -> str:
    return canonical_text(value)


def values(value: Any) -> tuple[str, ...]:
    if isinstance(value, dict):
        if "value" not in value or "choices" not in value:
            raise ValueError("field objects must contain value and choices")
        value = value["value"]
    if value is None or value == "":
        return ()
    if isinstance(value, list):
        return tuple(item for item in (normalized_text(item) for item in value) if item)
    result = normalized_text(value)
    return (result,) if result else ()


def descriptions(
    value: Any,
    *,
    context: str,
    allowed: Iterable[str],
) -> OrderedDict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object")
    allowed_keys = set(allowed)
    result: OrderedDict[str, str] = OrderedDict()
    for key, description in value.items():
        if key not in allowed_keys:
            raise ValueError(f"{context} contains unknown key {key!r}")
        if not isinstance(description, str):
            raise ValueError(f"{context}.{key} must be a string")
        result[key] = description
    return result


def classification_prediction_key(config: dict[str, Any]) -> str:
    task = config["task"]
    prompt = config.get("prompt")
    key = f"{task}: {prompt}" if prompt else task
    # The pinned decoder strips everything beginning with the first
    # description marker. Without descriptions, few-shot examples remain in
    # its raw result key and must be matched exactly.
    if config.get("label_descriptions"):
        return key
    for example_input, example_output in config.get("examples", ()):
        key += f" [EXAMPLE] {example_input} [OUTPUT] {example_output}"
    return key


def rows(path: Path, max_records: int | None = None) -> Iterator[tuple[str, dict[str, Any]]]:
    sources = [path] if path.is_file() else sorted(path.glob("*.jsonl")) if path.is_dir() else []
    if not sources:
        raise ValueError(f"{path}: expected a JSONL file or directory containing JSONL files")
    emitted = 0
    for source in sources:
        with source.open(encoding="utf-8") as lines:
            for line_no, raw in enumerate(lines, 1):
                if not raw.strip():
                    continue
                try:
                    record = json.loads(raw)
                    if not isinstance(record, dict):
                        raise ValueError("record must be an object")
                except (json.JSONDecodeError, ValueError) as exc:
                    raise ValueError(f"{source}:{line_no}: {exc}") from exc
                yield f"{source}:{line_no}", record
                emitted += 1
                if max_records is not None and emitted >= max_records:
                    return


@dataclass
class Gold:
    entities: Counter[tuple[str, str]] = field(default_factory=Counter)
    entities_present: bool = False
    classifications: OrderedDict[str, Counter[str]] = field(default_factory=OrderedDict)
    structures: OrderedDict[str, list[tuple[tuple[str, tuple[str, ...]], ...]]] = field(default_factory=OrderedDict)
    relations: OrderedDict[str, list[tuple[str, str]]] = field(default_factory=OrderedDict)


def schema_and_gold(record: dict[str, Any]) -> tuple[str, dict[str, Any], Gold]:
    text = record.get("input", record.get("text"))
    if not isinstance(text, str) or not text.strip():
        raise ValueError("record requires non-empty input/text")
    output = record.get("output")
    if not isinstance(output, dict):
        if isinstance(record.get("entities"), list):
            output = {"entities": {}}
            for item in record["entities"]:
                if (
                    not isinstance(item, dict)
                    or not clean_text(item.get("label"))
                    or not clean_text(item.get("text"))
                ):
                    raise ValueError("legacy entity entries require label and text")
                output["entities"].setdefault(item["label"], []).append(item.get("text"))
        else:
            raise ValueError("record requires output object")

    schema: dict[str, Any] = {}
    gold = Gold()

    entities = output.get("entities")
    if entities is not None:
        if not isinstance(entities, dict):
            raise ValueError("output.entities must be an object")
        entity_schema: OrderedDict[str, str] = OrderedDict()
        seen_labels: set[str] = set()
        for label, value in entities.items():
            label_name = clean_text(label)
            label_key = label_name.casefold()
            if not label_name or label_key in seen_labels:
                raise ValueError("entity labels must be non-empty and unique")
            seen_labels.add(label_key)
            entity_schema[label_name] = ""
            gold.entities.update((label_key, item) for item in values(value))
        if entity_schema:
            schema["entities"] = entity_schema
            gold.entities_present = True

    entity_descriptions = output.get("entity_descriptions")
    if entity_descriptions is not None:
        parsed = descriptions(
            entity_descriptions,
            context="output.entity_descriptions",
            allowed=schema.get("entities", ()),
        )
        if parsed:
            schema["entity_descriptions"] = parsed

    classifications = output.get("classifications")
    if classifications is not None:
        if not isinstance(classifications, list):
            raise ValueError("output.classifications must be an array")
        classification_schema: list[dict[str, Any]] = []
        seen_classification_tasks: set[str] = set()
        for item in classifications:
            if not isinstance(item, dict):
                raise ValueError("classification entries must be objects")
            task = clean_text(item.get("task"))
            raw_labels = item.get("labels")
            raw_true_labels = item.get("true_label")
            if not isinstance(raw_labels, list) or not isinstance(raw_true_labels, list):
                raise ValueError("classification labels and true_label must be arrays")
            labels = [clean_text(label) for label in raw_labels]
            true_labels = values(raw_true_labels)
            normalized_labels = [label.casefold() for label in labels]
            if not task or len(labels) < 2 or len(labels) != len(set(normalized_labels)) or not all(labels):
                raise ValueError("classification task and labels must be non-empty and unique")
            task_key = task.casefold()
            if task_key in seen_classification_tasks or not set(true_labels) <= set(normalized_labels):
                raise ValueError("classification true labels must be subsets of unique tasks")
            seen_classification_tasks.add(task_key)
            multi_label = item.get("multi_label", len(true_labels) > 1)
            if not isinstance(multi_label, bool):
                raise ValueError("classification multi_label must be boolean")
            if not multi_label and len(true_labels) > 1:
                raise ValueError("single-label classification allows at most one true label")
            config: dict[str, Any] = {
                "task": task,
                "labels": labels,
                "multi_label": multi_label,
            }
            if "prompt" in item:
                if not isinstance(item["prompt"], str):
                    raise ValueError("classification prompt must be a string")
                config["prompt"] = item["prompt"]
            if "label_descriptions" in item:
                config["label_descriptions"] = descriptions(
                    item["label_descriptions"],
                    context=f"classification {task!r} label_descriptions",
                    allowed=labels,
                )
            if "examples" in item:
                raw_examples = item["examples"]
                if not isinstance(raw_examples, list):
                    raise ValueError("classification examples must be an array")
                examples: list[list[str]] = []
                for example in raw_examples:
                    if (
                        not isinstance(example, (list, tuple))
                        or len(example) != 2
                        or not all(isinstance(part, str) for part in example)
                    ):
                        raise ValueError("classification examples must be string input/output pairs")
                    if example[1] not in labels:
                        raise ValueError("classification example outputs must belong to labels")
                    examples.append([example[0], example[1]])
                config["examples"] = examples
            classification_schema.append(config)
            gold.classifications[classification_prediction_key(config)] = Counter(true_labels)
        if classification_schema:
            schema["classifications"] = classification_schema

    structures = output.get("json_structures")
    if structures is not None:
        if not isinstance(structures, list):
            raise ValueError("output.json_structures must be an array")
        grouped: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for item in structures:
            if not isinstance(item, dict) or len(item) != 1:
                raise ValueError("JSON structure entries require exactly one parent")
            parent, fields = next(iter(item.items()))
            parent = clean_text(parent)
            if not parent or not isinstance(fields, dict) or not fields:
                raise ValueError("JSON structure parents and fields must be non-empty")
            grouped.setdefault(parent, []).append(fields)
        structure_schema: list[dict[str, Any]] = []
        structure_descriptions: OrderedDict[str, OrderedDict[str, str]] = OrderedDict()
        raw_structure_descriptions = output.get("json_descriptions")
        if raw_structure_descriptions is not None and not isinstance(raw_structure_descriptions, dict):
            raise ValueError("output.json_descriptions must be an object")
        raw_structure_descriptions = raw_structure_descriptions or {}
        unknown_parents = set(raw_structure_descriptions) - set(grouped)
        if unknown_parents:
            raise ValueError(
                f"output.json_descriptions contains unknown parent {sorted(unknown_parents)[0]!r}"
            )
        for parent, occurrences in grouped.items():
            templates: OrderedDict[str, Any] = OrderedDict()
            for occurrence in occurrences:
                for field_name, value in occurrence.items():
                    field_name = clean_text(field_name)
                    if not field_name:
                        raise ValueError("JSON field names must be non-empty")
                    template: Any = ""
                    if isinstance(value, dict):
                        choices = value.get("choices")
                        if not isinstance(choices, list) or len(choices) < 2:
                            raise ValueError("choice fields require at least two choices")
                        clean_choices = [clean_text(choice) for choice in choices]
                        normalized_choices = [choice.casefold() for choice in clean_choices]
                        if not all(clean_choices) or len(clean_choices) != len(set(normalized_choices)):
                            raise ValueError("choice values must be non-empty and unique")
                        if not set(values(value)) <= set(normalized_choices):
                            raise ValueError("selected choice values must belong to choices")
                        template = {"value": "", "choices": clean_choices}
                    previous = templates.get(field_name)
                    if previous is not None and previous != template:
                        raise ValueError(f"inconsistent schema for {parent}.{field_name}")
                    templates.setdefault(field_name, template)
            canonical: list[tuple[tuple[str, tuple[str, ...]], ...]] = []
            seen: set[tuple[tuple[str, tuple[str, ...]], ...]] = set()
            for occurrence in occurrences:
                by_name = {clean_text(name): value for name, value in occurrence.items()}
                instance = tuple((name, values(by_name.get(name))) for name in templates)
                if instance not in seen:
                    seen.add(instance)
                    canonical.append(instance)
            if canonical and all(not field_values for instance in canonical for _, field_values in instance):
                canonical = []
            structure_schema.append({parent: templates})
            if parent in raw_structure_descriptions:
                parsed = descriptions(
                    raw_structure_descriptions[parent],
                    context=f"output.json_descriptions.{parent}",
                    allowed=templates,
                )
                if parsed:
                    structure_descriptions[parent] = parsed
            gold.structures[parent] = canonical
        if structure_schema:
            schema["json_structures"] = structure_schema
        if structure_descriptions:
            schema["json_descriptions"] = structure_descriptions
    elif "json_descriptions" in output:
        parsed_json_descriptions = output["json_descriptions"]
        if not isinstance(parsed_json_descriptions, dict) or parsed_json_descriptions:
            raise ValueError("output.json_descriptions requires matching json_structures")

    relations = output.get("relations")
    if relations is not None:
        if not isinstance(relations, list):
            raise ValueError("output.relations must be an array")
        grouped_relations: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        for item in relations:
            if not isinstance(item, dict) or len(item) != 1:
                raise ValueError("relation entries require exactly one parent")
            parent, fields = next(iter(item.items()))
            parent = clean_text(parent)
            if not parent or not isinstance(fields, dict):
                raise ValueError("relation parents and fields must be non-empty")
            if set(fields) - {"head", "tail"}:
                raise ValueError(
                    f"relation {parent!r} has non-head/tail fields unsupported by the upstream public decoder"
                )
            if set(fields) != {"head", "tail"}:
                raise ValueError(f"relation {parent!r} must contain exactly head and tail")
            for field_name in ("head", "tail"):
                if len(values(fields[field_name])) != 1:
                    raise ValueError(
                        f"relation {parent!r}.{field_name} must contain exactly one non-empty value"
                    )
            grouped_relations.setdefault(parent, []).append(fields)
        relation_schema: list[dict[str, Any]] = []
        for parent, occurrences in grouped_relations.items():
            pairs: list[tuple[str, str]] = []
            seen_pairs: set[tuple[str, str]] = set()
            for occurrence in occurrences:
                heads, tails = values(occurrence.get("head")), values(occurrence.get("tail"))
                assert len(heads) == 1 and len(tails) == 1
                pair = (heads[0], tails[0])
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    pairs.append(pair)
            if pairs:
                relation_schema.append({parent: OrderedDict((field, "") for field in ("head", "tail"))})
                gold.relations[parent] = pairs
        if relation_schema:
            schema["relations"] = relation_schema

    if not schema:
        raise ValueError("record contains no supported task")
    return text, schema, gold


@dataclass
class PRF:
    true_positive: int = 0
    false_positive: int = 0
    false_negative: int = 0
    exact_correct: int = 0
    exact_cases: int = 0
    gold_atoms: int = 0

    def add(self, gold: Counter[Any], predicted: Counter[Any], *, exact: bool | None = None) -> None:
        overlap = gold & predicted
        self.true_positive += overlap.total()
        self.false_positive += (predicted - gold).total()
        self.false_negative += (gold - predicted).total()
        self.exact_correct += gold == predicted if exact is None else exact
        self.exact_cases += 1
        self.gold_atoms += gold.total()

    def result(self) -> dict[str, Any]:
        precision_denominator = self.true_positive + self.false_positive
        recall_denominator = self.true_positive + self.false_negative
        precision = self.true_positive / precision_denominator if precision_denominator else 1.0
        recall = self.true_positive / recall_denominator if recall_denominator else 1.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        return {
            "micro_f1": f1,
            "micro_precision": precision,
            "micro_recall": recall,
            "exact_match": self.exact_correct / self.exact_cases if self.exact_cases else None,
            "cases": self.exact_cases,
            "gold_atoms": self.gold_atoms,
            "true_positive": self.true_positive,
            "false_positive": self.false_positive,
            "false_negative": self.false_negative,
        }


@dataclass
class CountScore:
    correct: int = 0
    cases: int = 0
    absolute_error: int = 0

    def add(self, gold: int, predicted: int) -> None:
        self.correct += gold == predicted
        self.absolute_error += abs(gold - predicted)
        self.cases += 1

    def result(self) -> dict[str, Any]:
        return {
            "accuracy": self.correct / self.cases if self.cases else None,
            "mean_absolute_error": self.absolute_error / self.cases if self.cases else None,
            "cases": self.cases,
        }


@dataclass
class Scores:
    entities: PRF = field(default_factory=PRF)
    classifications: PRF = field(default_factory=PRF)
    structures: PRF = field(default_factory=PRF)
    relations: PRF = field(default_factory=PRF)
    count: CountScore = field(default_factory=CountScore)
    structure_count: CountScore = field(default_factory=CountScore)
    relation_count: CountScore = field(default_factory=CountScore)

    def result(self) -> dict[str, Any]:
        structures = self.structures.result()
        structures["count"] = self.structure_count.result()
        relations = self.relations.result()
        relations["count"] = self.relation_count.result()
        return {
            "entities": self.entities.result(),
            "classifications": self.classifications.result(),
            "json_structures": structures,
            "relations": relations,
            "count": self.count.result(),
        }


def counter_from_values(prefix: tuple[str, ...], value: Any) -> Counter[tuple[str, ...]]:
    return Counter((*prefix, item) for item in values(value))


def score_prediction(scores: Scores, gold: Gold, prediction: dict[str, Any]) -> None:
    if not isinstance(prediction, dict):
        raise ValueError("upstream prediction must be an object")

    predicted_entities: Counter[tuple[str, str]] = Counter()
    entity_result = prediction.get("entities", [])
    if isinstance(entity_result, list) and entity_result and isinstance(entity_result[0], dict):
        for label, result in entity_result[0].items():
            predicted_entities.update(counter_from_values((normalized_text(label),), result))
    if gold.entities_present:
        scores.entities.add(gold.entities, predicted_entities)

    for task, expected in gold.classifications.items():
        result = prediction.get(task)
        predicted = Counter()
        if isinstance(result, tuple) and result:
            predicted[normalized_text(result[0])] += 1
        elif isinstance(result, list):
            predicted.update(normalized_text(item[0] if isinstance(item, tuple) else item) for item in result)
        scores.classifications.add(expected, predicted)

    for parent, instances in gold.structures.items():
        parent_key = normalized_text(parent)
        expected_instances = Counter(instances)
        expected_atoms: Counter[tuple[str, str, str]] = Counter()
        for instance in instances:
            for name, field_values in instance:
                expected_atoms.update((parent_key, normalized_text(name), value) for value in field_values)
        predicted_instances: Counter[tuple[tuple[str, tuple[str, ...]], ...]] = Counter()
        predicted_atoms: Counter[tuple[str, str, str]] = Counter()
        result = prediction.get(parent, [])
        if isinstance(result, list):
            field_order = [name for name, _ in instances[0]] if instances else []
            for item in result:
                if not isinstance(item, dict):
                    continue
                names = field_order or sorted(clean_text(name) for name in item)
                by_name = {clean_text(name): value for name, value in item.items()}
                instance = tuple((name, values(by_name.get(name))) for name in names)
                predicted_instances[instance] += 1
                for name, field_values in instance:
                    predicted_atoms.update((parent_key, normalized_text(name), value) for value in field_values)
        scores.structures.add(
            expected_atoms,
            predicted_atoms,
            exact=expected_instances == predicted_instances,
        )
        scores.count.add(len(instances), predicted_instances.total())
        scores.structure_count.add(len(instances), predicted_instances.total())

    for parent, pairs in gold.relations.items():
        parent_key = normalized_text(parent)
        expected = Counter((parent_key, head, tail) for head, tail in pairs)
        predicted: Counter[tuple[str, str, str]] = Counter()
        result = prediction.get(parent, [])
        if isinstance(result, list):
            for item in result:
                if isinstance(item, (tuple, list)) and len(item) == 2:
                    predicted[(parent_key, normalized_text(item[0]), normalized_text(item[1]))] += 1
                elif isinstance(item, dict) and {"head", "tail"} <= item.keys():
                    head, tail = values(item["head"]), values(item["tail"])
                    if len(head) == len(tail) == 1:
                        predicted[(parent_key, head[0], tail[0])] += 1
        scores.relations.add(expected, predicted)
        scores.count.add(len(pairs), predicted.total())
        scores.relation_count.add(len(pairs), predicted.total())


def parse_minima(items: list[str]) -> dict[str, float]:
    minima: dict[str, float] = {}
    for item in items:
        key, separator, raw = item.partition("=")
        if not separator or key not in REQUIRED_MINIMA or key in minima:
            raise ValueError(f"invalid or duplicate --min-metric {item!r}")
        try:
            value = float(raw)
        except ValueError as exc:
            raise ValueError(f"invalid --min-metric value {item!r}") from exc
        if not math.isfinite(value) or not 0 < value <= 1:
            raise ValueError(f"--min-metric values must be finite numbers in (0,1]: {item!r}")
        minima[key] = value
    missing = REQUIRED_MINIMA - minima.keys()
    if missing:
        raise ValueError(f"missing required --min-metric values: {','.join(sorted(missing))}")
    return minima


def metric(metrics: dict[str, Any], key: str) -> float | None:
    section, name = key.split(".", 1)
    value = metrics.get(section, {}).get(name)
    return float(value) if isinstance(value, (int, float)) else None


def verify_upstream(source: Path, expected_commit: str = UPSTREAM_COMMIT) -> str:
    """Compatibility wrapper around the non-overridable pinned oracle check."""
    if expected_commit != UPSTREAM_COMMIT:
        raise ValueError(f"the Python oracle commit is fixed at {UPSTREAM_COMMIT}")
    return verify_upstream_checkout(source)["commit"]


def batches(items: Iterable[tuple[str, dict[str, Any]]], size: int) -> Iterator[list[tuple[str, dict[str, Any]]]]:
    batch: list[tuple[str, dict[str, Any]]] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--adapter-dir", type=Path)
    parser.add_argument(
        "--comparison-report",
        type=Path,
        help="Derive the base model and Zig adapter from a comparison_report.json",
    )
    parser.add_argument("--eval-data", type=Path, required=True)
    parser.add_argument("--upstream-source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-metric", action="append", default=[])
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--max-length", type=int)
    args = parser.parse_args()

    report: dict[str, Any] = {"pass": False}
    try:
        if not 0 <= args.threshold <= 1 or not math.isfinite(args.threshold):
            raise ValueError("--threshold must be finite and in [0,1]")
        if args.batch_size < 1 or args.batch_size > 256:
            raise ValueError("--batch-size must be in [1,256]")
        if args.max_records is not None and args.max_records < 1:
            raise ValueError("--max-records must be positive")
        if args.comparison_report:
            if args.model_dir or args.adapter_dir:
                raise ValueError("--comparison-report cannot be combined with --model-dir/--adapter-dir")
            comparison = json.loads(args.comparison_report.read_text(encoding="utf-8"))
            comparison_config = comparison.get("config")
            if not isinstance(comparison_config, dict):
                raise ValueError("comparison report has no config object")
            model_value = comparison_config.get("python_model") or comparison_config.get("model_dir")
            if not isinstance(model_value, str) or not model_value:
                raise ValueError("comparison report has no base model path/id")
            args.model_dir = Path(model_value)
            args.adapter_dir = args.comparison_report.parent / "zig"
        if args.model_dir is None or args.adapter_dir is None:
            raise ValueError("provide --comparison-report or both --model-dir and --adapter-dir")
        minima = parse_minima(args.min_metric)
        normalization_runtime = verify_canonical_python_runtime()
        package_versions = verify_canonical_oracle_packages()
        oracle = verify_upstream_checkout(args.upstream_source.resolve())
        for name in ("adapter_config.json", "adapter_model.safetensors"):
            if not (args.adapter_dir / name).is_file():
                raise ValueError(f"adapter is missing {name}")
        adapter_config = json.loads((args.adapter_dir / "adapter_config.json").read_text(encoding="utf-8"))
        if adapter_config.get("peft_type") != "LORA" or adapter_config.get("task_type") is not None:
            raise ValueError("adapter must use standard task-agnostic PEFT LoRA format")

        sys.dont_write_bytecode = True
        sys.path.insert(0, str(args.upstream_source.resolve()))
        import torch
        from gliner2 import GLiNER2, __version__ as gliner2_version
        from peft import PeftModel

        imported_from = verify_import_source(GLiNER2, args.upstream_source)
        if gliner2_version != CANONICAL_GLINER2_VERSION:
            raise ValueError(
                f"GLiNER2 source version {gliner2_version} does not match {CANONICAL_GLINER2_VERSION}"
            )
        torch.manual_seed(0)
        torch.use_deterministic_algorithms(True)
        base = GLiNER2.from_pretrained(str(args.model_dir), map_location="cpu")
        model = PeftModel.from_pretrained(base, str(args.adapter_dir), is_trainable=False)
        model.eval()

        scores = Scores()
        record_count = 0
        for batch in batches(rows(args.eval_data, args.max_records), args.batch_size):
            ids: list[str] = []
            texts: list[str] = []
            schemas: list[dict[str, Any]] = []
            golds: list[Gold] = []
            for record_id, record in batch:
                try:
                    text, schema, gold = schema_and_gold(record)
                except ValueError as exc:
                    raise ValueError(f"{record_id}: {exc}") from exc
                ids.append(record_id)
                texts.append(text)
                schemas.append(schema)
                golds.append(gold)
            predictions = model.batch_extract(
                texts,
                schemas,
                batch_size=len(batch),
                threshold=args.threshold,
                num_workers=0,
                format_results=False,
                include_confidence=False,
                include_spans=False,
                max_len=args.max_length,
            )
            if len(predictions) != len(batch):
                raise ValueError("upstream decoder returned the wrong prediction count")
            for record_id, gold, prediction in zip(ids, golds, predictions):
                try:
                    score_prediction(scores, gold, prediction)
                except ValueError as exc:
                    raise ValueError(f"{record_id}: {exc}") from exc
            record_count += len(batch)
        if record_count == 0:
            raise ValueError("eval data contains no records")

        metrics = scores.result()
        coverage_failures = [
            task
            for task in ("entities", "classifications", "json_structures", "relations")
            if metrics[task]["cases"] == 0 or metrics[task]["gold_atoms"] == 0
        ]
        if metrics["count"]["cases"] == 0:
            coverage_failures.append("count")
        failures = [f"eval data has no positive {task} coverage" for task in coverage_failures]
        for key, minimum in sorted(minima.items()):
            actual = metric(metrics, key)
            if actual is None or actual < minimum:
                failures.append(f"{key}={actual} below minimum {minimum}")
        count_minimum = minima["count.accuracy"]
        for task in ("json_structures", "relations"):
            actual = metrics[task]["count"]["accuracy"]
            if actual is None or actual < count_minimum:
                failures.append(f"{task}.count.accuracy={actual} below minimum {count_minimum}")
        report = {
            "contract": EVALUATION_CONTRACT,
            "pass": not failures,
            "failures": failures,
            "record_count": record_count,
            "metrics": metrics,
            "minimums": minima,
            "inference": {
                "device": "cpu",
                "threshold": args.threshold,
                "batch_size": args.batch_size,
                "max_length": args.max_length,
                "normalization": CANONICAL_NORMALIZATION,
                "normalization_runtime": normalization_runtime,
                "unicode_version": CANONICAL_UNICODE_VERSION,
                "relation_semantics": "upstream public head/tail decoder only",
            },
            "oracle": {
                **oracle,
                "imported_module": imported_from,
                "gliner2_version": gliner2_version,
                "python_version": platform.python_version(),
                "unicode_version": CANONICAL_UNICODE_VERSION,
                "torch_version": torch.__version__,
                "package_versions": package_versions,
            },
            # Retain the old report key for readers while making its checkout
            # and imported-module meanings unambiguous in ``oracle`` above.
            "upstream": {"commit": oracle["commit"], "version": gliner2_version, "source": imported_from},
            "model_dir": str(args.model_dir),
            "adapter_dir": str(args.adapter_dir),
            "eval_data": str(args.eval_data),
            "artifacts": {
                "base_model_fingerprint_sha256": base_model_fingerprint(args.model_dir),
                "peft_adapter_fingerprint_sha256": peft_adapter_fingerprint(args.adapter_dir),
                "adapter_bundle_fingerprint_sha256": (
                    adapter_bundle_fingerprint(args.adapter_dir)
                    if (args.adapter_dir / "task_head.safetensors").is_file()
                    else None
                ),
                "eval_data_fingerprint_sha256": path_fingerprint(args.eval_data),
            },
        }
    except Exception as exc:
        report = {"contract": EVALUATION_CONTRACT, "pass": False, "failures": [str(exc)]}

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"pass": report["pass"], "failures": report.get("failures", [])}, sort_keys=True))
    print(f"held-out quality report: {args.output}")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
