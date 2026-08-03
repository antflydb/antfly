"""Shared record normalization for the GLiNER2 cross-runtime parity harness."""

from __future__ import annotations

from typing import Any


def parse_label_csv(labels_csv: str) -> set[str]:
    return {label.strip() for label in labels_csv.split(",") if label.strip()}


def allowed_labels_for_objective(objective: str, labels_csv: str) -> set[str] | None:
    # Full total-loss schemas determine their own contextual vocabulary. The
    # CSV is only a legacy span-start label filter.
    return None if objective == "gliner2-total-loss" else parse_label_csv(labels_csv)


def summarize_upstream_output(output: dict[str, Any], labels: set[str], allowed_labels: set[str] | None) -> dict[str, int]:
    counts = {
        "entity_mentions": 0,
        "classifications": len(output.get("classifications", []) or []),
        "json_structures": len(output.get("json_structures", []) or []),
        "relations": len(output.get("relations", []) or []),
    }
    for label, mentions in (output.get("entities") or {}).items():
        if allowed_labels is not None and label not in allowed_labels:
            continue
        labels.add(label)
        counts["entity_mentions"] += len(mentions or [])
    return counts


def ensure_terminal_punctuation(text: str) -> str:
    """Mirror upstream SchemaTransformer text normalization."""
    if text and not text.endswith((".", "!", "?")):
        return text + "."
    return text or "."


def normalize_python_record(record: dict[str, Any], allowed_labels: set[str] | None) -> tuple[dict[str, Any], dict[str, int], set[str]]:
    labels: set[str] = set()
    if "input" in record and "output" in record:
        output = dict(record.get("output") or {})
        if allowed_labels is not None and "entities" in output:
            output["entities"] = {
                label: mentions
                for label, mentions in (output.get("entities") or {}).items()
                if label in allowed_labels
            }
        counts = summarize_upstream_output(output, labels, allowed_labels)
        return {"input": ensure_terminal_punctuation(record["input"]), "output": output}, counts, labels

    grouped: dict[str, list[str]] = {}
    for ent in record.get("entities", []):
        label = ent["label"]
        if allowed_labels is not None and label not in allowed_labels:
            continue
        grouped.setdefault(label, []).append(ent["text"])
        labels.add(label)
    return (
        {"input": ensure_terminal_punctuation(record["text"]), "output": {"entities": grouped}},
        {"entity_mentions": sum(len(v) for v in grouped.values()), "classifications": 0, "json_structures": 0, "relations": 0},
        labels,
    )
