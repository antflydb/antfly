# Regenerate: python scripts/gliner25/decide_oracle.py <model_dir> [out.json]  (gliner2==2.0.0)
"""GLiNER2.5-Decide reference oracle: captures model-facing input ids and
classifier logits per classification task, plus final decisions."""

import hashlib
import importlib.metadata
import json
import os
import sys

import torch
from gliner2 import AutoExtractor

M = os.path.expanduser(
    sys.argv[1]
    if len(sys.argv) > 1
    else "~/.antfly/inference/models/fastino/GLiNER2.5-Decide"
)
OUT = (
    sys.argv[2]
    if len(sys.argv) > 2
    else os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "testdata",
        "gliner25",
        "decide",
        "cases.json",
    )
)
model = AutoExtractor.from_pretrained(M)
model.eval()

captured = {}
enc_mod = model.encoder
orig_enc_forward = enc_mod.forward


def enc_forward(*args, **kwargs):
    ids = kwargs.get("input_ids", args[0] if args else None)
    captured.setdefault("input_ids", []).append(ids.tolist())
    out = orig_enc_forward(*args, **kwargs)
    return out


enc_mod.forward = enc_forward
orig_cls_forward = model.classifier.forward


def cls_forward(x):
    y = orig_cls_forward(x)
    captured.setdefault("cls_logits", []).append(y.squeeze(-1).tolist())
    return y


model.classifier.forward = cls_forward

CASES = [
    (
        "support_intent",
        "My subscription renewed on April 15 for ¥5,400 after the service was already down. Can I get that charge refunded?",
        {
            "intent": [
                "order_status",
                "refund_request",
                "cancel_subscription",
                "update_payment",
                "login_problem",
                "shipping_delay",
                "bug_report",
                "speak_to_human",
                "other",
            ]
        },
    ),
    (
        "travel",
        "I need to move my Friday flight to Paris to Saturday morning, same cabin, and keep the aisle seat if you can.",
        {
            "request": [
                "book",
                "change",
                "cancel",
                "status",
                "seat_change",
                "refund",
                "baggage",
            ]
        },
    ),
    (
        "hotel_multi_task",
        "Guest in room 1408 says the AC has been out since yesterday and they want to move tonight or leave. They also asked for the incidentals hold to be released.",
        {
            "intent": [
                "maintenance",
                "room_change",
                "checkout",
                "billing",
                "complaint",
                "amenity_request",
            ],
            "priority": ["low", "normal", "high", "urgent"],
            "needs_human": ["yes", "no"],
            "topics": {
                "labels": ["hvac", "billing", "housekeeping", "noise", "safety"],
                "multi_label": True,
                "cls_threshold": 0.4,
            },
        },
    ),
    (
        "passage_question",
        "The treaty was signed in Paris in 1992. It entered into force the following year, after the last signatory ratified it.",
        {
            "answer": {
                "labels": ["yes", "no"],
                "prompt": "Did the treaty enter into force in 1992?",
            }
        },
    ),
    (
        "described_labels",
        "Please reset the card PIN. The new one never arrived and the old one is locked after three tries.",
        {
            "intent": {
                "labels": {
                    "card_pin_change": "The customer wants a new PIN or the current PIN replaced",
                    "card_lost": "The physical card is missing",
                    "balance_inquiry": "The customer wants the current balance",
                }
            }
        },
    ),
    (
        "ordinal",
        "I finished it in two nights. The ending is earned, the middle drags, and I would still hand it to a friend.",
        {"rating": [str(i) for i in range(11)]},
    ),
    (
        "book_genre",
        "She closed the ledger, blew out the lamp, and listened for the stair. The house had been empty since the winter the river took the bridge",
        {
            "genre": [
                "mystery",
                "romance",
                "history",
                "science_fiction",
                "literary_fiction",
                "cookbook",
            ]
        },
    ),
    (
        "sentiment_multi",
        "Great food but the service was painfully slow and our waiter was rude!",
        {
            "sentiment": ["positive", "negative", "mixed", "neutral"],
            "aspects": {
                "labels": ["food", "service", "price", "ambience"],
                "multi_label": True,
            },
        },
    ),
]


def to_v2(tasks):
    """The same task set in the antfly schema_version:2 wire form."""
    out = []
    for name, cfg in tasks.items():
        cfg = cfg if isinstance(cfg, dict) else {"labels": cfg}
        labels = cfg["labels"]
        entry = {"name": name, "labels": list(labels)}
        if isinstance(labels, dict):
            entry["label_definitions"] = {
                k: {"description": v} for k, v in labels.items()
            }
        if cfg.get("multi_label"):
            entry["multi_label"] = True
        if "cls_threshold" in cfg:
            entry["threshold"] = cfg["cls_threshold"]
        if "prompt" in cfg:
            entry["prompt"] = cfg["prompt"]
        out.append(entry)
    return {"classifications": out}


cases = []
with torch.no_grad():
    for name, text, tasks in CASES:
        captured.clear()
        result = model.classify_text(text, tasks, include_confidence=True)
        ids = captured["input_ids"]
        assert len(ids) == 1 and len(ids[0]) == 1, "expected one unbatched forward"
        # Classifier runs once per task, in schema order.
        logits = captured.get("cls_logits", [])
        cases.append(
            {
                "name": name,
                "text": text,
                "tasks": tasks,
                "v2_schema": to_v2(tasks),
                "input_ids": ids[0][0],
                "classifier_logits": logits,
                "result": result,
            }
        )
        print(name, json.dumps(result))

    # Entity extraction (span head + CountLSTM v1) reference.
    ent_cases = []
    for text, labels in [
        (
            "My name is Clara and I live in Berkeley, California.",
            ["person", "location"],
        ),
        (
            "Apple CEO Tim Cook announced the iPhone 17 in Cupertino on September 9.",
            ["person", "organization", "product", "location", "date"],
        ),
    ]:
        r = model.extract_entities(
            text, labels, include_confidence=True, include_spans=True
        )
        ent_cases.append({"text": text, "labels": labels, "result": r})
        print("entities", json.dumps(r))


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


provenance = {
    "model": "fastino/GLiNER2.5-Decide",
    "versions": {
        pkg: importlib.metadata.version(pkg)
        for pkg in ("gliner2", "torch", "transformers")
    },
    "model_files": {
        name: sha256(os.path.join(M, name))
        for name in (
            "model.safetensors",
            "config.json",
            "encoder_config/config.json",
            "tokenizer.json",
        )
    },
}
with open(OUT, "w") as handle:
    json.dump(
        {**provenance, "classification": cases, "entities": ent_cases},
        handle,
        indent=1,
        ensure_ascii=False,
    )
print("wrote", OUT)
