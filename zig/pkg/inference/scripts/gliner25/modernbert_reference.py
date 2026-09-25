#!/usr/bin/env python3
"""Tiny ModernBERT GLiNER2.5 boundary reference for the native training encoder.

Builds a byte-level BPE tokenizer and a three-layer ModernBERT
``BoundaryExtractor`` with the published boundary-head settings on the pinned
upstream (``oracle.py``), saves it as a boundary checkpoint, and captures one
padded batch: the processor's token and routing tensors, the encoder's routed
text/query/classification states, and the gradient of every encoder weight for
fixed cotangents on those states.

The tokenizer and ``processor.json`` (token ids and routes) are small and are
checked in under ``testdata/gliner25/modernbert_tokenizer``; the checkpoint
and ``reference.safetensors`` are generated locally, and the tests that need
them read ``ANTFLY_GLINER25_MODERNBERT_REFERENCE``.

Upstream tokenizes every word and schema fragment on its own with
``tokenizer.tokenize(token)`` and adds no [CLS]/[SEP]. With ModernBERT's
byte-level pre-tokenizer (``add_prefix_space=False``) an isolated word gets
its start-of-text form ("john", not "Ġjohn"); ``tokenization`` in the report
records that, and the native processor must match it.

Diagnostic evidence only: this is not a pretrained model or a qualification.

    PYTHONDONTWRITEBYTECODE=1 <oracle venv>/bin/python modernbert_reference.py \\
        --upstream <GLiNER2 checkout at oracle.UPSTREAM_COMMIT> \\
        --output <dir outside Git>
    cp <dir>/checkpoint/tokenizer.json <dir>/processor.json \\
        ../../testdata/gliner25/modernbert_tokenizer/
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import oracle

SEED = 17
COTANGENT_SEED = 2025
MAX_WORDS = 128

# Each case carries the upstream schema and the native schema JSON for the
# same tasks (the formats differ; see boundary_engine.zig's parity test).
CASES = [
    {
        "id": "mixed_tasks",
        "text": "John works at Apple. Alice works at Google.",
        "upstream_schema": {"entities": ["person", "organization"],
                            "classifications": [{"task": "sentiment", "labels": ["positive", "negative"]}],
                            "relations": ["works_for"]},
        "native_schema": '{"entities":["person","organization"],"relations":[{"type":"works_for"}],'
                         '"classifications":[{"name":"sentiment","labels":["positive","negative"]}]}',
    },
    {
        "id": "unicode_offsets",
        "text": "İpek works at Apple in 東京. 🙂 Alice visits café é.",
        "upstream_schema": {"entities": {"person": "A named person", "location": "A named place"}},
        "native_schema": '{"entities":["person","location"],"entity_definitions":'
                         '{"person":{"description":"A named person"},"location":{"description":"A named place"}}}',
    },
]

SPECIALS = ["[PAD]", "[UNK]", "[CLS]", "[SEP]", "[MASK]"]


def build_tokenizer() -> Any:
    """A ModernBERT-style byte-level BPE with [PAD] at id 0 (the native pad id)."""
    from tokenizers import Tokenizer, decoders, models, pre_tokenizers, processors, trainers
    from transformers import PreTrainedTokenizerFast

    corpus = [case["text"] for case in CASES] + [
        "person organization location sentiment positive negative works_for",
        "a named person a named place entities classifications relations",
        "( ) , | the of and to in at john alice apple google",
    ]
    tokenizer = Tokenizer(models.BPE())
    tokenizer.pre_tokenizer = pre_tokenizers.ByteLevel(add_prefix_space=False, use_regex=True)
    tokenizer.decoder = decoders.ByteLevel()
    trainer = trainers.BpeTrainer(vocab_size=384, special_tokens=SPECIALS, show_progress=False,
                                  initial_alphabet=pre_tokenizers.ByteLevel.alphabet())
    tokenizer.train_from_iterator(corpus, trainer=trainer)
    cls, sep = tokenizer.token_to_id("[CLS]"), tokenizer.token_to_id("[SEP]")
    tokenizer.post_processor = processors.TemplateProcessing(
        single="[CLS] $A [SEP]", pair="[CLS] $A [SEP] $B [SEP]",
        special_tokens=[("[CLS]", cls), ("[SEP]", sep)])
    if tokenizer.token_to_id("[PAD]") != 0:
        raise oracle.ContractError("the fixture tokenizer must pad with id 0")
    return PreTrainedTokenizerFast(tokenizer_object=tokenizer, pad_token="[PAD]", unk_token="[UNK]",
                                   cls_token="[CLS]", sep_token="[SEP]", mask_token="[MASK]")


def encoder_config(vocab_size: int) -> Any:
    from transformers import ModernBertConfig

    # Layers 0 and 2 are global, layer 1 local; the 8-token window (+-4) is
    # shorter than every sequence, so the local mask is exercised.
    return ModernBertConfig(
        vocab_size=vocab_size, hidden_size=32, intermediate_size=48, num_hidden_layers=3,
        num_attention_heads=4, hidden_activation="gelu", max_position_embeddings=512,
        initializer_range=0.2, norm_eps=1e-5, norm_bias=False, pad_token_id=0, bos_token_id=2,
        cls_token_id=2, eos_token_id=3, sep_token_id=3, global_rope_theta=160000.0,
        local_rope_theta=10000.0, global_attn_every_n_layers=2, local_attention=8,
        attention_bias=False, attention_dropout=0.0, embedding_dropout=0.0, mlp_bias=False,
        mlp_dropout=0.0, reference_compile=False)


def build_model(torch: Any) -> Any:
    from gliner2 import BoundaryExtractor, ExtractorConfig

    from gliner2.processor import SchemaTransformer

    tokenizer = build_tokenizer()
    # The processor adds the schema markers (a no-op after this); size the
    # embedding for them so the extractor's resize_token_embeddings is the identity.
    tokenizer.add_special_tokens({"additional_special_tokens": SchemaTransformer.SPECIAL_TOKENS})
    vocab = len(tokenizer)
    # Published head settings, so the checkpoint passes the native training
    # source's head-shape checks; only the encoder is tiny.
    head = oracle.read_json(oracle.FIXTURES / "models" / "base" / "config.json")["boundary_head"]
    config = ExtractorConfig(model_name="tiny-modernbert-fixture", architecture="boundary",
                             boundary_head=head, token_pooling="first")
    torch.manual_seed(SEED)
    model = BoundaryExtractor(config, encoder_config=encoder_config(vocab), tokenizer=tokenizer,
                              use_flashdeberta=False).float().cpu().eval()
    if model.encoder.config.vocab_size != vocab or model.encoder.config.model_type != "modernbert":
        raise oracle.ContractError("unexpected tiny ModernBERT encoder")
    return model


def capture(args: argparse.Namespace) -> dict[str, Any]:
    provenance, torch = oracle.prepare_runtime(args.upstream)
    model = build_model(torch)
    tokenizer = model.processor.tokenizer
    batch = model.processor.collate_fn_inference(
        [(case["text"], oracle.build_extract_schema(case["upstream_schema"]).build()) for case in CASES],
        max_len=MAX_WORDS, architecture="boundary", error_policy="raise", build_targets=False,
        on_capacity_exceeded="raise")
    lengths = batch.attention_mask.sum(-1).tolist()
    if len(set(lengths)) != 2 or min(lengths) <= 8:
        raise oracle.ContractError("the batch must pad one row and exceed the local window")

    encoder = model.encoder
    names = [name for name, _ in encoder.named_parameters()]
    hidden = encoder(input_ids=batch.input_ids, attention_mask=batch.attention_mask).last_hidden_state

    def routed(indices: Any, mask: Any) -> Any:
        # Identical to upstream BoundaryExtractor._encode_core's gather_routed.
        safe = indices.clamp(0, hidden.shape[1] - 1)
        states = hidden.gather(1, safe.unsqueeze(-1).expand(-1, -1, hidden.shape[-1]))
        return states * mask.unsqueeze(-1).to(states.dtype)

    routes = {"text": (batch.text_word_indices, batch.text_word_mask),
              "query": (batch.query_marker_indices, batch.query_marker_mask),
              "cls": (batch.cls_marker_indices, batch.cls_marker_mask)}
    states = {kind: routed(*route) for kind, route in routes.items()}
    generator = torch.Generator().manual_seed(COTANGENT_SEED)
    cotangents = {kind: torch.randn(value.shape, generator=generator) * routes[kind][1].unsqueeze(-1)
                  for kind, value in states.items()}
    objective = sum((states[kind] * cotangents[kind]).sum() for kind in states)
    gradients = torch.autograd.grad(objective, list(encoder.parameters()))

    with oracle.atomic_output_directory(args.output) as directory:
        checkpoint = directory / "checkpoint"
        model.save_pretrained(str(checkpoint))
        tensors = {"input.ids": batch.input_ids, "input.attention_mask": batch.attention_mask}
        for kind, (indices, mask) in routes.items():
            tensors[f"route.{kind}.indices"] = indices
            tensors[f"route.{kind}.mask"] = mask
            tensors[f"encoded.{kind}"] = states[kind]
            tensors[f"cotangent.{kind}"] = cotangents[kind]
        tensors["encoded.hidden"] = hidden
        # Native graph names strip the checkpoint's `encoder.` prefix.
        for name, gradient in zip(names, gradients):
            tensors[f"gradient.{name}"] = gradient
        # The word as upstream sees it, and the same word after a space.
        words = ["john", "works", "apple", "café", "東京"]
        report = {
            "format_version": 1, "status": "captured", "scope": "tiny_modernbert_encoder_diagnostic",
            "real_model_qualified": False, "native_runtime_qualified": False, "training_qualified": False,
            "provenance": provenance, "generator_sha256": oracle.sha256_file(Path(__file__)),
            "seed": SEED, "cotangent_seed": COTANGENT_SEED,
            "cases": [{key: case[key] for key in ("id", "text", "native_schema")} for case in CASES],
            "encoder_parameters": names,
            "tokenization": {
                "rule": "each word and schema fragment is tokenized alone, without [CLS]/[SEP]",
                "words": {word: {"alone": tokenizer.tokenize(word), "after_space": tokenizer.tokenize(" " + word)}
                          for word in words},
            },
            "tensors": oracle.save_tensors(directory / "reference.safetensors", tensors, torch),
        }
        oracle.verify_upstream_checkout(args.upstream)
        oracle.write_json(directory / "capture.json", report)
        oracle.write_json(directory / "processor.json", {
            "format_version": 1, "upstream_commit": oracle.UPSTREAM_COMMIT,
            "generator_sha256": report["generator_sha256"], "cases": report["cases"],
            "tokenization": report["tokenization"],
            "input_ids": batch.input_ids.tolist(), "attention_mask": batch.attention_mask.tolist(),
            "routes": {kind: {"indices": indices.tolist(), "mask": mask.tolist()}
                       for kind, (indices, mask) in routes.items()},
        })
    return {"status": "captured", "output": str(args.output.resolve()), "sequence_lengths": lengths,
            "encoder_parameters": len(names)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    print(json.dumps(capture(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
