#!/usr/bin/env python3
"""Tiny actual GLiNER2.5 mixed-task forward/backward/AdamW composition.

No checkpoint or corpus is loaded. The first profile deliberately uses zero
dropout, fixed complete schemas, explicit negative-query draws, and tiny random
DeBERTa weights. Upstream losses and optimizer methods execute unchanged.
"""
from __future__ import annotations

import argparse
import ast
import copy
from dataclasses import fields, is_dataclass
import hashlib
import json
import logging
import math
from pathlib import Path
from types import SimpleNamespace

import oracle


def examples():
    records = {
        "deal": {"mode": "natural", "anchor": "party", "fields": {
            "party": {"cardinality": "required_one"}, "state": {"cardinality": "optional_one"}}},
        "event": {"mode": "latent", "fields": {
            "actor": {"cardinality": "required_one"}, "tags": {"cardinality": "zero_or_more"}}},
        "notice": {"mode": "anchorless", "fields": {
            "actor": {"cardinality": "optional_one"}, "tags": {"cardinality": "one_or_more"}}},
    }
    rich = {
        "json_structures": [
            {"deal": {"party": "Ada", "state": {"value": "paid", "choices": ["paid", "due"]}}},
            {"deal": {"party": "Bob", "state": "due"}},
            {"event": {"actor": "Ada", "tags": ["Acme", "12"]}},
            {"notice": {"actor": "Bob", "tags": ["met"]}},
            {"invoice": {"amount": "12"}},
        ],
        "entities": {"person": ["Ada", "Bob"], "company": ["Acme"],
                     "negative": ["Bob"], "positive": ["Ada"]},
        "relations": [{"met": {"head": "Ada", "tail": "Acme"}}],
        "classifications": [
            {"task": "sentiment", "labels": ["good", "bad"], "true_label": ["good"]},
            {"task": "topics", "labels": ["meeting", "billing"], "true_label": ["billing"], "multi_label": True},
        ],
        "record_metadata": records,
    }
    negative = {"entities": {"person": []}, "classifications": [
        {"task": "sentiment", "labels": ["good", "bad"], "true_label": []},
    ]}
    def source(start, end):
        return {"start": start, "end": end, "unit": "utf8_bytes"}

    def document(start, end):
        return {"document": [source(start, end)]}

    def field(index, *values):
        return {"field": index, "values": list(values)}

    def sample(text, upstream, schema, **annotations):
        # Map order defines prompt and query order. Preserve it inside a JSON
        # string because the outer provenance writer sorts mapping keys.
        return {"text": text, "upstream": upstream, "schema_version": 2,
                "schema_json": json.dumps(schema, ensure_ascii=False, separators=(",", ":")),
                "annotations": {"schema_fingerprint": [0] * 32, **annotations}}

    rich_schema = {
        "entities": ["person", "company"],
        "entity_attributes": {"tone": {"labels": ["positive", "negative"], "applies_to": ["person"]}},
        "classifications": [{"name": "sentiment", "labels": ["good", "bad"]},
                            {"name": "topics", "labels": ["meeting", "billing"], "mode": "multi"}],
        "structures": {
            "deal": {"mode": "natural", "anchor": "party", "fields": {
                "party": {"dtype": "str", "cardinality": "required_one"},
                "state": {"dtype": "str", "choices": ["paid", "due"], "cardinality": "optional_one"}}},
            "event": {"mode": "latent", "fields": {
                "actor": {"dtype": "str", "cardinality": "required_one"},
                "tags": {"dtype": "list", "cardinality": "zero_or_more"}}},
            "notice": {"mode": "anchorless", "fields": {
                "actor": {"dtype": "str", "cardinality": "optional_one"},
                "tags": {"dtype": "list", "cardinality": "one_or_more"}}},
            "invoice": {"fields": {"amount": {"dtype": "str"}}}},
        "relations": [{"type": "met"}],
    }
    negative_schema = {"entities": ["person"], "classifications": [{"name": "sentiment", "labels": ["good", "bad"]}]}
    return [
        {"id": "mixed", "samples": [
            sample("Ada met Acme. Bob paid 12.", rich, rich_schema,
                entities=[{"entity_type": 0, "source": source(0, 3), "attributes": [{"group": 0, "labels": [0]}]},
                          {"entity_type": 1, "source": source(8, 12)},
                          {"entity_type": 0, "source": source(14, 17), "attributes": [{"group": 0, "labels": [1]}]}],
                classifications=[{"task": 0, "labels": [0]}, {"task": 1, "labels": [1]}],
                records=[{"structure": 0, "id": "0:0", "fields": [field(0, document(0, 3)), field(1, {"choice": 0})]},
                         {"structure": 0, "id": "0:1", "fields": [field(0, document(14, 17)), field(1, {"choice": 1})]},
                         {"structure": 1, "id": "1:0", "fields": [field(0, document(0, 3)), field(1, document(8, 12), document(23, 25))]},
                         {"structure": 2, "id": "2:0", "fields": [field(0, document(14, 17)), field(1, document(4, 7))]},
                         {"structure": 3, "id": "3:0", "fields": [field(0, document(23, 25))]}],
                relations=[{"relation_type": 0, "head": {"entity": 0}, "tail": {"entity": 1}}]),
            sample("Ada.", {"entities": {"person": ["Ada"]}}, {"entities": ["person"]},
                   entities=[{"entity_type": 0, "source": source(0, 3)}]),
        ], "classification_supervised": True, "expected_record_count": 4, "expected_relation_count": 1},
        {"id": "unsupervised_classification", "samples": [
            sample("No matches.", negative, negative_schema),
            sample("Empty.", negative, negative_schema),
        ], "classification_supervised": False, "expected_record_count": 0, "expected_relation_count": 0},
    ]


def trainer_methods(source: Path, torch):
    path = source / "gliner2/training/trainer.py"
    text = path.read_text()
    trainer = next(node for node in ast.parse(text).body
                   if isinstance(node, ast.ClassDef) and node.name == "ExtractorTrainer")
    names = {"_create_optimizer", "_renormalize_partial_accumulation", "_optimizer_step"}
    methods = [node for node in trainer.body if isinstance(node, ast.FunctionDef) and node.name in names]
    if len(methods) != len(names) or any(method.decorator_list for method in methods):
        raise oracle.ContractError("pinned trainer method selection differs")
    namespace = {"AdamW": torch.optim.AdamW, "torch": torch, "logger": logging.getLogger(__name__)}
    exec(compile(ast.Module(body=methods, type_ignores=[]), str(path), "exec"), namespace)
    pins = {method.name: {"lines": [method.lineno, method.end_lineno],
                         "sha256": hashlib.sha256(ast.get_source_segment(text, method).encode()).hexdigest()}
            for method in methods}
    return namespace, pins


def capture(source: Path, output: Path, modes):
    provenance, torch = oracle.prepare_runtime(source)
    from gliner2 import BoundaryExtractor, ExtractorConfig
    from transformers import DebertaV2Config
    from tests.fixtures import tiny_tokenizer
    import gliner2.models.boundary.model as model_module
    import gliner2.models.boundary.records as record_module

    oracle.verify_import_source(tiny_tokenizer, source)
    methods, method_pins = trainer_methods(source, torch)
    source_files = [
        "gliner2/models/base.py", "gliner2/models/boundary/model.py", "gliner2/models/boundary/pool.py",
        "gliner2/models/boundary/records.py", "gliner2/models/boundary/relations.py",
        "gliner2/models/boundary/losses.py", "gliner2/training/trainer.py", "gliner2/training/lora.py",
        "gliner2/processor.py", "gliner2/processing/boundary_preprocessing.py",
        "gliner2/processing/targets.py", "tests/fixtures/tiny_tokenizer.py",
    ]
    settings = oracle.read_json(oracle.FIXTURES / "models/small/config.json")["boundary_head"]
    settings.update(boundary_dim=8, pair_dim=8, content_dim=4, record_dim=8,
                    boundary_attention_heads=2, boundary_attention_layers=1, boundary_attention_window=1,
                    multihead_pair_compat_heads=2, candidate_attention_heads=2,
                    record_instance_queries=2, pool_size=16, pool_boundary_top_k=16, min_pool_per_query=2,
                    candidate_budget=16, training_candidate_budget=16, max_gold_per_query=8,
                    start_top_k=8, end_top_k=8, ends_per_start=4, starts_per_end=4,
                    end_block_size=16, relation_heads_per_type=16, relation_tails_per_type=16,
                    relation_pair_cap=256, relation_argument_proposal_threshold=0.0,
                    dropout=0.0)
    batches = examples()
    all_tensors = {}
    tokenizer_data = None
    tokenizer_config = None
    fragment_ids = {}

    def save(key, tensor):
        if key in all_tensors:
            raise oracle.ContractError(f"duplicate tensor key: {key}")
        value = tensor.detach().cpu().contiguous().clone()
        if value.is_floating_point() and not bool(torch.isfinite(value).all()):
            raise oracle.ContractError(f"nonfinite mixed-step tensor: {key}")
        all_tensors[key] = value
        if sum(t.numel() * t.element_size() for t in all_tensors.values()) > 16 * 1024 * 1024:
            raise oracle.ContractError("mixed-step tensor capture exceeds 16 MiB")
        return key

    def named_tensor(prefix, values):
        return {name: save(f"{prefix}.{name}", tensor) for name, tensor in values.items()}

    def structure(prefix, value):
        """Preserve the exact target/routing tree, with typed tensors externalized."""
        if isinstance(value, torch.Tensor):
            return {"tensor": save(prefix, value)}
        if is_dataclass(value):
            return {field.name: structure(prefix + "." + field.name, getattr(value, field.name))
                    for field in fields(value)}
        if isinstance(value, dict):
            return {str(key): structure(prefix + "." + str(key), child) for key, child in value.items()}
        if isinstance(value, (tuple, list)):
            return [structure(prefix + "." + str(index), child) for index, child in enumerate(value)]
        if value is None or isinstance(value, (str, bool, int, float)):
            return value
        raise oracle.ContractError(f"unsupported captured target/routing value: {prefix}: {type(value).__name__}")

    def numeric(value):
        result = float(value.detach())
        if not math.isfinite(result):
            raise oracle.ContractError("nonfinite scalar objective")
        return result

    reports = []
    for mode in modes:
        torch.manual_seed(253955)
        tokenizer = tiny_tokenizer.build_tiny_tokenizer(extra_words=[
            "ada", "bob", "acme", "met", "paid", "due", "12", "good", "bad", "meeting", "billing",
            "sentiment", "topics", "deal", "party", "state", "event", "actor", "tags", "notice", "invoice",
            "amount", "no", "matches", "empty", "negative", "positive",
        ])
        encoder_config = DebertaV2Config(vocab_size=len(tokenizer), hidden_size=16, intermediate_size=32,
            num_hidden_layers=2, num_attention_heads=4, max_position_embeddings=256, position_buckets=16,
            max_relative_positions=32, relative_attention=True, position_biased_input=False,
            norm_rel_ebd="layer_norm", share_att_key=True, pos_att_type=["p2c", "c2p"],
            type_vocab_size=0, layer_norm_eps=1e-7, hidden_act="gelu", hidden_dropout_prob=0,
            attention_probs_dropout_prob=0, pad_token_id=tokenizer.pad_token_id)
        config = ExtractorConfig(model_name="tiny-deberta-training-fixture", architecture="boundary",
                                 boundary_head=copy.deepcopy(settings), token_pooling="first", attn_implementation="eager")
        base = BoundaryExtractor(config, encoder_config=encoder_config, tokenizer=tokenizer,
                                 use_flashdeberta=False).float().cpu().train()
        current_tokenizer = json.loads(tokenizer.backend_tokenizer.to_str())
        current_tokenizer_config = {"tokenizer_class": "PreTrainedTokenizerFast", "vocab_size": len(tokenizer),
            "special_tokens": tokenizer.special_tokens_map, "pad_token_id": tokenizer.pad_token_id,
            "cls_token_id": tokenizer.cls_token_id, "sep_token_id": tokenizer.sep_token_id,
            "unk_token_id": tokenizer.unk_token_id, "mask_token_id": tokenizer.mask_token_id,
            "add_special_tokens_per_fragment": False}
        if tokenizer_data is not None and (current_tokenizer != tokenizer_data or current_tokenizer_config != tokenizer_config):
            raise oracle.ContractError("tiny tokenizer identity changed across profiles")
        tokenizer_data, tokenizer_config = current_tokenizer, current_tokenizer_config
        if mode == "heads":
            for parameter in base.encoder.parameters():
                parameter.requires_grad_(False)
        model = base.apply_lora(r=2, alpha=3, dropout=0, targets=["encoder"], use_dora=mode == "dora") if mode in ("lora", "dora") else base
        model.train()
        base.boundary_head.set_consistency_scale(0.5)
        base.boundary_head.set_soft_iou_scale(0.75)
        base.boundary_head.set_gold_injection_prob(1.0)
        parameters = dict(model.named_parameters())
        trainable = {name: value for name, value in parameters.items() if value.requires_grad}
        if not trainable:
            raise oracle.ContractError("empty mixed-step training selection")
        options = SimpleNamespace(adam_beta1=0.9, adam_beta2=0.999, adam_epsilon=1e-8,
            encoder_lr=1e-5, task_lr=5e-4, weight_decay=0.01, use_lora=mode in ("lora", "dora"),
            fused_optimizer=False, fp16=False, gradient_accumulation_steps=2, max_grad_norm=0.7)
        controller = SimpleNamespace(model=model, config=options, device=SimpleNamespace(type="cpu"),
                                     scheduler=SimpleNamespace(step=lambda: None))
        controller.optimizer = methods["_create_optimizer"](controller)
        optimizer = controller.optimizer
        parameter_ids = {id(parameter): name for name, parameter in parameters.items()}
        groups = [{"parameters": [parameter_ids[id(p)] for p in group["params"]],
                   "lr": group["lr"], "weight_decay": group["weight_decay"],
                   "betas": list(group["betas"]), "eps": group["eps"], "foreach": group["foreach"]}
                  for group in optimizer.param_groups]
        report = {"mode": mode, "config": config.to_dict(), "encoder_config": base.encoder.config.to_dict(),
                  "parameters": [{"name": name, "shape": list(parameter.shape), "trainable": parameter.requires_grad}
                                 for name, parameter in parameters.items()], "optimizer_groups": groups,
                  "initial": named_tensor(f"{mode}.initial", parameters), "microbatches": [], "flushes": []}
        initial_frozen = {name: value.detach().clone() for name, value in parameters.items() if not value.requires_grad}
        initial_unused = {name: value.detach().clone() for name, value in parameters.items()
                          if "boundary_head.pair_scorer." in name or "boundary_head.boundary_proposer." in name}
        initial_snapshot = None
        window = 0
        step = 0

        def prepare(spec):
            batch = base.processor.collate_fn_inference([(s["text"], s["upstream"]) for s in spec["samples"]],
                architecture="boundary", build_targets=True, max_len=None, error_policy="raise",
                max_gold_per_query=8, on_capacity_exceeded="raise")
            if len(batch) != 2 or batch.input_ids.shape[1] > 128 or batch.text_word_mask.shape[1] > 32 or batch.query_marker_mask.shape[1] > 32:
                raise oracle.ContractError("mixed-step input exceeds declared dimensions")
            if not spec["classification_supervised"]:
                for labels, tasks in zip(batch.structure_labels, batch.task_types):
                    for index, task in enumerate(tasks):
                        if task == "classifications":
                            labels[index] = None
            fragments = set(tokenizer.all_special_tokens)
            for sample_tokens in batch.schema_tokens_list:
                for group_tokens in sample_tokens:
                    fragments.update(group_tokens)
            for words in batch.text_tokens:
                fragments.update(words)
            for fragment in sorted(fragments):
                ids = tokenizer.encode(fragment, add_special_tokens=False)
                if fragment in fragment_ids and fragment_ids[fragment] != ids:
                    raise oracle.ContractError("tiny fragment tokenization changed")
                fragment_ids[fragment] = ids
            return batch

        def execute(micro, spec, record):
            nonlocal window, step, initial_snapshot
            batch = prepare(spec)
            observed = {"classifier": [], "encoder_calls": 0, "negative_uniforms": [], "pair_queries": []}
            originals = []
            hooks = []
            warnings = []

            class CaptureWarnings(logging.Handler):
                def emit(self, message):
                    if message.levelno >= logging.WARNING:
                        warnings.append(message.getMessage())

            logger = logging.getLogger("gliner2.models.boundary.model")
            handler = CaptureWarnings(); logger.addHandler(handler)

            def replace(owner, name, replacement):
                originals.append((owner, name, getattr(owner, name)))
                setattr(owner, name, replacement)

            def watch(name, value):
                observed[name] = value
                if hasattr(value, "requires_grad") and value.requires_grad:
                    value.retain_grad()

            def encoder_hook(_module, _input, value):
                observed["encoder_calls"] += 1
                watch("encoder", value.last_hidden_state)

            def head_hook(_module, _input, value):
                observed["head"] = value

            def cls_hook(_module, _input, value):
                observed["classifier"].append(value)
                if value.requires_grad:
                    value.retain_grad()

            hooks.extend([base.encoder.register_forward_hook(encoder_hook),
                          base.boundary_head.register_forward_hook(head_hook),
                          base.classifier.register_forward_hook(cls_hook)])
            for name, module in (("pool_start", base.boundary_head.shared_pool_builder.start_projection),
                                 ("pool_end", base.boundary_head.shared_pool_builder.end_projection)):
                hooks.append(module.register_forward_hook(lambda _m, _i, value, name=name: watch(name, value)))
            hooks.append(base.boundary_head.boundary_encoder.register_forward_hook(
                lambda _m, _i, value: watch("boundary_states", value.states)))
            hooks.append(base.boundary_head.boundary_query_head.register_forward_hook(
                lambda _m, _i, value: observed.update(marginals=value)))
            hooks.append(base.boundary_head.shared_pool_builder.register_forward_hook(
                lambda _m, _i, value: observed.update(pool=value)))
            original_core = base._encode_core

            def encoded(*args, **kwargs):
                value = original_core(*args, **kwargs)
                observed["core"] = value
                return value

            replace(base, "_encode_core", encoded)
            original_finite = model_module._finite_loss_term

            def finite(value):
                if not bool(torch.isfinite(value).all()):
                    raise oracle.ContractError("upstream attempted to suppress a nonfinite loss")
                return original_finite(value)

            replace(model_module, "_finite_loss_term", finite)
            def uniform(*shape, **kwargs):
                dimensions = tuple(shape[0]) if len(shape) == 1 and isinstance(shape[0], (tuple, list, torch.Size)) else tuple(shape)
                if dimensions != tuple(batch.query_marker_mask.shape) or kwargs.keys() - {"device"}:
                    raise oracle.ContractError("unexpected random draw in zero-dropout mixed step")
                seed = int.from_bytes(hashlib.sha256(f"negative/{micro}".encode()).digest()[:4], "little")
                draw = (((torch.arange(dimensions[0] * dimensions[1], dtype=torch.int64) * 17 + seed) % 997).float() + 0.5) / 997
                draw = draw.reshape(dimensions).to(kwargs.get("device", "cpu"))
                observed["negative_uniforms"].append(draw)
                return draw

            replace(torch, "rand", uniform)
            original_hard = model_module.select_hard_negative_candidates

            def hard(*args, **kwargs):
                value = original_hard(*args, **kwargs); watch("hard_negative_mask", value); return value

            replace(model_module, "select_hard_negative_candidates", hard)
            original_pair = model_module.candidate_pair_loss

            def pair_loss(*args, **kwargs):
                observed["pair_queries"].append(kwargs["query_mask"])
                return original_pair(*args, **kwargs)

            replace(model_module, "candidate_pair_loss", pair_loss)
            original_generate = base.relation_pair_generator.generate_batched

            def generate(*args, **kwargs):
                result = original_generate(*args, **kwargs); observed["relations"] = result; return result

            replace(base.relation_pair_generator, "generate_batched", generate)
            hooks.append(base.relation_scorer.register_forward_hook(lambda _m, _i, value: watch("relation_logits", value)))
            original_records = base.record_decoder.forward_groups_dense

            def records(*args, **kwargs):
                result = original_records(*args, **kwargs); observed["records"] = result
                watch("record_objects", result.object_logits); watch("record_assignments", result.assign_logits)
                return result

            replace(base.record_decoder, "forward_groups_dense", records)
            original_filter = record_module.filter_match_indices

            def filtered(*args, **kwargs):
                result = original_filter(*args, **kwargs); observed["record_matches"] = result; return result

            replace(record_module, "filter_match_indices", filtered)
            try:
                result = model(batch, return_candidates=True, gold_injection_prob=1.0, collect_diagnostics=False)
                if warnings or observed["encoder_calls"] != 1 or len(observed["negative_uniforms"]) != 1:
                    raise oracle.ContractError(f"mixed-step supervision/forward event differs: {warnings}")
                required = {"start_loss", "end_loss", "pair_loss", "inside_loss", "soft_iou_loss", "rerank_listwise_loss",
                            "proposal_loss", "consistency_loss", "abstention_loss", "count_loss", "total_loss", "classification_loss"}
                if not required.issubset(result.losses):
                    raise oracle.ContractError("missing boundary/classification loss terms")
                cls_count = sum(len(labels) for sample_labels, tasks in zip(batch.structure_labels, batch.task_types)
                                for labels, task in zip(sample_labels, tasks) if task == "classifications" and labels is not None)
                if sum(value.numel() for value in observed["classifier"]) != cls_count:
                    raise oracle.ContractError("classification supervision was silently skipped")
                candidate = result.candidates
                target = batch.targets
                covered = ((candidate.indices.unsqueeze(2) == target.mention_pairs.unsqueeze(3)).all(-1)
                           & candidate.valid_mask.unsqueeze(2)).any(-1)
                if not bool((covered | ~target.mention_mask).all()):
                    raise oracle.ContractError("a gold mention disappeared from the retained pool")
                matches = observed.get("record_matches")
                match_count = 0 if matches is None else int(matches[0].numel())
                if match_count != spec["expected_record_count"]:
                    raise oracle.ContractError("record matching lost gold supervision")
                relation_positive = 0
                if "relations" in observed:
                    pairs = observed["relations"]
                    coords = torch.stack((pairs.head_start, pairs.head_end, pairs.tail_start, pairs.tail_end), -1)
                    gold, valid = target.edge_targets[:2]
                    selected = gold[pairs.batch_index, pairs.relation_index]
                    mask = valid[pairs.batch_index, pairs.relation_index]
                    relation_labels = ((coords[:, None] == selected).all(-1) & mask).any(-1) & pairs.pair_mask
                    relation_positive = int(relation_labels.sum())
                    watch("relation_labels", relation_labels)
                    for b in range(gold.shape[0]):
                        for r in range(gold.shape[1]):
                            for span in gold[b, r, valid[b, r]]:
                                if not bool(((coords == span).all(-1) & (pairs.batch_index == b) & (pairs.relation_index == r) & pairs.pair_mask).any()):
                                    raise oracle.ContractError("a gold edge disappeared from relation proposals")
                if relation_positive != spec["expected_relation_count"]:
                    raise oracle.ContractError("positive relation supervision differs")
                combined = result.losses["total_loss"] + result.losses["classification_loss"]
                if spec["expected_record_count"]:
                    if not {"record_object_loss", "record_field_loss"}.issubset(result.losses):
                        raise oracle.ContractError("record auxiliary loss was skipped")
                    combined = combined + settings["record_loss_weight"] * (result.losses["record_object_loss"] + result.losses["record_field_loss"])
                if spec["expected_relation_count"]:
                    if "relation_loss" not in result.losses:
                        raise oracle.ContractError("relation auxiliary loss was skipped")
                    combined = combined + result.losses["relation_loss"]
                if not bool(torch.allclose(combined, result.total_loss, rtol=1e-6, atol=1e-6)):
                    raise oracle.ContractError("total objective differs from declared component weights")
                gradients = torch.autograd.grad(result.total_loss, list(trainable.values()), allow_unused=True, retain_graph=True)
                unscaled = dict(zip(trainable, gradients))
                for name, gradient in unscaled.items():
                    if gradient is not None and not bool(torch.isfinite(gradient).all()):
                        raise oracle.ContractError(f"nonfinite trainable gradient: {name}")
                if mode in ("full", "heads"):
                    if not spec["classification_supervised"] and any(unscaled[name] is not None for name in trainable if name.startswith("classifier.")):
                        raise oracle.ContractError("unsupervised classifier acquired a gradient")
                    for prefix in ("boundary_head.pair_scorer.", "boundary_head.boundary_proposer."):
                        if any(unscaled[name] is not None for name in trainable if name.startswith(prefix)):
                            raise oracle.ContractError("unused legacy scorer acquired a gradient")
                    for prefix, positive in (("record_decoder.", spec["expected_record_count"]), ("relation_scorer.", spec["expected_relation_count"])):
                        values = [unscaled[name] for name in trainable if name.startswith(prefix)]
                        if any(value is None for value in values) or bool(any(torch.count_nonzero(value) for value in values)) != bool(positive):
                            raise oracle.ContractError(f"optional head gradient presence differs: {prefix}")
                (result.total_loss / options.gradient_accumulation_steps).backward()
                window += 1
                if record:
                    prefix = f"{mode}.micro{micro}"
                    inputs = {name: getattr(batch, name) for name in ("input_ids", "attention_mask", "text_word_indices", "text_word_mask",
                        "query_marker_indices", "query_marker_mask", "cls_marker_indices", "cls_marker_mask")}
                    tensors = {"encoder": observed["encoder"], "boundary_states": observed["boundary_states"],
                        "pool_start": observed["pool_start"], "pool_end": observed["pool_end"],
                        "inside_prefix": observed["marginals"].inside_prefix,
                        "inside_prefix_mean": observed["marginals"].inside_prefix_mean,
                        "text_states": observed["core"]["text_states"], "query_states": observed["core"]["query_states"],
                        "start_logits": result.start_logits,
                        "end_logits": result.end_logits, "inside_logits": result.inside_logits,
                        "null_logits": result.null_logits, "count_log_rates": result.count_log_rates,
                        "candidate_indices": candidate.indices, "candidate_valid": candidate.valid_mask,
                        "candidate_logits": candidate.pair_logits, "candidate_states": candidate.candidate_states,
                        "proposal_logits": candidate.proposal_logits, "hard_negative_mask": observed["hard_negative_mask"],
                        "negative_uniform": observed["negative_uniforms"][0], "pair_query_mask": observed["pair_queries"][0],
                        "mention_pairs": target.mention_pairs, "mention_mask": target.mention_mask}
                    for key in ("record_objects", "record_assignments", "relation_logits", "relation_labels"):
                        if key in observed:
                            tensors[key] = observed[key]
                    entry = {"id": spec["id"], "microbatch": micro, "optimizer_step_before": step,
                        "inputs": named_tensor(prefix + ".input", inputs),
                        "outputs": named_tensor(prefix + ".output", {key: value for key, value in tensors.items() if value is not None}),
                        "losses": {key: numeric(value) for key, value in result.losses.items()}, "total_loss": numeric(result.total_loss),
                        "gradients": {name: None if value is None else save(prefix + ".gradient." + name, value) for name, value in unscaled.items()},
                        "schema_tokens_list": batch.schema_tokens_list, "text_tokens": batch.text_tokens,
                        "task_types": batch.task_types, "structure_labels": batch.structure_labels,
                        "classification_label_count": cls_count, "record_match_count": match_count,
                        "relation_positive_count": relation_positive, "encoder_forward_calls": observed["encoder_calls"]}
                    entry["targets"] = structure(prefix + ".target", target)
                    entry["pool"] = structure(prefix + ".pool", observed["pool"])
                    entry["routing"] = structure(prefix + ".routing", {name: observed["core"][name]
                        for name in ("ext_specs", "cls_specs", "rel_specs", "text_lengths")})
                    if "records" in observed:
                        entry["records"] = structure(prefix + ".records", observed["records"])
                    entry["classification_logits"] = [save(prefix + f".classification.{index}", value) for index, value in enumerate(observed["classifier"])]
                    if matches is not None:
                        entry["record_matches"] = [save(prefix + f".record_match.{index}", value) for index, value in enumerate(matches)]
                    if "relations" in observed:
                        pairs = observed["relations"]
                        entry["relation_pairs"] = named_tensor(prefix + ".relation_pairs", {name: getattr(pairs, name) for name in
                            ("batch_index", "relation_index", "head_start", "head_end", "tail_start", "tail_end", "pair_mask")})
                    report["microbatches"].append(entry)
                if micro == 0:
                    initial_snapshot = {"model": copy.deepcopy(model.state_dict()), "optimizer": copy.deepcopy(optimizer.state_dict()),
                        "gradients": {name: None if parameter.grad is None else parameter.grad.detach().clone() for name, parameter in parameters.items()}}
                if window == 2 or micro == 2:
                    methods["_renormalize_partial_accumulation"](controller, window)
                    before = {name: None if parameter.grad is None else parameter.grad.detach().clone() for name, parameter in trainable.items()}
                    methods["_optimizer_step"](controller)
                    step += 1
                    if record:
                        prefix = f"{mode}.flush{step}"
                        report["flushes"].append({"optimizer_step": step, "after_microbatch": micro,
                            "microbatches": window, "grad_norm": numeric(controller._last_grad_norm),
                            "gradients_before_clip": {name: None if value is None else save(prefix + ".gradient." + name, value) for name, value in before.items()},
                            "parameters": {name: {"weight": save(prefix + ".weight." + name, parameter),
                                "state_present": parameter in optimizer.state,
                                "step": int(optimizer.state.get(parameter, {}).get("step", torch.tensor(0))),
                                "exp_avg": save(prefix + ".exp_avg." + name, optimizer.state[parameter]["exp_avg"]) if parameter in optimizer.state else None,
                                "exp_avg_sq": save(prefix + ".exp_avg_sq." + name, optimizer.state[parameter]["exp_avg_sq"]) if parameter in optimizer.state else None}
                                for name, parameter in trainable.items()}})
                    window = 0
            finally:
                for owner, name, value in reversed(originals):
                    setattr(owner, name, value)
                for hook in hooks:
                    hook.remove()
                logger.removeHandler(handler)

        for micro, spec_index in enumerate((0, 1, 1)):
            execute(micro, batches[spec_index], True)
        expected_weights = {name: value.detach().clone() for name, value in parameters.items()}
        expected_optimizer = copy.deepcopy(optimizer.state_dict())
        if initial_snapshot is None:
            raise oracle.ContractError("missing mid-window resume snapshot")
        model.load_state_dict(initial_snapshot["model"])
        optimizer.load_state_dict(initial_snapshot["optimizer"])
        for name, parameter in parameters.items():
            gradient = initial_snapshot["gradients"][name]
            parameter.grad = None if gradient is None else gradient.clone()
        window, step = 1, 0
        execute(1, batches[1], False); execute(2, batches[1], False)
        if any(not torch.equal(parameters[name], value) for name, value in expected_weights.items()):
            raise oracle.ContractError("resumed mixed-step parameters differ")
        actual_optimizer = optimizer.state_dict()
        if expected_optimizer["param_groups"] != actual_optimizer["param_groups"] or expected_optimizer["state"].keys() != actual_optimizer["state"].keys():
            raise oracle.ContractError("resumed optimizer groups/state membership differ")
        for key, state in expected_optimizer["state"].items():
            for name, value in state.items():
                actual = actual_optimizer["state"][key][name]
                if not torch.equal(value, actual):
                    raise oracle.ContractError("resumed optimizer tensor differs")
        if any(not torch.equal(parameters[name], value) for name, value in {**initial_frozen, **initial_unused}.items()):
            raise oracle.ContractError("frozen or unused parameter changed")
        report["mid_window_resume_exact"] = True
        report["frozen_and_unused_unchanged"] = True
        reports.append(report)

    output.mkdir(parents=False, exist_ok=False)
    tensors = oracle.save_tensors(output / "tensors.safetensors", all_tensors, torch)
    oracle.write_json(output / "tokenizer.json", tokenizer_data)
    oracle.write_json(output / "tokenizer_config.json", tokenizer_config)
    tokenizer_files = {name: {"sha256": oracle.sha256_file(output / name), "size_bytes": (output / name).stat().st_size}
                       for name in ("tokenizer.json", "tokenizer_config.json")}
    report = dict(format_version=1, scope="synthetic_zero_dropout_mixed_task_training_step",
        qualification=False, source_commit=oracle.UPSTREAM_COMMIT, provenance=provenance,
        generator_sha256=oracle.sha256_file(Path(__file__)), oracle_sha256=oracle.sha256_file(Path(oracle.__file__)),
        source_files={name: oracle.sha256_file(source / name) for name in source_files}, trainer_methods=method_pins,
        batches=batches, sequence=[0, 1, 1], settings=settings,
        optimizer={"accumulation_steps": 2, "max_grad_norm": 0.7, "scheduler": "constant",
                   "gold_injection_probability": 1.0, "consistency_scale": 0.5, "soft_iou_scale": 0.75},
        profiles=reports, tensors=tensors, tokenizer_files=tokenizer_files, tokenizer_fragments=fragment_ids,
        notes=["No pretrained checkpoint or corpus is loaded; every profile constructs the same tiny random DeBERTa baseline.",
               "model.train() executes real record/relation auxiliary losses. Encoder/head/PEFT dropout are all zero.",
               "Complete-schema supervised inference collation removes stochastic schema augmentation; attributes are explicitly represented as hidden entity labels.",
               "Each sample includes an ordered schema_json and original UTF-8 annotations; consumers replace the placeholder schema_fingerprint with the actual compiled schema fingerprint.",
               "The legacy invoice annotation supplies its field mention but is excluded from explicit record-object matching. Four natural/latent/anchorless gold records remain.",
               "Tiny WordLevel fragment IDs and tokenizer files are exact. A fixture-only table tokenizer must reject unknown fragments; no general WordLevel implementation is qualified.",
               "Unsupervised classification is a declared internal-label adaptation: labels become None after complete query routing is prepared.",
               "Source proposal and relation selection read current live logits; negative-query draws are explicit and saved.",
               "Every intended mention/record/edge is checked for retained supervision; upstream skipped/nonfinite loss events fail capture.",
               "The exact pinned trainer optimizer construction, partial-window renormalization and update methods run on actual Torch parameters.",
               "An in-memory mid-window snapshot reproduces final weights and optimizer states byte-exactly; durable publication and native equality are separate tests.",
               "None gradients remain distinct from explicit optional-head zero gradients; frozen/unused weights are unchanged."])
    oracle.write_json(output / "capture.json", report)
    return {"path": str(output), "profiles": modes, "tensor_bytes": tensors["size_bytes"],
            "capture_bytes": (output / "capture.json").stat().st_size,
            "capture_sha256": oracle.sha256_file(output / "capture.json"), "qualification": False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--modes", nargs="+", choices=("full", "heads", "lora", "dora"), default=["full", "heads", "lora", "dora"])
    args = parser.parse_args()
    if len(args.modes) != len(set(args.modes)) or __import__("sys").flags.optimize:
        raise oracle.ContractError("distinct modes and enabled upstream assertions are required")
    print(capture(args.upstream, args.output_dir, args.modes))


if __name__ == "__main__":
    main()
