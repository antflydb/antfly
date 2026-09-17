"""Opt-in loss-boundary diagnostics; never used in paired throughput steps."""
from dataclasses import replace
from pathlib import Path

import numpy as np

import benchmark_cpu as common


class ModuleTrace:
    """Replay retained Linear/LayerNorm inputs without touching the training graph."""

    def __init__(self, model, torch, metadata):
        self.model, self.torch, self.metadata = model, torch, metadata
        self.hooks, self.results = [], []
        self.replaying = False
        self.data = None

    def __enter__(self):
        metadata = self.metadata
        path = Path(metadata["path"])
        size = metadata["size_bytes"]
        if not 0 < size <= 128 * 1024**2 or size % 4 or path.stat().st_size != size:
            raise ValueError("invalid native module trace size")
        records = {}
        ambiguous = set()
        for record in metadata["tensors"]:
            offset, elements = record["offset"], record["elements"]
            if offset < 0 or offset % 4 or elements <= 0 or offset + elements * 4 > size:
                raise ValueError("invalid native module trace extent")
            key = record["name"], record["kind"]
            if key[1] not in ("input", "output"):
                raise ValueError("invalid native module trace record kind")
            if key in records:
                ambiguous.add(key[0])
            records[key] = slice(offset // 4, offset // 4 + elements)
        self.data = np.memmap(path, mode="r", dtype="<f4")
        try:
            for name, module in self.model.named_modules():
                if name in ambiguous:
                    self.results.append({"name": name, "skipped": "multiple native invocations require explicit alignment"})
                    continue
                if (name, "output") not in records and (name, "input") not in records:
                    continue
                if not isinstance(module, (self.torch.nn.Linear, self.torch.nn.LayerNorm)):
                    self.results.append({"name": name, "skipped": "unsupported replay module", "type": type(module).__name__})
                    continue
                self.hooks.append(module.register_forward_hook(self.callback(name, records)))
        except BaseException:
            self.__exit__()
            raise
        return self

    @staticmethod
    def compare(native, python):
        reference = python.detach().float().cpu().numpy().reshape(-1).astype(np.float64)
        actual = native.astype(np.float64)
        if actual.shape != reference.shape:
            return {"shape_mismatch": [int(actual.size), int(reference.size)]}
        if not np.isfinite(actual).all() or not np.isfinite(reference).all():
            raise ValueError("non-finite module trace")
        delta = actual - reference
        index = int(np.argmax(np.abs(delta)))
        return {"elements": int(actual.size), "max_absolute_error": float(abs(delta[index])),
                "l2_error": float(np.linalg.norm(delta)), "worst_index": index,
                "native": float(actual[index]), "python": float(reference[index])}

    def callback(self, name, records):
        def capture(module, inputs, output):
            if self.replaying:
                return
            row = {"name": name, "type": type(module).__name__,
                   "comparison_scope": "all_storage_including_inactive_rows"}
            if (name, "output") in records:
                row["output"] = self.compare(self.data[records[name, "output"]], output)
            if (name, "input") in records:
                raw = self.data[records[name, "input"]]
                row["input"] = self.compare(raw, inputs[0])
                if (name, "output") in records and raw.size == inputs[0].numel():
                    self.replaying = True
                    try:
                        with self.torch.no_grad():
                            value = self.torch.from_numpy(np.array(raw, copy=True)).to(inputs[0].device).reshape(inputs[0].shape)
                            replayed = module(value)
                            row["same_input_output"] = self.compare(self.data[records[name, "output"]], replayed)
                            if isinstance(module, self.torch.nn.Linear):
                                unfused = self.torch.nn.functional.linear(value, module.weight, None)
                                if module.bias is not None:
                                    unfused = unfused + module.bias
                                row["same_input_unfused_output"] = self.compare(self.data[records[name, "output"]], unfused)
                    finally:
                        self.replaying = False
            self.results.append(row)
        return capture

    def __exit__(self, *unused):
        for hook in self.hooks:
            hook.remove()
        self.hooks.clear()
        if self.data is not None:
            self.data._mmap.close()
            self.data = None


class BoundaryTrace:
    def __init__(self, model, torch, native_path=None):
        self.head, self.torch = model.boundary_head, torch
        self.model, self.modules = model, None
        self.native = None
        self.result = None
        if native_path is not None:
            with native_path.open("rb") as source:
                payload = source.read(2 * 1024**2 + 1)
            if len(payload) > 2 * 1024**2:
                raise ValueError("native loss trace exceeds limit")
            self.native = common.strict_json(payload)
        if self.head.settings.negative_query_ratio != 0:
            raise ValueError("loss trace requires disabled query subsampling")

    def __enter__(self):
        if self.native is not None and self.native.get("_modules"):
            self.modules = ModuleTrace(self.model, self.torch, self.native["_modules"]).__enter__()
        self.previous = self.head.__dict__.get("_compute_losses")
        self.original = self.head._compute_losses
        self.head._compute_losses = self.capture
        return self

    def __exit__(self, *unused):
        if self.previous is None:
            del self.head._compute_losses
        else:
            self.head._compute_losses = self.previous
        if self.modules is not None:
            self.modules.__exit__(*unused)
            if self.result is not None:
                self.result["modules"] = self.modules.results

    def values(self, tensor):
        if tensor is None:
            return None
        if tensor.numel() > 131072:
            raise ValueError("loss trace tensor exceeds limit")
        return tensor.detach().cpu().flatten().tolist()

    def capture(self, *args, **kwargs):
        if self.result is not None:
            raise ValueError("loss trace observed more than one boundary call")
        if kwargs.get("pooled") is None:
            raise ValueError("loss trace requires a shared candidate pool")
        torch = self.torch
        marginal, proposals, _, targets, queries, boundaries, text, nulls, counts = args
        pooled = kwargs["pooled"]
        leaves = dict(starts=marginal.start_logits, ends=marginal.end_logits,
                      inside=marginal.inside_logits,
                      pairs=kwargs["pooled_pair_logits"].transpose(1, 2),
                      proposals=pooled.proposal_logits, nulls=nulls, counts=counts)
        if sum(x.numel() for x in leaves.values() if x is not None) > 131072:
            raise ValueError("loss trace element budget exceeded")
        inputs = {name: self.values(value) for name, value in leaves.items()}
        inputs.update(pool_mask=self.values(pooled.mask), query_mask=self.values(queries),
                      spans=pooled.indices.detach().cpu().tolist(),
                      gold_spans=targets.mention_pairs.detach().cpu().tolist(),
                      gold_mask=self.values(targets.mention_mask))

        def differentiate(native=None):
            independent = {}
            for name, value in leaves.items():
                if value is None:
                    independent[name] = None
                elif native is None:
                    independent[name] = value.detach().clone().requires_grad_(True)
                else:
                    raw = native[name]
                    if not isinstance(raw, list) or len(raw) != value.numel():
                        raise ValueError("native loss trace shape differs: " + name)
                    item = torch.tensor(raw, dtype=value.dtype, device=value.device).reshape(value.shape)
                    if not torch.isfinite(item).all():
                        raise ValueError("native loss trace contains non-finite values")
                    independent[name] = item.requires_grad_(True)
            copied_args = list(args)
            copied_args[0] = replace(marginal, start_logits=independent["starts"],
                                     end_logits=independent["ends"], inside_logits=independent["inside"])
            copied_args[2] = independent["pairs"]
            copied_args[7], copied_args[8] = independent["nulls"], independent["counts"]
            copied_kwargs = dict(kwargs, pooled=replace(pooled, proposal_logits=independent["proposals"]),
                                 pooled_pair_logits=independent["pairs"].transpose(1, 2))
            losses = self.original(*copied_args, **copied_kwargs)
            names = [name for name, value in independent.items() if value is not None]
            gradients = torch.autograd.grad(losses["total_loss"], [independent[name] for name in names], allow_unused=True, retain_graph=True)
            components = {}
            if independent["pairs"].numel() <= 32768:
                weights = {
                    "pair_loss": self.head.loss_weights.get("pair", 1.0),
                    "soft_iou_loss": self.head.settings.soft_iou_aux_weight * self.head._soft_iou_scale,
                    "rerank_listwise_loss": self.head.settings.rerank_listwise_weight,
                    "consistency_loss": self.head.settings.consistency_loss_weight * self.head._consistency_scale,
                }
                for name, weight in weights.items():
                    if losses[name].requires_grad:
                        component, = torch.autograd.grad(losses[name] * weight, independent["pairs"], allow_unused=True, retain_graph=True)
                        components[name] = self.values(torch.zeros_like(independent["pairs"]) if component is None else component)
            return {"pair_gradient_components": components, "terms": {name: float(value.detach()) for name, value in losses.items()},
                    "gradients": {name: self.values(torch.zeros_like(independent[name]) if gradient is None else gradient)
                                  for name, gradient in zip(names, gradients)}}

        self.result = {"inputs": inputs, "python": differentiate()}
        if self.native is not None:
            native = self.native
            geometry_errors = []
            for name in ("pool_mask", "query_mask"):
                if inputs[name] != native[name]:
                    geometry_errors.append(name)
            native_spans = [(span["start"], span["end"]) for span in native["spans"]]
            python_spans = [tuple(span) for batch in inputs["spans"] for span in batch]
            if len(native_spans) != len(python_spans) or any(a != b for a, b, valid in zip(native_spans, python_spans, inputs["pool_mask"]) if valid):
                geometry_errors.append("spans")
            if inputs["gold_mask"] != native["gold"]["valid"]:
                geometry_errors.append("gold_mask")
            native_gold = [(span["start"], span["end"]) for span in native["gold"]["spans"]]
            python_gold = [tuple(span) for batch in inputs["gold_spans"] for query in batch for span in query]
            if len(native_gold) != len(python_gold) or any(a != b for a, b, valid in zip(native_gold, python_gold, inputs["gold_mask"]) if valid):
                geometry_errors.append("gold_spans")
            self.result["geometry_errors"] = geometry_errors
            if not geometry_errors:
                self.result["native_logits_python_backward"] = differentiate(native)
        # The actual training graph is evaluated only once, without replacing
        # its tensors, gradients, optimizer state or loss arithmetic.
        return self.original(*args, **kwargs)
