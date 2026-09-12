#!/usr/bin/env python3
"""Capture bounded assignment/record decoder parity from the pinned oracle.

Run in the locked oracle environment with --source pointing at the immutable
upstream checkout. Does not load a pretrained model or perform network access.
"""
from __future__ import annotations
import argparse
from pathlib import Path
from oracle import prepare_runtime, write_json


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    provenance, torch = prepare_runtime(args.source)
    import gliner2.models.boundary  # initialize before matching's circular import
    from gliner2.training.matching import linear_sum_assignment
    from gliner2.models.boundary.records import RecordGroupOutput, decode_group
    from gliner2.processing.records import RecordSpec, RecordFieldSpec, FieldCardinality
    import scipy
    assignments = []
    for rows, columns in ((2, 3), (3, 4), (4, 3), (3, 3)):
        # Costs repeat every seven seeds modulo 7. Capture each distinct
        # matrix once, retaining the source solver's exact tie decisions.
        for seed in range(7):
            values = [float((seed * 37 + i * i * 13 + i * seed * 7) % 7 - 3)
                      for i in range(rows * columns)]
            r, c = linear_sum_assignment(torch.tensor(values, dtype=torch.float64).reshape(rows, columns))
            assignments.append({"rows": rows, "columns": columns, "costs": values,
                                "pairs": [{"row": a, "column": b} for a, b in zip(r.tolist(), c.tolist())]})
    write_json(args.output / "assignment.json", {
        "format_version": 1, "provenance": provenance, "scipy_version": scipy.__version__,
        "profile": "scipy_perturbed", "cases": assignments,
    })
    records = []
    for mode in ("natural", "latent", "anchorless"):
        for seed in range(6):
            torch.manual_seed(314 + seed)
            ni = 3
            counts = [3, 0 if seed == 5 else 3, 2]
            fields, spans, logits = [], [], []
            for f, count in enumerate(counts):
                cardinality = ([FieldCardinality.REQUIRED_ONE, FieldCardinality.OPTIONAL_ONE,
                                FieldCardinality.ZERO_OR_MORE][f])
                fields.append(RecordFieldSpec(10 + f, f"field{f}", f, cardinality,
                                              is_anchor=(mode == "natural" and f == 0),
                                              exclusive=(seed % 2 == 0)))
                spans.append(torch.tensor([[i * 3 + f, i * 3 + f + 2] for i in range(count)],
                                          dtype=torch.long).reshape(count, 2))
                logits.append(torch.randn(ni, count + 1, dtype=torch.float32) * 2)
            objects = torch.tensor([4., 3., 2.])
            instance_spans = [(6, 8), (0, 2), (3, 5)] if mode == "natural" else [None] * ni
            group = RecordGroupOutput(
                spec=RecordSpec(0, "record", "structures", mode, tuple(fields),
                                anchor_query_id=10 if mode == "natural" else None),
                object_logits=objects, assign_logits=logits,
                field_query_ids=[f.query_id for f in fields], field_specs=fields,
                field_spans=spans, field_cand_mask=[torch.ones(n, dtype=torch.bool) for n in counts],
                field_cand_logits=[torch.zeros(n) for n in counts],
                instance_seed=[(0, i) for i in range(ni)] if mode != "anchorless" else [None] * ni,
                instance_spans=instance_spans,
            )
            decoded = decode_group(group)
            records.append({
                "id": f"{mode}_{seed}",
                "group": {
                    "mode": mode, "anchor_field": 0 if mode == "natural" else None,
                    "object_logits": objects.tolist(),
                    "instance_spans": [None if s is None else {"start": s[0], "end": s[1]} for s in instance_spans],
                    "fields": [{"query_id": f.query_id, "scalar": f.is_scalar,
                                "allows_absent": f.allows_absent, "exclusive": f.exclusive,
                                "spans": [{"start": s, "end": e} for s, e in spans[i].tolist()],
                                "assignment_logits": logits[i].flatten().tolist()} for i, f in enumerate(fields)],
                },
                "expected": [{"probability": r.score,
                              "anchor": None if r.anchor_span is None else {"start": r.anchor_span[0], "end": r.anchor_span[1]},
                              "fields": [{"query_id": f.query_id,
                                          "values": [{"span": {"start": s, "end": e}, "probability": p}
                                                     for (s, e), p in zip(r.fields.get(f.query_id, []), r.field_scores.get(f.query_id, []))]}
                                         for f in fields]} for r in decoded],
            })
    write_json(args.output / "records.json", {"format_version": 1, "provenance": provenance, "cases": records})
    from gliner2.models.base import QueryLayout
    from gliner2.models.outputs import CandidateTensorBatch
    from gliner2.models.boundary.relations import RelationProposalSettings, RelationTypeSpec, TypedRelationPairGenerator
    relation_cases = []
    shared = torch.tensor([[[4, 5], [0, 1], [8, 9], [0, 0]], [[3, 5], [0, 2], [8, 9], [0, 0]]])
    valid = torch.tensor([[True, True, True, False], [True, True, False, False]])
    qm = torch.tensor([[True, True, True], [True, False, True]])
    logits = torch.tensor([[[0., 0., 1., 99.], [2., -1., 0., 99.], [1., 1., -2., 99.]],
                           [[1., 2., 99., 99.], [99., 99., 99., 99.], [1., -1., 99., 99.]]])
    candidates = CandidateTensorBatch(indices=shared[:, None].expand(-1, 3, -1, -1),
                                     proposal_logits=None, pair_logits=logits,
                                     valid_mask=valid[:, None].expand(-1, 3, -1), query_mask=qm)
    for threshold in (0., 0.55, 1.):
        settings = RelationProposalSettings(3, 2, 4, threshold)
        schemas = [RelationTypeSpec("one", (0, 2), (1, 2)), RelationTypeSpec("two", (2,), (0,), True)]
        pairs = TypedRelationPairGenerator(settings).generate(candidates, [QueryLayout(queries=())] * 2, schemas)
        relation_cases.append({
            "id": f"ragged_{threshold}",
            "input": {"batch": 2, "queries": 3, "capacity": 4,
                      "spans": [{"start": s, "end": e} for s, e in shared.reshape(-1, 2).tolist()],
                      "valid": valid.flatten().tolist(), "query_mask": qm.flatten().tolist(), "logits": logits.flatten().tolist()},
            "routes": [{"batch_index": b, "relation_index": r, "head_queries": list(spec.head_query_ids),
                        "tail_queries": list(spec.tail_query_ids), "allow_self": spec.allow_self}
                       for b in range(2) for r, spec in enumerate(schemas)],
            "options": {"heads_per_relation": 3, "tails_per_relation": 2, "pair_cap": 4, "argument_threshold": threshold},
            "expected": [{"batch_index": int(pairs.batch_index[i]), "relation_index": int(pairs.relation_index[i]),
                          "head_query": int(pairs.head_keys[i][0]), "tail_query": int(pairs.tail_keys[i][0]),
                          "head_span": {"start": int(pairs.head_start[i]), "end": int(pairs.head_end[i])},
                          "tail_span": {"start": int(pairs.tail_start[i]), "end": int(pairs.tail_end[i])},
                          "head_probability": float(pairs.head_prob[i]), "tail_probability": float(pairs.tail_prob[i])}
                         for i in range(len(pairs))],
        })
    write_json(args.output / "relations.json", {"format_version": 1, "provenance": provenance, "cases": relation_cases})
    from gliner2.models.boundary.engine import BoundaryExtractor
    dedup_cases = []
    fixed = [
        [(('Alice', 0, 5), ('Acme', 20, 24), .8),
         (('Alice Smith', 0, 11), ('Acme Corp', 20, 29), .7),
         (('Alice', 0, 5), ('Acme', 20, 24), .9)],
        [(('Straße', 0, 6), ('Acme', 100, 104), .99),
         (('STRASSE', 90, 97), ('ACME', 100, 104), .8)],
        [(('ALICE\u00a0Smith', 0, 11), ('ACME', 20, 24), .8),
         (('alice smith', 100, 111), ('acme', 120, 124), .9)],
        [(('Alice', 0, 5), ('Acme', 20, 24), .9),
         (('Alice Smith', 100, 111), ('Acme', 120, 124), .8),
         (('Alice Alice Smith', 200, 217), ('Acme Corp', 220, 229), .7)],
        [(('İ Σς K ﬃ', 0, 8), ('B', 10, 11), .7),
         (('i\u0307 σσ k ffi', 100, 111), ('b', 113, 114), .9)],
        [], [(('X', 0, 1), ('Y', 2, 3), .5)],
    ]
    import random
    rng = random.Random(1763)
    mentions = [('Alice', 0, 5), ('Alice Smith', 0, 11), ('ALICE', 20, 25),
                ('Bob', 30, 33), ('Bob Jones', 30, 39), ('BOB', 50, 53),
                ('Acme', 60, 64), ('Acme Corp', 60, 69), ('acme', 80, 84)]
    for _ in range(32):
        fixed.append([(rng.choice(mentions), rng.choice(mentions), rng.choice([.5, .7, .9]))
                      for _ in range(rng.randrange(1, 20))])
    for i, raw in enumerate(fixed):
        edges = [dict(head=h, tail=t, score=p, source_index=j) for j, (h, t, p) in enumerate(raw)]
        def encode(edge):
            return dict(head=dict(text=edge['head'][0], start=edge['head'][1], end=edge['head'][2]),
                        tail=dict(text=edge['tail'][0], start=edge['tail'][1], end=edge['tail'][2]),
                        probability=edge['score'], source_index=edge['source_index'])
        dedup_cases.append(dict(id=f'dedup_{i}', edges=[encode(e) for e in edges],
                                expected=[encode(e) for e in BoundaryExtractor._deduplicate_relation_edges(edges)]))
    write_json(args.output / "relation_dedup.json", {"format_version": 1, "provenance": provenance, "cases": dedup_cases})
    print(f"Captured {len(assignments)} assignments, {len(records)} record decodes, {len(relation_cases)} relation proposals, {len(dedup_cases)} relation dedup cases")


if __name__ == "__main__":
    main()
