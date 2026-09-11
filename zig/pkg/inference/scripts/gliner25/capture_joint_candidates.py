#!/usr/bin/env python3
"""Capture bounded sparse JointIE candidate admission without an encoder run."""
from __future__ import annotations
import argparse
from pathlib import Path
from oracle import prepare_runtime, write_json


def capture(source: Path, output: Path) -> None:
    provenance, _ = prepare_runtime(source)
    from gliner2.joint_ie.candidate_scores import MentionScore, ScoredRelationEdge, CandidateScoreSet, candidate_score_set_to_problem
    from gliner2.joint_ie.candidates import sigmoid
    entities = ["person", "organization"]
    relations = ["works_for", "knows"]
    text = "a b c d e f"
    mentions = [MentionScore(q, name, i, i + 1, logit, sigmoid(logit), 0.6 if q == 0 else 0.4)
                for q, name in enumerate(entities)
                for i, logit in enumerate(([3., -5., 2., 1., -2., 0.], [2., 1., -4., 3., 0., -1.])[q])]
    edges = [ScoredRelationEdge("works_for", ("person", 1, 2), ("organization", 2, 3), 4., sigmoid(4.), 0.7),
             ScoredRelationEdge("works_for", ("person", 1, 2), ("organization", 2, 3), 3., sigmoid(3.), 0.7),
             ScoredRelationEdge("works_for", ("person", 4, 5), ("organization", 5, 6), 2., sigmoid(2.), 0.7),
             ScoredRelationEdge("knows", ("person", 0, 1), ("person", 2, 3), -1., sigmoid(-1.), 0.5)]
    cases = []
    configurations = [
        {}, {"max_mentions_per_type": 1}, {"max_mentions_per_type": 1, "rescue_relation_endpoints": False},
        {"max_edges_per_type": 1}, {"mention_threshold": 0.8}, {"edge_candidate_threshold": 0.9},
        {"max_mentions_by_type": {"person": 1, "organization": 3}},
        {"entity_weight": 0.25, "relation_weight": 2.},
        {"max_mentions_per_type": 1, "max_edges_per_type": 1, "edge_candidate_threshold": 0.999},
    ]
    for index, overrides in enumerate(configurations):
        options = dict(mention_threshold=0.05, max_mentions_per_type=32, max_edges_per_type=128,
                       rescue_relation_endpoints=True, edge_candidate_threshold=0.05,
                       entity_weight=1., relation_weight=1.)
        options.update(overrides)
        problem = candidate_score_set_to_problem(CandidateScoreSet(text, tuple(mentions), edges=tuple(edges)), **options)
        node_by_key = {node.key: i for i, node in enumerate(problem.nodes)}
        key = lambda k: {"entity_type": entities.index(k[0]), "span": {"start": k[1], "end": k[2]}}
        cases.append({
            "id": f"admission_{index}", "text": text, "entity_names": entities, "relation_names": relations,
            "entity_thresholds": [0.6, 0.4], "relation_thresholds": [0.7, 0.5],
            "mentions": [{"key": key(m.key), "logit": m.logit, "probability": m.probability} for m in mentions],
            "edges": [{"relation_type": relations.index(e.relation_type), "head": key(e.head), "tail": key(e.tail), "logit": e.logit, "probability": e.probability} for e in edges],
            "options": options,
            "expected": {
                "nodes": [{"entity_type": entities.index(n.entity_type), "token_span": {"start": n.start, "end": n.end},
                           "start": 2 * n.start, "end": 2 * n.end - 1, "utility": n.score, "probability": n.probability,
                           "rescued": n.source.value == "relation_rescue"} for n in problem.nodes],
                "edges": [{"relation_type": relations.index(e.relation_type), "head": node_by_key[e.head], "tail": node_by_key[e.tail],
                           "utility": e.score, "probability": e.head_probability, "slot": e.slot,
                           "hypothesis": relations.index(e.hypothesis)} for e in problem.edges],
            },
        })
    write_json(output, {"format_version": 1, "provenance": provenance, "cases": cases})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    capture(args.upstream, args.output)
