#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import math

import pytest
import torch
from qwen_hybrid_eval import compose_memory_rows, record_gate_decision
from qwen_span_copy import (
    MULTI_WINDOW_ROWS,
    WINDOW_POLICIES,
    CacheReader,
    SpanHead,
    batch_targets,
    best_span,
    best_threshold,
    doc_bounds,
    document_best_spans,
    evidence_window,
    gold_spans,
    marginal_nll,
    multi_windows,
    pad_batch,
    parse_args,
    parse_quotas,
    parse_read_cascade,
    parse_sources,
    policy_rows,
    quota_sample,
    read_budget_documents,
    roc_auc,
    scan_windows,
    gate_routes,
    gate_routes_table,
    seed_mean_scores,
    seeds,
    span_cascade_gate_offset,
    span_cascade_widens,
    span_gate_decision,
    span_read_cascade_config,
    span_read_routes_table,
    span_read_set,
    span_token_f1,
    token_occurrences,
    window_ids,
)


def test_doc_bounds_and_occurrences():
    assert doc_bounds([3, 2]) == [(0, 3), (3, 5)]
    with pytest.raises(ValueError):
        doc_bounds([3, 0])
    assert token_occurrences([1, 2, 1, 2, 1], [1, 2]) == [0, 2]
    assert token_occurrences([1, 2], []) == []
    # A match straddling the document boundary is not a gold span.
    ids = [5, 7, 8, 7, 8]
    assert gold_spans(ids, [7, 8], doc_bounds([2, 3])) == [(3, 4)]


def never(_):
    return False


def test_evidence_window_is_left_biased_and_stays_in_the_document():
    bounds = doc_bounds([40, 40])
    ids = list(range(80))
    lo, hi = evidence_window(ids, 20, 21, bounds, 12, 0.65, never)
    assert lo <= 20 and hi >= 22 and hi - lo == 12
    assert 20 - lo > hi - 22  # more context before the span than after
    # Near the document start the unused left budget moves to the right.
    lo, hi = evidence_window(ids, 41, 41, bounds, 12, 0.65, never)
    assert lo == 40 and hi == 52
    # Never crosses into the next document.
    lo, hi = evidence_window(ids, 38, 39, bounds, 12, 0.65, never)
    assert hi == 40 and hi - lo == 12
    with pytest.raises(ValueError):
        evidence_window(ids, 39, 40, bounds, 12, 0.65, never)
    # A span longer than the window keeps its first tokens.
    assert evidence_window(ids, 2, 30, bounds, 8, 0.5, never) == (2, 10)


def test_evidence_window_trims_partial_words():
    ids = list(range(30))
    continues = {7, 8, 25}.__contains__  # tokens 7, 8 and 25 continue a word
    lo, hi = evidence_window(ids, 15, 15, doc_bounds([30]), 18, 0.5, continues)
    assert lo == 9  # starting at 7 or 8 would begin mid-word
    assert hi == 24  # ending before 25 would split the word 24-25
    assert window_ids(ids, 3, 6, 5, 99) == [3, 4, 5, 99, 99]


def test_best_span_respects_documents_and_length():
    bounds = doc_bounds([3, 3])
    start = torch.tensor([0.0, 0, 5, 0, 0, 0, 0])
    end = torch.tensor([0.0, 0, 0, 0, 6, 0, 0])
    s, e, score, null = best_span(start, end, bounds, 10)
    # (1, 3) would cross documents; the best in-document span differs.
    assert bound_check(bounds, s, e)
    assert null == 0.0 and score in (5.0, 6.0)
    start = torch.tensor([3.0, 0, 4, 0, 0])
    end = torch.tensor([3.0, 0, 0, 0, 4])
    s, e, score, null = best_span(start, end, doc_bounds([4]), 2)
    assert e - s + 1 <= 2 and null == 6.0


def bound_check(bounds, s, e):
    return any(lo <= s <= e < hi for lo, hi in bounds)


def test_marginal_nll_and_targets():
    logits = torch.tensor([[0.0, 2.0, 2.0, -1.0]])
    valid = torch.tensor([[True, True, True, False]])
    both = marginal_nll(logits, [[1, 2]], valid)
    one = marginal_nll(logits, [[1]], valid)
    assert both < one
    expected = -math.log(2 * math.exp(2) / (1 + 2 * math.exp(2)))
    assert both.item() == pytest.approx(expected, rel=1e-5)
    starts, ends = batch_targets([{"gold": [[2, 4], [7, 9]]}, {"gold": []}])
    assert starts == [[3, 8], [0]] and ends == [[5, 10], [0]]


def test_scores_and_thresholds():
    assert span_token_f1((2, 5), (2, 5)) == 1.0
    assert span_token_f1((None, None), (2, 5)) == 0.0
    assert span_token_f1((0, 1), (4, 5)) == 0.0
    assert 0 < span_token_f1((2, 3), (2, 5)) < 1
    assert roc_auc([3, 4], [1, 2]) == 1.0
    assert roc_auc([1], [1]) == 0.5
    assert roc_auc([], [1]) is None
    threshold, accuracy = best_threshold([3.0, 4.0], [1.0, 2.0])
    assert 2.0 < threshold < 3.0 and accuracy == 1.0


def test_quotas():
    rows = {"squad_v2": [{"n": i} for i in range(10)], "drop": [{"n": i} for i in range(3)]}
    chosen = quota_sample(rows, {"squad_v2": 4, "drop": 5}, 1)
    assert len(chosen) == 7
    assert quota_sample(rows, {"squad_v2": 4, "drop": 5}, 1) == chosen
    assert parse_quotas("squad_v2=4,depot=2") == {"squad_v2": 4, "depot": 2}
    with pytest.raises(ValueError):
        parse_quotas("wikipedia=3")


def test_span_head_shapes_and_masking():
    torch.manual_seed(0)
    head = SpanHead(width=16, hidden=32, layers=1, heads=4, dropout=0.0, max_positions=64)
    rows = [torch.randn(5, 16), torch.randn(3, 16)]
    features, valid, segments = pad_batch(rows, [doc_bounds([3, 2]), doc_bounds([3])])
    assert segments[0].tolist() == [0, 0, 0, 1, 1]
    start, end, mask = head(features, valid, segments)
    assert start.shape == (2, 6) and end.shape == (2, 6)
    assert mask[1].tolist() == [True, True, True, True, False, False]
    assert torch.isinf(start[1, 4:]).all() and torch.isfinite(start[1, :4]).all()
    loss = marginal_nll(start, [[2], [0]], mask) + marginal_nll(end, [[3], [0]], mask)
    loss.backward()
    assert head.null.grad is not None


def test_hybrid_accepts_window_records_and_always_gate():
    soft = torch.zeros(16, 4)
    assert compose_memory_rows(soft, torch.ones(48, 4)).shape == (64, 4)
    with pytest.raises(ValueError):
        compose_memory_rows(soft, torch.ones(0, 4))
    assert record_gate_decision("always", [], [0.0], [0.0]) is True


def test_cli():
    args = parse_args(
        ["train", "--cache", "c", "--heldout-cache", "h", "--parent", "p", "--output", "o"]
    )
    assert args.window == 32 and args.left_fraction == 0.65
    with pytest.raises(SystemExit):
        parse_args(
            ["cache", "--assets", "a", "--candidate", "c", "--examples", "e",
             "--depot-data", "d", "--output", "o", "--split", "train", "--quotas", "x=1"]
        )


def test_span_head_accepts_more_documents_and_rows_than_its_tables():
    torch.manual_seed(0)
    head = SpanHead(width=8, hidden=16, layers=1, heads=2, dropout=0.0,
                    max_positions=6, max_documents=2)
    bounds = doc_bounds([3, 3, 3])
    features, valid, segments = pad_batch([torch.randn(9, 8)], [bounds])
    assert segments.max() == 2
    start, end, mask = head(features, valid, segments)
    assert start.shape == (1, 10) and torch.isfinite(start).all()


def test_scan_windows_cover_each_document_on_word_boundaries():
    bounds = doc_bounds([20, 6])
    ids = list(range(26))
    windows = scan_windows(ids, bounds, 8, 4, never)
    assert windows[0] == (0, 8) and (12, 20) in windows  # ends at the document end
    assert all(hi - lo <= 8 for lo, hi in windows)
    assert all(any(a <= lo and hi <= b for a, b in bounds) for lo, hi in windows)
    assert (20, 26) in windows and len(windows) == len(set(windows))
    # Starts move forward and ends move back off word continuations.
    continues = {4, 12}.__contains__
    trimmed = scan_windows(ids, doc_bounds([20]), 8, 4, continues)
    assert trimmed == [(0, 8), (5, 11), (8, 16), (13, 20)]
    # The lead window is the first window of the top-1 document.
    assert scan_windows(ids, bounds[:1], 8, 8, never)[0] == (0, 8)
    with pytest.raises(ValueError):
        scan_windows(ids, bounds, 8, 0, never)


def test_leave_one_out_sources_and_cache_filter(tmp_path):
    assert parse_sources("triviaqa, narrativeqa") == {"triviaqa", "narrativeqa"}
    assert parse_sources("") == set()
    with pytest.raises(ValueError):
        parse_sources("wikipedia")
    rows = [{"source": s, "shard": "a", "offset": i} for i, s in enumerate(["triviaqa", "drop", "depot"])]
    (tmp_path / "index.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    reader = CacheReader(tmp_path, {"triviaqa"})
    assert [m["source"] for m in reader.meta] == ["drop", "depot"]
    assert len(reader.by_shard["a"]) == 2
    args = parse_args(["train", "--cache", "c", "--heldout-cache", "h", "--parent", "p",
                       "--output", "o", "--exclude-sources", "triviaqa"])
    assert args.exclude_sources == "triviaqa"
    assert args.max_documents == 4
    wide = parse_args(["train", "--cache", "c", "--heldout-cache", "h", "--parent", "p",
                       "--output", "o", "--max-documents", "64", "--window", "64"])
    assert wide.max_documents == 64 and wide.window == 64
    with pytest.raises(SystemExit):
        parse_args(["train", "--cache", "c", "--heldout-cache", "h", "--parent", "p",
                    "--output", "o", "--exclude-sources", "nope"])


def test_seed_mean_scores_and_seeds_command(tmp_path):
    assert seed_mean_scores([{"a": 1.0, "b": 0.0}, {"a": 0.0, "c": 1.0}]) == {"a": 0.5}
    assert seed_mean_scores([]) == {}
    names = ["a", "b", "c", "d"]
    per_row = {"soft": {"budget": "full", "scores": dict.fromkeys(names, 0.0)}}
    for seed, right in enumerate(([1.0, 1.0, 0.0, 1.0], [1.0, 0.0, 0.0, 1.0])):
        per_row[f"s{seed}"] = {"budget": "full", "scores": dict(zip(names, right))}
    path = tmp_path / "per-row.json"
    path.write_text(json.dumps(per_row))
    result = seeds(parse_args(["seeds", "--per-row", str(path), "--right", "s0,s1",
                               "--resamples", "200"]))
    assert [s["mean_delta"] for s in result["per_seed"]] == [75.0, 50.0]
    assert result["seed_mean"]["mean_delta"] == 62.5 and result["seed_sd"] > 0
    with pytest.raises(ValueError):
        seeds(parse_args(["seeds", "--per-row", str(path), "--right", "s9"]))


def test_policy_rows_pick_window_above_threshold():
    soft = {"a": {"name": "a", "p": "s"}, "b": {"name": "b", "p": "s"}, "c": {"name": "c", "p": "s"}}
    window = {"a": {"name": "a", "p": "w"}, "b": {"name": "b", "p": "w"}}
    rows = policy_rows(soft, window, {"a": 1.0, "b": -1.0, "c": 5.0}, 0.0)
    assert [(r["name"], r["p"], r["policy_window"]) for r in rows] == [
        ("a", "w", True),
        ("b", "s", False),
    ]


def toy_routes():
    """The Zig test's table: qa (1,0) open, persona (0,1) closed, facts
    (0.6,0.8) at -6; floors 0.9 / -inf / 0.5."""
    return {
        "profiles": ["qa", "persona", "facts"],
        "centroids": torch.tensor([[1.0, 0.0], [0.0, 1.0], [0.6, 0.8]]),
        "offsets": torch.tensor([-math.inf, math.inf, -6.0]),
        "floors": torch.tensor([0.9, -math.inf, 0.5]),
    }


def test_span_gate_decision_matches_the_zig_vectors():
    t = toy_routes()

    def routed(q, margin, fallback=True):
        return span_gate_decision("routed", margin=margin, offset=-3, table=t, question=q, fallback=fallback)

    d = routed([5, 0.5], -40)
    assert d["open"] and d["profile"] == "qa" and not d["fallback"]
    assert d["similarity"] == pytest.approx(0.99504, abs=1e-4)
    d = routed([0.1, 1], 40)
    assert not d["open"] and d["profile"] == "persona"
    assert not routed([0.6, 0.8], -6.5)["open"] and routed([0.6, 0.8], -6)["open"]
    d = routed([2.03, 1], -3.5)
    assert d["fallback"] and d["profile"] == "qa" and d["offset"] == -3 and not d["open"]
    assert routed([2.03, 1], -3)["open"]
    assert routed([2.03, 1], -3.5, fallback=False)["open"]
    assert routed([-1, -1], 0, fallback=False)["profile"] == "qa"  # first maximum
    d = span_gate_decision("always", margin=40, table=t, declared="persona")
    assert not d["open"] and d["profile"] == "persona"
    with pytest.raises(ValueError, match="unknown"):
        span_gate_decision("learned", margin=0, table=t, declared="other")
    with pytest.raises(ValueError, match="no gate routes"):
        span_gate_decision("learned", margin=0, declared="qa")
    assert span_gate_decision("always", margin=-100)["open"]
    assert not span_gate_decision("learned", margin=-3.5, offset=-3)["open"]
    assert span_gate_decision("learned", margin=-3, offset=-3)["open"]
    with pytest.raises(ValueError):
        span_gate_decision("learned", margin=math.nan)
    with pytest.raises(ValueError, match="routes"):
        span_gate_decision("routed", margin=0, question=[1, 0])


def frozen_gate(names=("qa", "persona")):
    centroids = torch.zeros(len(names), 2560)
    for i in range(len(names)):
        centroids[i, i] = 1.0
    return {
        "benchmarks": list(names),
        "centroids": centroids,
        "lcb": {"s31": {"qa": -math.inf, "persona": math.inf}},
    }


def test_gate_routes_table_from_the_frozen_gate():
    table = gate_routes_table(frozen_gate(), "s31")
    assert table["profiles"] == ["qa", "persona"]
    assert table["offsets"].tolist() == [-math.inf, math.inf]
    assert table["floors"].tolist() == [-math.inf, -math.inf]
    with_floor = gate_routes_table(frozen_gate(), "s31", {"benchmarks": ["qa", "persona"], "centroid": [0.4, 0.5]})
    assert with_floor["floors"].tolist() == pytest.approx([0.4, 0.5])
    with pytest.raises(ValueError, match="differ"):
        gate_routes_table(frozen_gate(), "s31", {"benchmarks": ["persona", "qa"], "centroid": [0.4, 0.5]})
    bad = frozen_gate()
    bad["centroids"] = bad["centroids"] * 2
    with pytest.raises(ValueError, match="unit"):
        gate_routes_table(bad, "s31")
    with pytest.raises(ValueError, match="unique"):
        gate_routes_table(frozen_gate(("qa", "qa")), "s31")


def test_gate_routes_writes_a_pinned_candidate(tmp_path):
    import encoder_lora_export as export
    import exporter
    from safetensors.torch import load_file
    from test_encoder_lora_export import CONFIG, write_candidate

    parent = tmp_path / "cand"
    write_candidate(parent)
    (parent / "span-head.safetensors").write_bytes(b"span head bytes")
    protocol = json.loads((parent / "protocol.json").read_text())
    protocol["record_kind"] = "span-window"
    (parent / "protocol.json").write_text(json.dumps(protocol))
    pinned = json.loads((parent / "candidate.json").read_text())
    pinned["protocol_sha256"] = exporter.sha256(parent / "protocol.json")
    pinned["span_head_sha256"] = exporter.sha256(parent / "span-head.safetensors")
    (parent / "candidate.json").write_text(json.dumps(pinned))
    torch.save(frozen_gate(), tmp_path / "gate.pt")
    torch.save({"benchmarks": ["qa", "persona"], "centroid": [0.4, 0.5]}, tmp_path / "fb.pt")
    args = parse_args(["gate-routes", "--candidate", str(parent), "--output", str(tmp_path / "routed"),
                       "--gate", str(tmp_path / "gate.pt"), "--fallback", str(tmp_path / "fb.pt"),
                       "--seed", "s31", "--offset", "-3"])
    gate_routes(args)
    out = tmp_path / "routed"
    candidate, protocol = export.load_candidate(out)
    assert protocol["span_gate_policy"] == "routed" and protocol["span_gate_offset"] == -3.0
    assert protocol["span_gate_routes"] == {"profiles": ["qa", "persona"], "fallback": True}
    routes = load_file(str(out / "span-gate-routes.safetensors"))
    assert set(routes) == {"centroids", "offsets", "floors"}
    assert candidate["span_gate_routes_sha256"] == exporter.sha256(out / "span-gate-routes.safetensors")
    with pytest.raises(ValueError, match="span head"):
        export.export_reader(out, tmp_path / "b0", config=CONFIG, include_span_gate_routes=True)
    manifest = export.export_reader(out, tmp_path / "b1", config=CONFIG,
                                    include_span_head=True, include_span_gate_routes=True)
    assert "span-gate-routes.safetensors" in manifest["files"]
    with pytest.raises(SystemExit):
        parse_args(["gate-routes", "--candidate", "c", "--output", "o", "--gate", "g",
                    "--seed", "s31", "--offset", "inf"])


def test_read_budget_reads_ranked_documents_until_the_budget():
    docs = ["a" * 300, "b" * 150, "c" * 100, "d" * 10]
    count = len  # one token per character
    assert read_budget_documents(count, docs, 512) == docs[:2]  # 450; +100 would pass 512
    assert read_budget_documents(count, docs, 555) == docs[:3]  # 550; +10 would pass 555
    assert read_budget_documents(count, docs, 560) == docs  # exactly 560 fits
    # The top document is always read, even past the budget; reading stops at
    # the first document that would exceed it (no skipping ahead).
    assert read_budget_documents(count, ["x" * 900, "y"], 512) == ["x" * 900]
    assert read_budget_documents(count, docs, 10_000) == docs
    with pytest.raises(ValueError):
        read_budget_documents(count, docs, 0)
    with pytest.raises(ValueError):
        read_budget_documents(count, [], 512)


def test_multi_windows_stay_in_documents_fit_the_rows_and_follow_span_scores():
    counts = [40, 12, 50, 8]
    bounds = doc_bounds(counts)
    ids = torch.arange(1, sum(counts) + 1)
    never = lambda token: False  # noqa: E731
    start = torch.zeros(sum(counts) + 1)
    end = torch.zeros(sum(counts) + 1)
    # Best spans: doc 2 strongest, then doc 0, doc 3, doc 1 (positions + 1:
    # index 0 is the null logit).
    for pos, value in ((60, 9.0), (10, 6.0), (105, 4.0), (45, 2.0)):
        start[pos + 1] = value
        end[pos + 1] = value
    spans = document_best_spans(start, end, bounds, 30)
    assert [round(score) for score, _, _ in spans] == [12, 4, 18, 8]
    joined, placed = multi_windows(
        ids, bounds, spans, width=32, rows=MULTI_WINDOW_ROWS, left_fraction=0.65, continues=never
    )
    assert len(joined) <= MULTI_WINDOW_ROWS
    owners = [next(i for i, (lo, hi) in enumerate(bounds) if lo <= a < hi) for a, _ in placed]
    assert owners[:2] == [2, 0]  # descending span score
    for (lo, hi), owner in zip(placed, owners):
        doc_lo, doc_hi = bounds[owner]
        assert doc_lo <= lo < hi <= doc_hi and hi - lo <= 32
    assert joined == [int(t) for lo, hi in placed for t in ids[lo:hi]]
    # Short documents (triples) pack several whole windows into the rows.
    tiny = doc_bounds([6] * 20)
    tiny_ids = torch.arange(1, 121)
    tiny_spans = [(float(20 - i), lo, lo) for i, (lo, _) in enumerate(tiny)]
    joined, placed = multi_windows(
        tiny_ids, tiny, tiny_spans, width=32, rows=64, left_fraction=0.65, continues=never
    )
    assert len(placed) == 10 and len(joined) == 60  # 11 x 6 would pass 64 rows
    assert [lo for lo, _ in placed] == [lo for lo, _ in tiny[:10]]


def test_window_policies_are_opt_in():
    assert WINDOW_POLICIES == ("P1", "P2", "P3")
    from qwen_hybrid_eval import parse_paper_args

    base = ["paper-run", "--target", "triviaqa", "--assets", "a", "--candidate", "c",
            "--cases", "x", "--output", "o", "--arms", "hybrid-bench", "--data-root", "d"]
    args = parse_paper_args(base)
    assert args.record_read_tokens == 0 and args.record_window_policy is None
    args = parse_paper_args(base + ["--record-read-tokens", "512", "--record-window-policy", "P3"])
    assert args.record_read_tokens == 512 and args.record_window_policy == "P3"


def test_span_read_set_matches_the_zig_vectors():
    # memorilla_span.zig "read plans follow the Python read budget vectors".
    routes = {"triviaqa": {"read_tokens": 512, "window_tokens": 64}}
    counts = [300, 150, 100, 10]
    assert span_read_set(None, None, counts, 32) == {"documents": 4, "budget_tokens": 0, "window_tokens": 32}
    assert span_read_set({}, "factkg", counts, 32)["documents"] == 4
    assert span_read_set(routes, "triviaqa", counts, 32) == {"documents": 2, "budget_tokens": 450, "window_tokens": 64}
    assert span_read_set(routes, "factkg", counts, 32) == {"documents": 2, "budget_tokens": 0, "window_tokens": 32}
    assert span_read_set(routes, "factkg", [7], 32)["documents"] == 1
    wide = {"q": {"read_tokens": 555, "window_tokens": 64}}
    assert span_read_set(wide, "q", counts, 32)["documents"] == 3
    wide["q"]["read_tokens"] = 560
    assert span_read_set(wide, "q", counts, 32) == {"documents": 4, "budget_tokens": 560, "window_tokens": 64}
    assert span_read_set(routes, "triviaqa", [900, 1], 32) == {"documents": 1, "budget_tokens": 900, "window_tokens": 64}
    assert span_read_set(routes, "triviaqa", [100, 500, 10], 32)["documents"] == 1
    with pytest.raises(ValueError):
        span_read_set(routes, None, counts, 32)
    with pytest.raises(ValueError):
        span_read_set(routes, "triviaqa", [], 32)


def test_span_read_routes_table_rejects_what_zig_rejects():
    profiles = ["triviaqa", "narrativeqa", "factkg"]
    ok = {"narrativeqa": {"window_tokens": 64, "read_tokens": 512}, "triviaqa": {"read_tokens": 512, "window_tokens": 64}}
    assert span_read_routes_table(profiles, ok) == ok
    edge = {"factkg": {"read_tokens": 16384, "window_tokens": 256}}
    assert span_read_routes_table(profiles, edge) == edge
    bad = [
        {"squad": {"read_tokens": 512, "window_tokens": 64}},
        {},
        [],
        {"triviaqa": 512},
        {"triviaqa": {"read_tokens": 0, "window_tokens": 64}},
        {"triviaqa": {"read_tokens": 512, "window_tokens": 0}},
        {"triviaqa": {"read_tokens": -512, "window_tokens": 64}},
        {"triviaqa": {"read_tokens": 512.5, "window_tokens": 64}},
        {"triviaqa": {"read_tokens": "512", "window_tokens": 64}},
        {"triviaqa": {"read_tokens": True, "window_tokens": 64}},
        {"triviaqa": {"read_tokens": 512, "window_tokens": 257}},
        {"triviaqa": {"read_tokens": 16385, "window_tokens": 64}},
        {"triviaqa": {"read_tokens": 512}},
        {"triviaqa": {"read_tokens": 512, "window_tokens": 64, "policy": "P2"}},
        {"triviaqa": {"read_tokens": 512, "window": 64}},
    ]
    for table in bad:
        with pytest.raises(ValueError):
            span_read_routes_table(profiles, table)


def test_gate_routes_writes_read_routes(tmp_path):
    import encoder_lora_export as export
    import exporter
    from test_encoder_lora_export import CONFIG, write_candidate

    parent = tmp_path / "cand"
    write_candidate(parent)
    (parent / "span-head.safetensors").write_bytes(b"span head bytes")
    protocol = json.loads((parent / "protocol.json").read_text())
    protocol["record_kind"] = "span-window"
    (parent / "protocol.json").write_text(json.dumps(protocol))
    pinned = json.loads((parent / "candidate.json").read_text())
    pinned["protocol_sha256"] = exporter.sha256(parent / "protocol.json")
    pinned["span_head_sha256"] = exporter.sha256(parent / "span-head.safetensors")
    (parent / "candidate.json").write_text(json.dumps(pinned))
    torch.save(frozen_gate(), tmp_path / "gate.pt")
    base = ["gate-routes", "--candidate", str(parent), "--gate", str(tmp_path / "gate.pt"),
            "--seed", "s31", "--policy", "learned", "--offset", "-3"]
    gate_routes(parse_args(base + ["--output", str(tmp_path / "plain")]))
    gate_routes(parse_args(base + ["--output", str(tmp_path / "d1"), "--read-route", "qa=512:64"]))
    _, plain = export.load_candidate(tmp_path / "plain")
    candidate, d1 = export.load_candidate(tmp_path / "d1")
    assert "span_read_routes" not in plain
    assert d1.pop("span_read_routes") == {"qa": {"read_tokens": 512, "window_tokens": 64}}
    assert d1 == plain  # nothing else changes
    assert exporter.sha256(tmp_path / "d1" / "span-gate-routes.safetensors") == exporter.sha256(
        tmp_path / "plain" / "span-gate-routes.safetensors")
    manifest = export.export_reader(tmp_path / "d1", tmp_path / "bundle", config=CONFIG,
                                    include_span_head=True, include_span_gate_routes=True)
    assert manifest["files"]["protocol.json"]["sha256"] == candidate["protocol_sha256"]
    for bad in (["--read-route", "other=512:64"], ["--read-route", "qa=512:300"],
                ["--read-route", "qa=512:64", "--read-route", "qa=256:32"]):
        with pytest.raises(ValueError):
            gate_routes(parse_args(base + ["--output", str(tmp_path / "bad")] + bad))
    for bad in ("qa", "qa=512", "qa=x:64", "=512:64"):
        with pytest.raises(SystemExit):
            parse_args(base + ["--output", "o", "--read-route", bad])


def test_span_read_cascade_config_rejects_what_zig_rejects():
    # memorilla_span.zig "protocol parses the read cascade and refuses read routes".
    ok = {"window_tokens": 64, "threshold": -4.0, "read_tokens": 512}
    assert span_read_cascade_config(ok) == {"threshold": -4.0, "read_tokens": 512, "window_tokens": 64}
    assert span_read_cascade_config({"threshold": -4, "read_tokens": 512, "window_tokens": 16})["threshold"] == -4.0
    edge = {"threshold": 12.5, "read_tokens": 16384, "window_tokens": 256}
    assert span_read_cascade_config(edge) == edge
    low = {"threshold": -100, "read_tokens": 1, "window_tokens": 1}
    assert span_read_cascade_config(low)["read_tokens"] == 1
    bad = [
        [], -4, {},
        {"threshold": -4, "read_tokens": 512},
        {"threshold": -4, "read_tokens": 512, "window": 64},
        {"threshold": -4, "read_tokens": 512, "window_tokens": 64, "mode": "replace"},
        {"threshold": "-4", "read_tokens": 512, "window_tokens": 64},
        {"threshold": None, "read_tokens": 512, "window_tokens": 64},
        {"threshold": float("inf"), "read_tokens": 512, "window_tokens": 64},
        {"threshold": float("nan"), "read_tokens": 512, "window_tokens": 64},
        {"threshold": True, "read_tokens": 512, "window_tokens": 64},
        {"threshold": -4, "read_tokens": 0, "window_tokens": 64},
        {"threshold": -4, "read_tokens": 512, "window_tokens": 0},
        {"threshold": -4, "read_tokens": -512, "window_tokens": 64},
        {"threshold": -4, "read_tokens": 512.5, "window_tokens": 64},
        {"threshold": -4, "read_tokens": "512", "window_tokens": 64},
        {"threshold": -4, "read_tokens": True, "window_tokens": 64},
        {"threshold": -4, "read_tokens": 512, "window_tokens": 257},
        {"threshold": -4, "read_tokens": 16385, "window_tokens": 64},
    ]
    for value in bad:
        with pytest.raises(ValueError):
            span_read_cascade_config(value)
    assert parse_read_cascade("-4:512:64") == {"threshold": -4.0, "read_tokens": 512, "window_tokens": 64}
    for text in ("-4:512", "-4:512:64:1", "x:512:64", "-4:512:300", "-4:0:64"):
        with pytest.raises(ValueError):
            parse_read_cascade(text)


def test_span_cascade_widens_matches_the_zig_cases():
    # memorilla_span.zig "cascade widens below the threshold only".
    cascade = {"threshold": -4.0, "read_tokens": 512, "window_tokens": 64}
    inf = float("inf")
    for margin in (-4.0001, -12, -inf, float("nan")):
        assert span_cascade_widens(cascade, "auto", margin)
    for margin in (-4, -3.9999, 7, inf):
        assert not span_cascade_widens(cascade, "auto", margin)
    assert not span_cascade_widens(cascade, "auto", -12, wide_available=False)
    for mode in ("narrow", "wide"):
        assert not span_cascade_widens(cascade, mode, -12)
        assert not span_cascade_widens(cascade, mode, 3)
    with pytest.raises(ValueError):
        span_cascade_widens(cascade, "max", -12)


def test_gate_routes_writes_read_cascade(tmp_path):
    import encoder_lora_export as export
    import exporter
    from test_encoder_lora_export import CONFIG, write_candidate

    parent = tmp_path / "cand"
    write_candidate(parent)
    (parent / "span-head.safetensors").write_bytes(b"span head bytes")
    protocol = json.loads((parent / "protocol.json").read_text())
    protocol["record_kind"] = "span-window"
    (parent / "protocol.json").write_text(json.dumps(protocol))
    pinned = json.loads((parent / "candidate.json").read_text())
    pinned["protocol_sha256"] = exporter.sha256(parent / "protocol.json")
    pinned["span_head_sha256"] = exporter.sha256(parent / "span-head.safetensors")
    (parent / "candidate.json").write_text(json.dumps(pinned))
    torch.save(frozen_gate(), tmp_path / "gate.pt")
    base = ["gate-routes", "--candidate", str(parent), "--gate", str(tmp_path / "gate.pt"),
            "--seed", "s31", "--policy", "learned", "--offset", "-3"]
    gate_routes(parse_args(base + ["--output", str(tmp_path / "plain")]))
    gate_routes(parse_args(base + ["--output", str(tmp_path / "f"), "--read-cascade=-4:512:64"]))
    _, plain = export.load_candidate(tmp_path / "plain")
    candidate, cascade = export.load_candidate(tmp_path / "f")
    assert "span_read_cascade" not in plain
    assert cascade.pop("span_read_cascade") == {"threshold": -4.0, "read_tokens": 512, "window_tokens": 64}
    assert cascade == plain  # nothing else changes
    manifest = export.export_reader(tmp_path / "f", tmp_path / "bundle", config=CONFIG,
                                    include_span_head=True, include_span_gate_routes=True)
    assert manifest["files"]["protocol.json"]["sha256"] == candidate["protocol_sha256"]
    with pytest.raises(ValueError):
        gate_routes(parse_args(base + ["--output", str(tmp_path / "bad"), "--read-cascade=-4:512:64",
                                       "--read-route", "qa=512:64"]))
    for bad in ("-4:512", "x:512:64", "-4:512:300"):
        with pytest.raises(SystemExit):
            parse_args(base + ["--output", "o", "--read-cascade=" + bad])


def test_span_read_cascade_floor_and_wide_gate_offset_match_zig():
    # memorilla_span.zig "cascade floor and wide gate offset: parsing,
    # boundaries, C* defaults".
    base = {"threshold": -4, "read_tokens": 512, "window_tokens": 64}
    cstar = span_read_cascade_config(base)
    assert "floor" not in cstar and "wide_gate_offset" not in cstar
    g = span_read_cascade_config({**base, "floor": -8, "wide_gate_offset": -5.5})
    assert g == {"threshold": -4.0, "read_tokens": 512, "window_tokens": 64, "floor": -8.0, "wide_gate_offset": -5.5}
    for extra in ({"floor": -4}, {"floor": -3}, {"floor": "-8"}, {"floor": None}, {"floor": float("-inf")},
                  {"wide_gate_offset": "-5"}, {"wide_gate_offset": float("inf")}, {"wide_gate_offset": True},
                  {"floor": -8, "ceiling": 0}):
        with pytest.raises(ValueError):
            span_read_cascade_config({**base, **extra})
    inf, nan = float("inf"), float("nan")
    # Default floor: exactly C*'s `not m >= t`.
    for m in (-inf, -1000, -8.0001, -8, -7.9999, -4.0001, -4, -3.9999, 0, 9, inf, nan):
        assert span_cascade_widens(cstar, "auto", m) == (not m >= -4)
    for m, want in ((-8, True), (-7.9999, True), (-4.0001, True), (-8.0001, False), (-100, False),
                    (-inf, False), (-4, False), (3, False)):
        assert span_cascade_widens(g, "auto", m) is want
    assert not span_cascade_widens(g, "auto", -6, wide_available=False)
    assert not span_cascade_widens(g, "wide", -6)
    for record in (None, "narrow", "wide"):
        assert span_cascade_gate_offset(cstar, record, -3) == -3.0
        assert span_cascade_gate_offset(None, record, -3) == -3.0
    assert span_cascade_gate_offset(g, "wide", -3) == -5.5
    assert span_cascade_gate_offset(g, "narrow", -3) == -3.0
    assert parse_read_cascade("-4:512:64", -8, -5.5) == g
