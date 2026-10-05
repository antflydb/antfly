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

# /// script
# requires-python = ">=3.11"
# dependencies = ["mlx-lm>=0.28"]
# ///
"""Teacher throughput: per-question prompts vs one shared-state prefill per case.

    uv run --script scripts/laya/benchmark_laya_teacher.py <model_dir> <records.jsonl> --cases 12
    uv run --script scripts/laya/benchmark_laya_teacher.py <model_dir> <records.jsonl> --score-output scores.jsonl

Both methods score every label by teacher forcing. The shared method prefills
the longest token prefix common to a case's prompts once and branches every
question (and every label) off a copy of that KV cache. The prompt matches
prepare_laya_longcontext_teacher.py's layout (state before question and
labels), so the state is the shared prefix.

The benchmark also prefills each prompt in two chunks at the same boundary with
no fork. The fork's disagreement with a single-chunk prefill should equal that
split's disagreement; any excess would be a cache bug, not numerics.

--score-output scores every record with the shared method and writes raw label
scores and gold targets per decision, for teacher-quality comparisons.
"""

import argparse, collections, json, math, sys, time
import mlx.core as mx
from mlx_lm import load
from mlx_lm.models import cache as cache_mod

KIND = {"choice": "choice", "score": "score", "noul": "yes/no"}


def prompt(r):
    labels, descs = r["labels"], r.get("descriptions") or [""] * len(r["labels"])
    lines = [
        "You are given a state (context) and a question about it.",
        "Read the state carefully, then answer the question by choosing exactly one of the given labels.",
        "",
        "STATE:",
        r["text"],
        "",
        f"QUESTION ({KIND[r['kind']]}): {r['instruction']}",
        "LABELS:",
    ]
    lines += [f"- {l}: {d}" if d else f"- {l}" for l, d in zip(labels, descs)]
    lines += [
        "",
        "Answer with exactly one label from the list above, verbatim, and nothing else.",
    ]
    return "\n".join(lines)


def ids_for(tok, r):
    return tok.apply_chat_template(
        [{"role": "user", "content": prompt(r)}],
        add_generation_prompt=True,
        enable_thinking=False,
    )


def snapshot(cache):
    """Each layer's cache class and state; newer mlx-lm states are not bare
    (keys, values) pairs, and some cache classes carry meta state."""
    return [(type(x), x.state, x.meta_state) for x in cache]


def fork(saved):
    return [cls.from_state(state, meta) for cls, state, meta in saved]


def labels_from(model, tok, saved, last, r):
    last = last - mx.logsumexp(last)
    scores = []
    for label in r["labels"]:
        cont = tok.encode(str(label), add_special_tokens=False)
        lp = float(last[cont[0]])
        if len(cont) > 1:
            lg = model(mx.array([cont[:-1]]), cache=fork(saved)).astype(mx.float32)
            lg = lg[0] - mx.logsumexp(lg[0], axis=-1, keepdims=True)
            mx.eval(lg)
            lp += sum(float(lg[i, n]) for i, n in enumerate(cont[1:]))
        scores.append(lp)
    return scores


def per_question(model, tok, r):
    ids = ids_for(tok, r)
    c = cache_mod.make_prompt_cache(model)
    lg = model(mx.array([ids]), cache=c)
    mx.eval(lg)
    return labels_from(model, tok, snapshot(c), lg[0, -1].astype(mx.float32), r), len(
        ids
    )


def shared(model, tok, case):
    all_ids = [ids_for(tok, r) for r in case]
    n = min(len(i) for i in all_ids)
    p = 0
    while p < n - 1 and all(i[p] == all_ids[0][p] for i in all_ids):
        p += 1
    c = cache_mod.make_prompt_cache(model)
    mx.eval(model(mx.array([all_ids[0][:p]]), cache=c))
    base = snapshot(c)
    out, tokens = [], p
    for r, ids in zip(case, all_ids):
        branch = fork(base)
        lg = model(mx.array([ids[p:]]), cache=branch)
        mx.eval(lg)
        tokens += len(ids) - p
        out.append(
            labels_from(model, tok, snapshot(branch), lg[0, -1].astype(mx.float32), r)
        )
    return out, tokens, p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("model")
    ap.add_argument("records")
    ap.add_argument("--cases", type=int, default=12)
    ap.add_argument("--score-output")
    a = ap.parse_args()
    model, tok = load(a.model)
    groups = collections.OrderedDict()
    for line in open(a.records):
        r = json.loads(line)
        groups.setdefault(r["group_id"], []).append(r)
    if a.score_output:
        with open(a.score_output, "w") as out:
            t, n = time.perf_counter(), 0
            for case in groups.values():
                scores, _, _ = shared(model, tok, case)
                for r, s in zip(case, scores):
                    out.write(
                        json.dumps(
                            {
                                "id": r["id"],
                                "kind": r["kind"],
                                "scores": s,
                                "target": r["target"],
                            }
                        )
                        + "\n"
                    )
                    n += 1
        print(
            json.dumps(
                {
                    "decisions": n,
                    "s_per_decision": round((time.perf_counter() - t) / n, 3),
                }
            )
        )
        return 0
    cases = list(groups.values())[: a.cases]
    per_question(model, tok, cases[0][0])  # warm up
    t = time.perf_counter()
    base, btok = [], 0
    for case in cases:
        for r in case:
            s, n = per_question(model, tok, r)
            base.append(s)
            btok += n
    t_base = time.perf_counter() - t
    t = time.perf_counter()
    fast, ftok, prefixes = [], 0, []
    for case in cases:
        s, n, p = shared(model, tok, case)
        fast += s
        ftok += n
        prefixes.append(p)
    t_fast = time.perf_counter() - t
    split = []
    for case, p in zip(cases, prefixes):
        for r in case:
            ids = ids_for(tok, r)
            c = cache_mod.make_prompt_cache(model)
            mx.eval(model(mx.array([ids[:p]]), cache=c))
            lg = model(mx.array([ids[p:]]), cache=c)
            mx.eval(lg)
            split.append(
                labels_from(model, tok, snapshot(c), lg[0, -1].astype(mx.float32), r)
            )

    def sm(v):
        m = max(v)
        e = [math.exp(x - m) for x in v]
        z = sum(e)
        return [x / z for x in e]

    worst = max(
        abs(x - y) for a_, b_ in zip(base, fast) for x, y in zip(sm(a_), sm(b_))
    )
    worst_split = max(
        abs(x - y) for a_, b_ in zip(base, split) for x, y in zip(sm(a_), sm(b_))
    )
    fork_vs_split = max(
        abs(x - y) for a_, b_ in zip(split, fast) for x, y in zip(sm(a_), sm(b_))
    )
    agree = sum(
        max(range(len(a_)), key=a_.__getitem__)
        == max(range(len(b_)), key=b_.__getitem__)
        for a_, b_ in zip(base, fast)
    )
    # Prefill throughput versus batch size at a fixed length.
    batch_rates = {}
    for bsz in (1, 4, 8, 16):
        x = mx.array([[100 + (i % 1000) for i in range(256)]] * bsz)
        mx.eval(model(x))
        t = time.perf_counter()
        mx.eval(model(x))
        batch_rates[bsz] = round(bsz * 256 / (time.perf_counter() - t))
    decisions = len(base)
    print(
        json.dumps(
            {
                "cases": len(cases),
                "decisions": decisions,
                "per_question_s_per_decision": round(t_base / decisions, 3),
                "per_question_tokens": btok,
                "shared_s_per_decision": round(t_fast / decisions, 3),
                "shared_tokens": ftok,
                "speedup": round(t_base / t_fast, 2),
                "mean_shared_prefix_tokens": round(sum(prefixes) / len(prefixes)),
                "prefill_tok_per_s": round(btok / t_base),
                "max_prob_diff": worst,
                "split_max_prob_diff": worst_split,
                "fork_vs_split_max_prob_diff": fork_vs_split,
                "argmax_agree": f"{agree}/{decisions}",
                "prefill_tok_per_s_by_batch_len256": batch_rates,
            }
        )
    )


if __name__ == "__main__":
    sys.exit(main())
