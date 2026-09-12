# Pinned Fastino JointIE optimizer control capture

This fixture executes the unchanged `candidates`, `constraints`, `base`,
`greedy`, and `beam` modules from Fastino commit
`3c913c7369301133d3b7699252074c4303ada50e`. It uses nine small, fixed-score
candidate graphs. No model, tokenizer, tensor computation, training, timing
benchmark, or quality evaluation is involved.

`capture.json` is the authoritative source result. It retains the exact inputs,
Python body-token candidate IDs and their `str` representations, returned node
and edge order, utility, explicit feasibility flag, and independent final
constraint validation. Small beam-finish summaries retain the count, best
feasible score and whether the independent greedy result beats every retained
finish. Full intermediate snapshots are omitted; no native or Python regression
needs those repeated candidate graphs.
The capture's source-file pins cover the exact bytes compiled by the generator.

The compact metadata was projected from the original capture without changing
any of its nine inputs, final solutions, semantic keys or summary values.
Only intermediate snapshots and the generator identity changed; the source
contract remains byte-identical. This edit did not run a numerical model.
`test_saved_compact_capture_matches_source_replay_and_provenance` separately
replays the pinned, model-free source and compares the entire compact file,
including its generator, source and interpreter identities.

| Fixed case | Source beam32 | Source greedy |
| --- | --- | --- |
| Six disjoint negative edges, twelve positive nodes | 3 edges, utility 117 | 6 edges, utility 114 |
| Greedy baseline needed | utility 114; best retained beam finish 94 | utility 114 |
| Quoted Unicode IDs and slots 2/10 | input edge 0, slot 2 | input edge 1, slot 10 |
| Exact zero gain | edge retained, utility 0 | edge retained, utility 0 |
| Allowed self endpoint | node only, utility 10 | edge retained, reported utility 5 |
| Symmetric edge creates forbidden derived cycle | nodes only, utility 2 | empty, explicit infeasible flag |
| Count alternatives and slots | alternative 1, utility 9 | alternative 2, utility 8 |
| Valid inverse companion | primary plus derived edge, utility 5 | same |
| Equal positive overlapping free nodes | source label-order winner | same |

The self-edge case intentionally records the pinned source's duplicate unseen
endpoint contribution: greedy reports 5 although the unique-node-plus-edge sum
is −5. The beam compares that candidate with the better node-only result. This
fixture does not redefine the source objective or claim an optimizer is exact.

The independent no-edge assignment in the first case is feasible with utility 120.
That makes the difference between this approximate source algorithm and an exact
native solver observable without numerical model differences. Source final
semantic-key ties also differ from input index order and from numerical slot
order (`"10" < "2"`). The source result formatter's later `e1`/`e10` presentation
ordering is a separate integration contract, outside these optimizer captures.

Regenerate with CPython 3.12.3 to retain the captured interpreter identity:

```sh
cd zig/pkg/inference/testdata/gliner25/joint_optimizer_source_v1
PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0 \
  python3.12 capture.py \
  --upstream /path/to/GLiNER2 \
  --output /absolute/path/to/regenerated-capture.json
PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0 \
  python3.12 -m unittest -v test_capture
```

The interpreter path supplies Python only; no installed ML package is used.
The generator rejects preloaded or attempted Torch/Transformers/NumPy imports.
Empty package containers bypass model-loading package initializers; only the
five allowlisted modules execute, directly from bounded, same-descriptor,
hash-checked bytes. A temporary validation observer reads actual beam finish
returns and is restored on success or error. No upstream methods are replaced.

Bounds are nine cases, at most 32 nodes and 16 edges per case, beam width 32,
15 seconds, and a 1 MiB result. Output creation fails if the destination exists.
The retained tests cover exact control results, source-pin rejection, forbidden
imports, source-exception cleanup and byte-exact replay against the saved fixture.
The replay comparison requires the captured CPython 3.12.3 version. Expected warnings
describe the deliberately infeasible greedy derived-cycle case.

`capture.json` and `contract.json` retain the authoritative input, source and
generator identities. Per-run logs and checkpoint/repeat receipts are external
campaign output. Native fixture consumption is validated separately; this
source capture alone does not qualify native behavior or model accuracy.
