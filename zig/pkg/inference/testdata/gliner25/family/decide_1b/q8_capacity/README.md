# Decide-1B Q8 classification capacity

This regression corpus binds the unchanged, pinned Fastino FP32 model and
upstream Python tree to 32 full prompts. It includes 13 labeled Codex
supersession pairs, six approval-action examples with two classification
tasks, eight concept-triage examples with natural session context, and exact
199-, 512-, 700-, 1024-, and 1025-token controls. The 1025-token reference is
retained to test rejection; it is not an admitted request.

The two JSONL inputs came from `antflydb/codex` at
`d4a7792657d6558f755f28bbccf68654c1b914ce`, under
`codex-rs/antfly/tests/fixtures/knowledge_eval/`. Their complete bytes are
pinned alongside the reference in `family/manifest.json`. They are evaluation
inputs, not span-head training examples.

The reference was generated with the complete upstream GLiNER2 Python tree
at `55656fbfa01d3d4a77485e1a1eeeaf682990ccdf` (tree SHA-256
`70f7b5084c98a420558be740b726de8ef86edb2882f93cba8af6946db914e6d4`),
Python 3.12, Torch 2.9.1, Transformers 5.17.0, and Tokenizers 0.23.2.
`capture_decide_capacity.py` verifies the upstream tree, complete model and
sidecar pins before running the untouched upstream classifier. It records
full token IDs, ordered raw logits, all-label probabilities and source-model
answers. The corpus does not grant any capability by itself.

The 31 admitted prompts pass Native and physical Metal Q8 pipeline replay
at the predeclared 0.05 absolute raw-logit tolerance. Maximum drift is
0.012716 Native and 0.012679 Metal. A separate FP32 Native replay checks that
the oracle agrees with the original model rather than attributing a framework
difference to quantization: its maximum raw-logit drift is 0.000005365.
HTTP checks compare every probability at 0.01
absolute tolerance, exact prompt-token usage, and every upstream winning
label; maximum probability drift is below 0.005 on both backends.

Both real HTTP backends execute 64 nonoverlapping windows of exactly 1024
tokens each (65,536 prompt tokens), and reject 65 windows, a 1025-token single sequence, and two request
items. A subsequent ordinary request succeeds. These checks establish
classification request capacity subject to memory admission. They do not
establish concurrent throughput, cancellation ownership, extraction quality,
or autonomous approval safety. Full run reports belong with the model release
evidence outside Git; this directory retains regression inputs and the oracle.

Numerical parity and model quality are separate: the preserved upstream
answers score 7/13 supersession, 6/6 action effects, and 0/8 concept triage on
these small examples. The four synthetic in-budget controls retain `refund`.
The intent question in approval examples has no human gold label. The triage
misses must not be described as a successful application-quality evaluation.

Reproduce with `capture_decide_capacity.py --upstream <pinned checkout>
--model-dir <pinned FP32 directory> --fixtures <this directory> --output <file>`.
Convert the reference's admitted `cases` to the benchmark's five-field array
(`id`, `text`, `schema_json`, `input_ids`, `logits`) and run
`inference-bench-gliner-decide-quant-build`, then the resulting benchmark with
`--warmups 0 --reps 3 --tolerance 0.05 --backend native|metal`. Use
`qualify_decide_http.py --reference <reference.json> --backend native|metal
--url <resident server> --output <external report>` for the actual HTTP checks.
The server must require the chosen backend and retain ordinary memory
admission. The campaign used two CPU threads. Native's exact maximum passed with a
16 GiB process envelope. Metal's exact maximum passed with a 24 GiB envelope
after resource admission refused it at 16 GiB; physical headroom and the
emergency reserve remained enforced. These envelopes are operator ceilings,
not measurements of actual RAM consumption. The earlier 61,696-token
64-window passes are retained separately.
