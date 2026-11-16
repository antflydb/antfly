# GLiNER2 specialty oracle

The capture imports the reviewed GLiNER2 source tree through `PYTHONPATH` and
rejects dependency, source-tree, model-weight, or module-path drift. It never
uses the separately installed `gliner2` distribution.

Use Python 3.12.3 and the existing locked dependencies in
`zig/pkg/inference/scripts/gliner25/requirements.txt`. A fresh worktree-local
environment can install the full lock:

```sh
python3 -m venv .benchmark-results/gliner-specialty/oracle-venv
.benchmark-results/gliner-specialty/oracle-venv/bin/python -m pip install \
  -r zig/pkg/inference/scripts/gliner25/requirements.txt
```

On the original qualification host, the base Python already supplied the
other exact wheels, so only the two missing packages were installed locally:

```sh
python3 -m venv --system-site-packages \
  .benchmark-results/gliner-specialty/oracle-venv
.benchmark-results/gliner-specialty/oracle-venv/bin/python -m pip install \
  --no-deps 'accelerate==1.10.1' 'peft==0.17.1'
```

Verify the environment without hashing weights or running inference:

```sh
PYTHONPATH=.benchmark-results/gliner-specialty/upstream \
  .benchmark-results/gliner-specialty/oracle-venv/bin/python \
  scripts/gliner_specialty/capture_oracle.py \
  --verify-environment-only --profile pii_multi \
  --source-root .benchmark-results/gliner-specialty/upstream \
  --model-root .benchmark-results/gliner-specialty/models
```

Run one profile at a time. Supply `--check` to reproduce and compare an
existing capture byte-for-byte. Numeric arrays use the shared GLiNER2.5
width-bounded formatter, keeping values intact without one scalar per line.
PII capture format version 2 stores raw span logits as
`[slots, labels, valid_spans]`. Within each slot and label, the valid-span axis
is word-major then width-major and contains only candidates satisfying
`word + width < words`; masked tail cells have no inference contract and are
not retained. GLiGuard remains capture format version 1.

The offline fixture contract needs neither Python ML packages nor model files:

```sh
python3 -m unittest scripts/gliner_specialty/test_fixture_contract.py -v
```

### Privacy window qualification

`qualify_privacy_windows.py` exercises short text, UTF-8 byte coordinates and a
long source with an email beyond the first inspection window. It requires the
selected backend and can compare all discrete entities and scores against a
native reference. Use the pinned original PII model directory; model loading
performs its normal byte qualification. This synthetic check does not measure
customer-text recall.

```sh
python3 scripts/gliner_specialty/qualify_privacy_windows.py \
  --server zig/zig-out/bin/antfly --models-dir "$PWD/.benchmark-results/privacy/models" \
  --backend native --output .benchmark-results/privacy/native
python3 scripts/gliner_specialty/qualify_privacy_windows.py \
  --server zig/zig-out/bin/antfly --models-dir "$PWD/.benchmark-results/privacy/models" \
  --backend metal --output .benchmark-results/privacy/metal \
  --reference .benchmark-results/privacy/native/native.json
```

On CUDA use `--backend cuda` and the CUDA-built inference HTTP server (or the
unified Antfly binary). Results, required-backend logs and comparison summaries
are written under the requested output directory. Each invocation stops its own
server after qualification.
