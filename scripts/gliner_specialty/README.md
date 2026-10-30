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

The offline fixture contract needs neither Python ML packages nor model files:

```sh
python3 -m unittest scripts/gliner_specialty/test_fixture_contract.py -v
```
