# Model loading and download integrity

Antfly is a generic inference engine. Pull models using Hugging Face repository
references, with an optional format, quantization, or revision:

```sh
antfly inference pull Qwen/Qwen3-Embedding-0.6B-GGUF
antfly inference pull Qwen/Qwen3-Embedding-0.6B:safetensors
```

Pull does not accept short model aliases. Explicit pinned bundle variants remain
available for reproducing qualification runs, but they are not required to serve
a model. Local chat shortcuts are separate from the pull interface.

Serving checks the selected artifact route: required files and tensors, tensor
formats, graph validity, model role, and backend support. An unrecognized
architecture is attempted by default; the loader reports unsupported operations
or architectures when it cannot construct a session. Known invalid or unsafe
runtime paths remain rejected. Qualification catalogs and exact model receipts
are not serving allowlists.

Managed download receipts record which files belong to a completed transaction.
They prevent partially downloaded or mixed bundles from being published as
ready. Expected hashes detect corruption or substitution relative to a trusted
reference; immutable revisions also make debugging and benchmarks reproducible.
A receipt is neither a model certification nor proof of numerical quality,
performance, or compatibility with every backend. Local, unmanaged models can
be loaded without a receipt and undergo the same artifact checks.

Qualification reports describe evidence for particular artifacts, hardware,
and runtime versions. They do not prevent other models or quantizations from
using an implemented runtime.
