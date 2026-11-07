# Antfly inference

Antfly's local inference engine provides embeddings, reranking, classification,
extraction, generation, transcription, and model management. All first-party
Antfly code is Apache-2.0.

Use the public `antfly` CLI:

```sh
antfly inference pull <model>
antfly inference list
antfly inference chat <model>
antfly inference run
antfly inference --help
```

`libantfly` exposes the same runtime in-process through the C API and embedded
language bindings. Neither the library nor its packages require a CLI executable.
See the [repository README](../../../README.md) and [C API](../../CAPI.md).

From `zig/`, build the public CLI with `zig build antfly`, the embedding library
with `zig build libantfly`, or both with `zig build release`.

For backend development and qualification, `zig build` inside this directory
builds `zig-out/bin/antfly-inference-dev`. It accepts direct commands such as
`antfly-inference-dev run`; benchmark and qualification scripts use it to avoid
building server adapters. It is a development tool and is not shipped in native
release archives or language packages. Browser inference retains its independent
WASM build targets.

## CUDA deployment

Canonical artifacts are generated with CUDA 13.2.86. Precompiled native images
require a driver exposing CUDA Driver API 13.0 or newer (normally NVIDIA R580+);
installing the CUDA toolkit on the deployment host is unnecessary. L4 uses the
bundled SM89 image and A100 uses SM80.

The portable profile, and fatbin fallback on a future GPU without a bundled
native image, require CUDA Driver API 13.2 or newer to JIT the bundled PTX 9.2.
Use a driver compatible with CUDA 13.2 (normally R595+) for those paths. The
standalone SM89 profile admits only compatible SM89 devices. Canonical profiles
require compute capability 7.5 or newer.

Context initialization checks the selected profile, device and driver capability
before retaining a context or allocating device storage. An old driver reports
`CudaDriverTooOld` with the required version and upgrade guidance; an incompatible
standalone image reports `CudaArtifactUnsupportedDevice`. The fast-compressed
fatbin retains all supported native targets and fallback PTX. Its compression
format floor of CUDA 12.4 is distinct from this deployment contract.
