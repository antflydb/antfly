# Gemma 4 E4B on NVIDIA A100

This is a conservative functional starting point for A100 (compute capability
8.0), with the ordinary CUDA kernels. The checked-in fatbin includes native
SM80 machine code. The new GB10 prefetch kernels remain gated to compute 12.1;
their presence in the fatbin does not select them on A100. The RMS/Q8 mirror,
architecture-specific attention profiles, graph-required settings and BF16
autotuning are disabled by this profile.

No A100 execution or speed qualification has been performed. Building for an
A100 host and inspecting SM80 code are separate from testing on that GPU. The
GB10 throughput results do not establish A100 performance.

## Host and build

The prepared host target is Linux **x86_64**, baseline CPU instructions,
glibc 2.28 or newer. Use a matching ARM64 build for an ARM64 A100 host instead;
the x86_64 executable cannot run there. Install an NVIDIA driver compatible
with the CUDA 13.2 artifacts. A CUDA 13.2-capable driver is preferred; older
CUDA 13.x drivers have minor-version compatibility restrictions. CUDA library
acceleration is optional (`-Dcuda-libs=auto`); the runtime loads driver and
available libraries dynamically.

NVIDIA documents the CUDA 13.2 release driver in the R595 family and
[CUDA 13.x minor-version compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/minor-version-compatibility.html)
on R580 or newer with feature and PTX-JIT restrictions. Prefer a driver that
fully supports CUDA 13.2; see the
[CUDA 13.2 release notes](https://docs.nvidia.com/cuda/archive/13.2.0/cuda-toolkit-release-notes/).
The bundled SM80 SASS avoids depending on PTX JIT for the canonical kernels.
Do not assume a CUDA 12-only driver can load this bundle.

From the repository root, using the worktree's own caches and one output prefix:

```sh
export ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache"
export ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global"
zig build --build-file zig/pkg/inference/build.zig http-server \
  -Dtarget=x86_64-linux-gnu.2.28 -Dcpu=baseline \
  -Dcuda=true -Dcuda-artifacts=fatbin -Dcuda-libs=auto \
  -Doptimize=fast -Dstrip=true -j1 \
  --prefix "$PWD/zig-out" --prefix-exe-dir bin/a100-linux-amd64
```

The executable is
`zig-out/bin/a100-linux-amd64/antfly-inference-http-server`. Copy it together
with this launch helper and `gemma4_e4b_a100.env`, retaining their relative
locations. Model weights are separate. The model registry layout must be
`MODELS_DIR/generators/google/gemma-4-E4B-it-qat-q4_0-gguf/`, with its manifest
and GGUF. Keep the same weights when comparing runtimes.

## Launch

Use the helper rather than sourcing the GB10 profile. It removes inherited
Antfly CUDA tuning in the child process and preserves `CUDA_VISIBLE_DEVICES`,
driver/library paths and caller resource settings. The limits below are
example ceilings for a single E4B request on a 40 GB A100; adjust them to the
actual host and free GPU memory. Admission remains enforced.

```sh
CUDA_VISIBLE_DEVICES=0 \
  bash zig/pkg/inference/scripts/gemma4/with_gemma4_a100_cuda.sh \
  ./zig-out/bin/a100-linux-amd64/antfly-inference-http-server run \
  --host 127.0.0.1 --port 8082 \
  --models-dir /path/to/models \
  --preload-model generator:cuda:google/gemma-4-E4B-it-qat-q4_0-gguf \
  --max-concurrent-requests 1 --max-work-units 256 \
  --process-memory-budget-mb 32768 --host-budget-mb 8192 \
  --backend-budget-mb 24576 --combined-budget-mb 28672 \
  --kv-budget-mb 2048 --scratch-budget-mb 2048
```

On the A100 host, verify the reported model/GPU identity, then make a small
request with explicit CUDA selection:

```sh
nvidia-smi --query-gpu=name,compute_cap,driver_version,memory.total --format=csv
curl --fail http://127.0.0.1:8082/healthz
curl --fail -N http://127.0.0.1:8082/ai/v1/generate \
  -H 'Content-Type: application/json' \
  -d '{"model":"google/gemma-4-E4B-it-qat-q4_0-gguf","backend":"cuda","messages":[{"role":"user","content":"Reply with a short greeting."}],"max_tokens":64,"temperature":0,"cache_dtype":"f16","prompt_cache":false,"chat_template_kwargs":{"enable_thinking":false},"stream":true,"stream_options":{"include_usage":true}}'
```

Before an A100 performance claim, run the existing text benchmark with one
resident server at a time, fixed weights/prompts/F16 KV, exact prompt-token
preflight, warmups, repeated measurements and quiet-GPU auditing. Keep this
baseline separate from experiments enabling additional kernels. Record cold
startup, first-token latency, complete request latency and resident memory as
well as sustained decode throughput.
