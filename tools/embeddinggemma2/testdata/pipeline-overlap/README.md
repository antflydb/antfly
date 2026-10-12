# Preparation before model execution qualification

The first video part starts a bounded producer after shared execution admission
and before the model gate/provider lease. Its two-slot queue hands CPU patches
or Metal preparations to the model consumer. The producer never touches model
weights or compute operations. Later parts stay sequential; library calls without
an Io retain their existing synchronous path. Shared geometry-derived admission
covers waiting producers and their retained preparations.

Cold Metal preparation borrows the process device through atomic once setup
without requiring a model provider. Process protection spans producer startup
through joined/fenced cleanup. Producer-owned preparers survive queued imports;
CoreVideo imports also retain their originating cache. Closure wakes blocked
producers and cleanup drains/fences before returning resource capacity.

All 24 model tests pass. Actual Metal checks prepare while another backend holds
its provider lease, abandon full queues for MJPEG/VP9/static AVC/PAFF, and reclaim
all owners. CPU checks cover pre-model preparation, abandonment, cancellation,
shared denial and exhaustive allocation failures. The full macOS video gate has
157 passes and two platform skips. The separate cache-lifetime binary passes
four selected tests (three import gates and one substantive regression).

Current production Metal and actual Linux ARM64 native binaries each pass six
original pretrained/mixed cases, reduced dimensions and admission/decode retries,
eight WebM vectors and ten logical/timestamp/window vectors. Every reference
ranking and top-k set matches. WebM completes ten load requests at concurrency
two; sampling completes eight. Disconnect/retry passes but does not expose the
completion of cancellation. Linux uses the existing offline ARM64 qualification
image with this worktree/model and the static musl server mounted readonly,
8 GiB container memory and four CPUs. Reference weights are never copied here.

The reports bind current source, binaries, fixtures and references. Shared-host
measurements establish correctness and lifetime only, not isolated speedup or
capacity. Model execution still serializes by session. Measured tuning,
cross-group frame reuse, wider codec/display/streaming qualification and Linux
x86_64 execution remain active requirements of the full non-CUDA goal.
