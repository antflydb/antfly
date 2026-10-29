# AS: Metal host FFN execution family

Baseline: `e0dd52140b`. Implementation: `d5439dc3c5`. Four host-backed K-block FFN entry points now consume
one always-inline phase owner, replacing 52,714 bytes of repeated functions.
Including the owner, its policy and public wrappers removes **32,326 maintained
production bytes**. Shader source, generated regions, CPU/CUDA code and device
images are unchanged. This is a host execution reduction, not a shader reduction.

The public signatures and their validation/error precedence stay unchanged.
Policies preserve the existing differences instead of selecting a common
projection algorithm:

| Gate/up → down | Single-row gate normalization | Down projection |
| --- | --- | --- |
| Q4_K → Q4_K | Existing reduction helper | Existing direct pipeline |
| Q6_K → Q6_K | Existing row kernel | Existing direct pipeline |
| Q4_K → Q5_K | Existing row kernel | Existing direct pipeline |
| Q4_K → Q6_K | Existing row kernel | Existing quantized descriptor |

The two original output-copy policies remain explicit. Input/residual/output
buffer construction, hot scratch selection, activation/multiply separation,
post-down normalization, encoder boundaries and command commit/wait order are
preserved. Diagnostic command labels retain the original public entry point.
Inlining folds format and policy choices; optimized call and ARC retain/release
counts match each original function. No runtime interpreter or new PSO lookup
is introduced.

Validation compares every candidate against its frozen original function and an
independent F64 arithmetic reference. The matrix covers four format combinations,
one/three/eight/sixteen rows, GELU/SiLU, both optional normalizations and both output
storage paths. Test-only forwarding proxies record real Metal pipeline identities,
buffer bindings/offsets, parameter bytes, dispatch geometry, encoder boundaries,
allocations and commit/wait order. Proxies are absent from wall-clock timed calls. The device-time assay uses a
proxy only to capture the real completed command buffer; it adds no GPU commands.

Results:

- **256 cases** preserve byte-identical original outputs and command traces,
  satisfy the independent oracle, preserve canaries and leave inputs, residuals
  and stored GPU weights unchanged.
- **128 aliased + 128 forced-copy outputs** pass. Unaligned pointers alone do not
  force a copy on this device; the checker explicitly declines output no-copy
  allocation to exercise fallback.
- **176 failure comparisons** preserve status and trace, covering command/encoder
  creation, completion and each host-owned allocation decline.
- **84 malformed-admission cases** per implementation preserve the original
  status and precedence, including missing pipelines and invalid slots/shapes.
- The permanent `zig build test-metal-host-ffn` target passes and is included in
  the Metal runtime regression target. It runs without the external baseline
  header, using explicit error/encoder contracts and independent arithmetic.
- The full production Metal projection checker, both-backend check and generated
  freshness (70 files + 129 native regions) pass.
- A source proof verifies unchanged public signatures and byte-identical source
  outside the selected C family, shader factories and generated regions.

Performance results are separated by measured quantity:

- **Device time passes all 96 profile/order checks**, including AA controls.
  Candidate median ratios are **0.995–1.007**; maximum candidate p95 ratio is
  **1.074**, within the preset 1.05 median / 1.10 p95 bounds. The experiment
  sums real `MTLCommandBuffer.GPUStartTime`/`GPUEndTime` intervals, with 64
  adjacent pairs and four calls per arm. Each profile/order also passes four
  trials of the repository's existing 32-arm Metal paired estimator.
- **Host wall-clock tail latency remains unqualified.** The initial 21-pair,
  32-call independent-quantile experiment misses two checks. Its samples switch
  between shared latency modes (about 12 and 20 ms in one case). The revised
  accessor implementation restores original pipeline load sites and avoids
  holding pipeline references across the whole phase. Its separate 64-pair,
  four-call adjacent-ratio experiment passes all medians and all 32-arm trial
  medians, but fails p95 checks including AA controls. Both failed experiments
  remain failed and retained. Neither is presented as a throughput gain.

The GPU assay measures device execution separately from host allocation,
encoding, completion/wait scheduling and their observed tail jitter. It does
not close the host p95 limitation. GPU shaders and the recorded device command
stream remain identical. No failed experiment was automatically retried.

Evidence: `.benchmark-assets/1166/as-campaign/`. It retains original functions,
complete source pins, optimized IR comparisons, independent/original differential
checks, real command traces and all three timing protocols/receipts, including `field-accessor-v2/` and
`gpu-timestamps/`. Initial fixture failures
are retained: the first oracle used the wrong activation ABI value; an early
coverage check incorrectly assumed unaligned no-copy allocations would decline.
