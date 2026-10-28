# AK: rotary and layout ownership

Baseline: `6fe03327ab56df30b55a4fef80a8fcc80947b2aa`.

## Source result

The follow-up removes **2,134 maintained production bytes / 22 lines**:
1,681 bytes / 23 lines from CUDA and 453 bytes with one additional line from
Metal. No production text moves into generated code, tooling or documentation.
The rotary family shrinks from 6,703 to 5,022 bytes (**25.1%**), including all
replacement owners and its typed export catalog. CUDA images and validation
tooling are accounted separately. This is a smaller follow-up than AJ; it is not a further 20% reduction of a backend. Validation
tooling adds 22,192 bytes / 420 lines; no generated production text or image
bytes change.

## Implemented owners

- CUDA's four serving/training rotary forward/VJP exports share one Q/K traversal
  and one pair arithmetic recipe. Two typed ABI recipes and a four-record export
  catalog retain each buffer declaration and policy mapping. Training still
  rounds each product and the final addition to BF16, while serving rounds coefficients and its output
  boundary. Forward preserves masked values; backward writes masked gradients
  as zero. Official training frequency tables and fallback frequency arithmetic
  remain distinct from serving's frequency computation.
- Metal's float/half paged KV seed exports share one typed traversal, including
  independent key and value row strides, partially filled pages and arbitrary
  physical page ordering. A two-record typed ABI catalog owns the float/half
  export declarations.
- Four Metal packed projection split exports share one body. Third-projection
  stores and bias addition are compile-time policies. Existing buffer indices,
  constant records, partial-width behavior and guard checks stay at the exports.

CUDA uses a lexical native recipe because the device-function versions add
registers on older architectures. An indexed device helper and a helper that
retained typed BF16 coefficients did not remove that cost. The accepted recipe
keeps training pointer-base expressions and sequential stores together with
BF16 coefficient storage; equivalent index/store rearrangements increased
registers on SM110. The discarded candidates and their static results are retained without qualifying them.

## Metal acceptance

Both full production libraries compile with Metal 3.1 and their precise/fast
math policies. Forty-eight profiles cover both modes, zero first-projection
width, padded/short packed rows, unequal Q/KV widths, half conversion (including
normal-range ties to even), reversed physical pages, key padding and independent value strides. Every profile
passes AA, AB and BA with exact baseline outputs, zero-tolerance independent
references, output guards and unchanged pipeline resources.

| Cohort | Worst candidate/baseline median | Worst p95 |
| --- | ---: | ---: |
| AA | 1.016429 | 1.055720 |
| AB | 1.013770 | 1.063672 |
| BA, normalized | 1.017786 | 1.018437 |

Each arm uses 2,000 launches, following a 500-launch and 500 ms warmup, with
32 balanced arms. Limits are 1.05 median and 1.10 p95. Timing uses complete
production source factories, not isolated kernel excerpts.

## CUDA acceptance

The final source passes the full CUDA 13.2 audit: all **454 exported kernels**
retain identical machine instructions and resources on SM75/80/89/90/100/110/120,
and the separately compiled SM121a image matches too. After applying the
canonical PTX trailing-whitespace/EOF normalization, **all three checked-in
images rebuild byte for byte**: portable PTX, seven-target fatbin and SM89
cubin. The explicit SM121a cubin is also byte-identical to the baseline. No
artifact update is required, and this establishes unchanged device code,
constants, layout and resource use independently of timing noise.

All 24 fresh rotary profiles pass the independent CPU oracle on Glowworm,
with exact baseline/candidate outputs and intact guards.

The fresh runtime AA/AB/BA matrix also passes all 24 profiles in each cohort,
including independently reset graph replay, guards and register/occupancy
checks. No failed measurement was retried.

| Cohort | Worst candidate/baseline median | Worst p95 |
| --- | ---: | ---: |
| AA | 1.002701 | 1.008127 |
| AB | 1.001935 | 1.034606 |
| BA | 1.001252 | 1.008869 |

The rotary fixture matrix contains 24 profiles across all four exports,
HD2/30/256/512, unequal Q/KV head counts, batch and sequence tails, official
and fallback frequencies, scaling, absent/partial/all-zero masks and excess
launch coordinates. The CPU oracle independently implements each rounding
boundary; frequency-table bits are configuration inputs. It validates both
exports against numerical references, each other byte for byte, and guarded
allocations.

In-place performance inputs are restored outside CUDA event intervals before
each 100-node graph. This bounds repeated rotation rather than allowing an
entire timing run to drift toward zero/nonfinite data. Each arm measures
20,000 kernel launches; reset transfers are excluded. Separately reset graph
replays compare the two versions after identical repeated work. The ordinary
kernel-family harness protocol remains the default.

## Integration and evidence

Local code-generation freshness, compilation with Metal and CUDA enabled,
source-accounting tests and CUDA artifact policy tests pass. CPU kernels and
dispatch decisions are unchanged.

Evidence is retained in `.benchmark-assets/1166/ak-campaign/`: `metal-final/` source
snapshots/manifests/receipts, rejected CUDA candidates/probes, source accounting
and local checks. `final-v3/` contains the accepted-candidate source pins,
compiler artifacts, static results and runtime controller. All 82 CUDA
baseline/candidate source hashes are checked before the final build.

The maintained `generate_metal_layout_fixtures.py` emits the 48-profile matrix;
`check_cuda_rotary.py` emits the 24-profile CUDA matrix and can run its CPU
oracle checks through the CUDA Driver API without Torch/NumPy. The CUDA
family harness adds an opt-in `--inplace-stable` protocol; its existing timing
mode remains the default. The Metal harness honors explicit language versions
and production floating-point-function policies, and reports/gates p95.

The completed runtime archive has SHA256
`0e344361214c02d2cce2031fdb2b254d129563d1fbece84ceb2376f27ebe6f3c`. Independent verification
checks every recorded input hash and recomputes median/p95 gates from raw
arms. The CPU oracle maximum absolute error is `1.1920928955078125e-07`.
