# Gemma context windowing implementation status

Release decision (2026-10-22): attention matching remains unqualified and disabled
for public serving. The latest fixed-seed regularized one-cell candidate fails
unseen development case02: mass error 0.84589005 against 0.05, aggregate output
RRMSE 0.18208815 against 0.10, and worst-head RRMSE 0.30913523 against 0.10.
Independent FP64 agrees with Metal to 1.385e-6, below the unchanged 1e-5 bound.
This is a generalization failure despite passing both original and case00
training populations. Case03 and the full-matrix campaign remain on hold.

Two subsequent controlled 0.995% internal mass-margin experiments also fail
qualification at the fixed 420-second key deadline. The original line search
ends with a discarded 2.694% checkpoint; the larger-step recovery experiment
ends at 2.644%. Both restore the original seed, remain below 1 GiB, cause zero
swap growth and fully drain. Neither produces a usable candidate. The recovery
experiment improves terminal mass by only 1.86% under the common time budget;
it does not establish a solver speedup. New training and separate internal
development captures are now complete and audited. Their three-prompt training
source preflight and fresh seed generation pass. The seed takes 2.01 seconds
at 207.23 MiB with zero swap growth and full drain; its payload audit also
passes. The actual fitter loading path and worker/supervisor authorization
roundtrip also pass. The three-prompt fit then misses the same 420-second key
deadline, ending with a discarded 4.214% mass checkpoint against the 0.995%
internal target. Its resource guard passes at 989.22 MiB with zero swap growth
and full drain. Independent byte auditing proves complete restoration of the
fresh seed; no candidate or subsequent value/output evaluation is produced.
The new internal-development prompt remains unused. Official gates, deadlines,
memory limits and evaluation separation remain unchanged.

A subsequent matched training-only diagnostic finds that switching the cold
mass solver to HiGHS IPM is slower on the fresh-seed matrix: 6.86 seconds of
solver time versus 2.76 seconds for the current dual-simplex method. Both pass
the unchanged full-coordinate witness and stored-float32 audits. The diagnostic
uses 512.89 MiB with zero swap growth and full drain. This single-matrix result
does not qualify repeated fitting or serving performance. No solver change is
selected; the next diagnostic examines accumulated constraint rows.

The subsequent active-dual hint probe is nonqualifying: seven shadow solves
complete, the eighth times out, and two completed solves change stored bias
bytes. No solver optimization is adopted. The next distinct experiment is a
fresh three-prompt baseline with the original 0.04995 internal mass target;
the official 0.05/0.10/0.10 gates remain unchanged. The preceding 0.00995 target
was an additional research condition, not a production requirement. This new
experiment must start from the audited seed and complete support, value and
output fitting; no discarded strict-margin checkpoint may be reused. It has
not yet run and grants no numerical, evaluation or serving authority.

The candidate passes the unchanged pooled training gates at coefficient
`2^-14`: mass error 0.04994804, aggregate output RRMSE 0.09683376 and worst-head
RRMSE 0.09700055. Its value norm is 2.70 times the seed norm, down from 55.05
times without regularization; maximum component change falls from 2484.03125
to 60.08594. That training stability improvement does not establish unseen
accuracy. The 183.59-second reference fit is not
serving-performance evidence. Full-matrix
accuracy, fresh held-out qualification, measured 25% physical context savings,
serving performance and soak testing remain open.

The preceding Main6 installed run reaches numerical validation
and fails with `CompactionQualityRejected` before publication. It takes 441.80
seconds at 4001.44 MiB combined RSS, with zero swap growth, full process drain,
zero guard violations and zero reported leaks. The ledger ends in states
`[1, 0, 4]` with durable abort; tuple
`0593d39be21e60b7cd66a5c7c29afecb7905a42f82a70ee82c07449e38587996`
is permanently consumed and cannot be retried or used to tune this recipe.
The Main6 binary does not persist the failing coordinate or numerical margins,
so this result establishes a quality rejection but not which metric failed.
No compacted allocation was published and no measured 25% savings pass exists.
Exact terminal audit:
`/private/tmp/attention-matching-main6-installed-all36-terminal-independent-audit-v1.json`
(SHA-256 `44bad2efe989038f734d61740c520451e61f64572e778b1be0de242bd3867840`).
Main7 adds bounded per-artifact validation reporting and exact profile-aware
serving admission. The remaining numerical work is an independently developed
training recipe followed by new held-out qualification. The numerical thresholds
remain unchanged.

The Main7 successor adds bounded per-coordinate validation reports, exact
float32 bit preservation, independent report/readback checks, and profile-aware
retained-row admission before allocation. The request keeps its attention
matching admission plan while packed storage uses a separate exact ingestion
plan. The frozen 12-block profile needs 6272 rows including continuation, so it
correctly fails the 6144 serving cap. A 341-row owner4 profile would use 6140
rows; it now has a one-cell training pass but no full-matrix or held-out
numerical qualification. Protection settings, quality thresholds and public serving gates
are unchanged. The Python trace and loader-budget checks pass 29 tests.

Main7 local1 compiles and selects 575 native tests: 530 pass, 44 skip, one fails
and none leak. Its ownership fixture supplied inconsistent policy/request caps
(4096 versus 1028), which the new admission check correctly rejects. The
fixture-only repair uses its intended 1028 cap and explicit prefix1/recent512
geometry, and asserts equality with the exact ingestion plan. The failed build,
source archive and resource report remain preserved at
`/Volumes/Extreme/antfly/qualification/context-full-release-build-main7-local1`.

Main7 local2 passes the full native build: 575 selected, 531 passed, 44 opt-in
skips, zero failures and zero leaks. It takes 597.93 seconds at 4263.73 MiB peak
process-group RSS, with zero swap growth and full process drain. Independent
auditing binds all eight changed/added files to the live worktree and full source
archive. Build receipt:
`/Volumes/Extreme/antfly/qualification/context-full-release-build-main7-local2/full-release-build.json`
(SHA-256 `209cc107a6eeb10972df3010cbc51f6f6c8986f672abb2bdd8daa5a799bee11c`).
The actual compiled Zig receipt also passes the Python decoder in a separate
one-test serialization fixture, including exact group counts, float bits and
nonfinite values: 0.64 seconds, 266.48 MiB combined RSS, zero swap and full drain.
Interop audit:
`/private/tmp/attention-matching-main7-trace-interop-root-audit-v1.json`
(SHA-256 `7981c371ec6dc2b7eb166395167a9d31e2abe1f8a6c546d6feb5e8fbbca2e027`).
These checks qualify the diagnostics and local code tests, not a new model
recipe, measured context savings, serving performance or production release.

The fresh Main7 V2 training capture passes both native phases. Coverage takes
121.49 seconds; the full 36-cell query capture takes 420.07 seconds at 3611.83 MiB
combined RSS with zero swap growth and full process drain. Source sealing
completes with receipt
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-training-source-v2/final-source-receipt.json`
(SHA-256 `bbf4fb5e9f7075aa12694b7e34024e15efca34195896e19da3c2002287e75d69`).
Independent terminal audit V3 passes and verifies all 36 cells and 361 files
(729737479 bytes), exact build identities, and resource drainage:
`/private/tmp/attention-matching-main7-source-terminal-independent-audit-v3.json`
(SHA-256 `b88ff9a9d629c12a6c6a58198bf80b8ba9160e5dc23647e50fd270e3433e1018`).
Root revalidation under the 512 MiB metadata guard passes in 2.24 seconds at
333.23 MiB combined RSS with zero swap and full drain. Auditor V2's failed
metadata-only attempt is preserved: it incorrectly expected an empty
predecessor list where the reviewed template, plan and operational receipt
consistently use null. V3 checks the actual null contract; the successful native
capture was not repeated.

The preceding Main7 V1 launch stopped before preflight or model load because
its coverage-resource path did not match the operational receipt's derived
sibling path. V2 uses fresh output paths and checks this relationship before
launch. Two regressions exercise the actual operational wrapper. Neither this
prelaunch failure nor the terminal-auditor mismatch supplies numerical evidence.

The first two Main7 development tokenizer batches complete with zero model
forwards, zero swap and full drain. Their question lengths are respectively
`[24, 30, 25, 26, 20, 24]` and `[50, 51, 48, 46, 47, 49]`; all retain the exact
6656-token document prefix. Their zero-eligible and one-eligible outcomes remain
preserved. The V4 batch completes in 36.89 seconds at 3290.50 MiB combined RSS,
with zero swap growth, full drain and zero model forwards. Its lengths are
`[50, 51, 50, 50, 50, 50]`; five questions meet the frozen geometry, and the
declared first-three ordering selects cases 00, 02 and 03. Independent terminal
and selection reviews pass, and all six question texts are frozen before any
development numerical testing. Terminal audit:
`/private/tmp/attention-matching-main7-development-tokenizer-terminal-independent-audit-v4.json`
(SHA-256 `caf124cb1a77cf50ad12b46eb0588c5ba363691d600f6f8a5056dfd5babc6827`).
This establishes prompt geometry only. Harness integration now distinguishes raw document identity,
ordered selected-source identity, ordered-message request identity and native
structured-request identity; these are separate hash domains, not interchangeable
digests.

Frozen development case00 now passes coverage, the 36-cell query capture and
both source and nonheld-role audits. Query capture takes 418.28 seconds at
3638.09 MiB combined RSS with zero swap growth and full process drain. The source
contains 36 cells and 361 graph files (729737695 bytes):
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-development-case00-source-v1/final-source-receipt.json`
(SHA-256 `ee2197b95310f14c424f57526248df88b16b0dc108d820d77b3be5dffa2ee132`).
The independent source audit recomputes the entire execution/resource chain,
and role conversion binds the frozen question, original input identities,
sealer-created copies and reviewed-template lineage:
`/private/tmp/attention-matching-main7-nonheld-development-case00-source-independent-audit-v1.json`
(SHA-256 `1f2682ff8337765fe0de4029599dced0fee01b0ccd23bca62f852d7570c6381d`).
This establishes development data provenance, not candidate accuracy or held-out
qualification. Only the development training-query partition may be consumed.

A fresh owner4/block2048 seed retains 341 rows and covers all 512 exact source
rows. Seed generation passes in 0.93 seconds at 163.86 MiB combined RSS with zero
swap and full drain. Independent auditing verifies stored array types, geometry,
assignment counts and the domain-separated seed digest:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-seed-terminal-independent-audit-v1.json`
(SHA-256 `81d42ab7c795796908f217b4d92ad2e92c56883c272b0de10d29970d8c500bd4`).
This is a fresh training seed, not a successful numerical fit. A static estimate
projects 25.78% physical savings with this profile relative to the historical
Main6 baseline, assuming unchanged session/pool accounting. Current-build
installed allocation measurement remains mandatory.

The first r341 pilot attempt stops in source preflight before fitting because
the loaded-helper closure omits a lazily imported source-validation helper.
The resource receipt records 0.88 seconds, 110.20 MiB combined RSS, zero swap,
full drain and no resource violation; its failed process status is preserved.
No fit, candidate payload or numerical validation was produced. The failed
campaign remains at
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-owner4-block2048-r341-feasibility-v1/campaign-receipt.json`.
The successor binds the actual source-load import closure and uses a fresh
output directory. Its real command-line source check passes with all 23 helper
origins, 912 training queries and 341 retained rows, without invoking a fit.
The check also canonicalizes worker self-identity across imported and command-line
execution; metadata-only preflight did not establish those properties.

The subsequent r341 single-cell feasibility run completes with stored mass
error 0.0499481011, aggregate output RRMSE 0.0966781705 and worst consumer RRMSE
0.0966838420, passing the unchanged 0.04995 internal mass and 0.10 output targets.
The mass phase alone has worst output RRMSE 0.1223348849; conditional tangent
refinement reaches the passing result. Maximum anchor drift is 0.1452618149,
below 0.375. Fitting takes 80.62 seconds at 696.11 MiB combined RSS with zero swap
and full drain, and the separate stored-payload validation passes. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-owner4-block2048-r341-feasibility-v2/campaign-receipt.json`.
Independent terminal audit passes the complete source/seed/payload/resource
chain, including 18 mass workers and six bias workers in the guarded fit process
group, all drained:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-pilot-terminal-independent-audit-v1.json`
(SHA-256 `27adeee2cddbe3ace9e464ece48e9ed50e22ee76836b449e5c58f718a096b15c`).
This is training-only owner4/block2048 feasibility, not a full matrix, native
numerical comparison, development/held-out accuracy, measured allocation or
serving performance qualification. Its direct single-cell fitting route does not
establish the full matrix's memory or latency behavior.

The next native check rejects that same r341 payload on frozen development
case00 queries. Exact source K/V byte identities match the pilot, but stored
mass error is 1.4427967072 against 0.05, aggregate output RRMSE is 0.4968056083
against 0.10, and worst consumer RRMSE is 0.8326495290 against 0.10. FP64 agrees
with the native report to a maximum absolute difference of 6.0375622e-7, below
the unchanged 1e-5 parity bound. The diagnostic completes in 8.87 seconds and
preserves its numerical rejection:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-r341-case00-development-validation-v1/receipt.json`.
This result blocks a full fitting campaign with the current recipe. Query
alignment and sampling must be checked before attributing the entire gap to
generalization. A prospective pooled-query training recipe must use only
training partitions, explicitly treat any reused development case as training,
and retain unused development cases plus a fresh held-out campaign. Numerical
thresholds and serving gates remain unchanged.

The follow-up training-only FP64 diagnostic confirms identical source K/V,
positions, query scale, sampling protocol and group layout. All 24 prompt/head/
source query segments differ, while query L2 norms remain approximately 494.
The fresh centroid seed performs similarly poorly on both prompts: worst-head
RRMSE 0.31392 on the original prompt and 0.28652 on case00. Fitting improves the
original to 0.09668 while worsening case00 to 0.83265. Maximum stored component
changes are 0.02717 for keys, 3.03324 for bias and 1104.05469 for values. Fixed
cross-component ablations also fail. This supports over-specialization of the
coupled fit rather than a Metal/FP64 discrepancy; it does not identify a single
parameter component as the sole cause. Diagnostic receipt:
`/private/tmp/attention-matching-main7-r341-original-case00-training-diagnostic-result-v1.json`
(SHA-256 `d31902f7c3d51844d84a494869e59ca7af858eda47e4b421133446e1663c5952`).
The guarded diagnostic takes 1.14 seconds at 153.70 MiB combined RSS with zero
swap and full drain. Eight whole-head groups supply the unchanged accuracy gate;
24 source subgroups are reported separately. A prelaunch review caught and
corrected an earlier diagnostic-only grouping error before any execution.

The next isolated experiment combines the original 912 training queries and
case00's 912 training queries, preserving 16 independent prompt/head groups and
48 source diagnostics. It requires a newly generated 341-row seed and the
unchanged solver, numerical thresholds, 1024 MiB process-group limit and zero
swap allowance. Case00 is retired from unseen-development use. Cases02 and 03
remain unused evaluation prompts. This population change is not evidence of
generalization, regularization, full36 feasibility or production readiness.

The pooled source boundary passes with 1824 rows and 16 independent groups:
1.50 seconds, 237.91 MiB combined RSS, zero swap and full drain. Fresh pooled
seed generation also passes in 1.16 seconds at 247.06 MiB with zero swap and full
drain. Its 341 clusters cover all 512 source rows, with two Lloyd iterations and
cluster sizes from one to eight. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-owner4-block2048-r341-pooled-seed-v1/receipt.json`
(SHA-256 `fd7c89d5fe53215d93598f28a33db6ead828e6108af2d3a026aecf7d57e76222`).
Independent terminal metadata auditing binds source, payload bytes, loaded
helpers and resources; the fit consumer still must validate the seed arrays.
These are seed-generation results, not numerical fitting results.

The first pooled fit stops before producing a fitted numerical verdict. Its
value solver rejects duplicate physical `(consumer_layer, query_head)` identities
across the two prompts with `invalid consumer identity`. The prior adapter
review covered the group builder but missed this downstream validator. The mass
worker restores the original seed and records the cause; the supervisor then
raises a secondary `mass phase evidence missing` error while trying to inspect
a success-only field on the rollback record. Both failure records are preserved:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-owner4-block2048-r341-pooled-training-v1`.
The guarded mass phase completes in 119.93 seconds at 823.16 MiB combined RSS,
with zero swap growth and full process drain. This establishes resource behavior
for that attempt, not pooled-recipe accuracy. The successor must carry the
prompt namespace through actual value-solver group validation while preserving
physical layer/head identities, and must retain the original rollback cause.
Cases02 and 03 remain unused; regularization is still only prospective.

The corrected pooled worker carries `(pooled_prompt, consumer_layer, query_head)`
through the actual mass and warm value solvers, preserving physical head labels,
row order and objective arithmetic. Its source preflight now invokes both real
group validators before optimization. The fresh V2 fit passes the unchanged
stored training gates: mass error 0.0499480997, aggregate RRMSE 0.0970406453 and
worst prompt/head RRMSE 0.0971612206. Maximum anchor drift is 0.2011489037, below
0.375. The fit transaction takes 145.76 seconds; peak combined RSS is 853.69 MiB,
with zero swap growth and full drain in every phase. The subsequent independent
stored-payload validation also passes. Campaign receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-owner4-block2048-r341-pooled-training-v2/campaign-receipt.json`
(SHA-256 `653f5464a4042af34a4a80e5870e721fc89b6437946aa7b35645e1534f943b58`).
The result remains a one-cell, two-prompt training pass. Unused-prompt accuracy,
full36 fitting, native parity, measured memory savings, serving performance and
production qualification remain open.

The separate post-fit diagnostic verifies that each training prompt passes:
original mass 0.0499480127, aggregate RRMSE 0.0969286832 and worst-head RRMSE
0.0971612206; case00 mass 0.0499480997, aggregate RRMSE 0.0971525797 and
worst-head RRMSE 0.0971608934. It uses eight whole-head primary groups per
prompt and reports the 24 source segments separately. The value matrix norm
increases from 401.8816 to 22122.3716, with maximum component change 2484.03125.
This is a training stability concern, not an observed unseen-prompt failure.
The diagnostic takes 1.51 seconds at 154.95 MiB combined RSS, with zero swap
growth and full drain. Result:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-pooled-final-training-diagnostic-result-v4.json`
(SHA-256 `175f819dda44b734b54e871318c562650c6e515d82dbb32048717d9a59b36cf7`).
Its predecessor fails during guard import before producing any numerical or
resource output; V4 fixes the canonical worktree import path. Cases02 and 03
remain unused pending a regularized training experiment.

The successful pooled run's active executable closure has matching current
code hashes. Five missing historical expectations found during metadata archive
preparation belong to superseded draft templates and older tangent plans, not
the executed plan. The broad historical archive is therefore not fully
replayable; this does not invalidate the separately checked active run closure.
Exact referrer and JSON-pointer classification:
`/private/tmp/attention-matching-main7-metadata-snapshot-conflict-classification-v1.json`
(SHA-256 `7f5dc180c4249cee4f2b53037b19c4623515f5c9356f8708b6bab1d9c3971dab`).
The preservation-only snapshot is complete at
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-replay-metadata-snapshot-20261022-v4`:
5360 files, 103393396 bytes, manifest SHA-256
`49712f948aecf0cca9b54e82972d90fbc50e538a36e8a19351f7b7d25e64a91a`.
Independent auditing rehashes every copied file. The copy takes 4.23 seconds at
97.05 MiB combined RSS with zero swap growth and full drain. Large artifacts
remain external references, and the manifest explicitly records
`replay_complete: false` and `qualification_complete: false`.

Read-only inspection of the frozen r341 solver confirms that its value phases
solve unregularized weighted least squares. Incoming values provide fallback and
no-regression comparisons; they are not a prior in the objective. FP16 range and
stationarity checks permit the observed large value excursions. An independently
reviewed prospective successor specifies fixed-seed Tikhonov regularization,
with raw accuracy kept separate from the regularized primal/dual gap and fixed
coefficient normalization before dual optimization. At that review stage no
coefficient was selected and no regularized fit had executed. Solver audit:
`/private/tmp/attention-matching-r341-value-solver-regularization-audit-v1.json`
(SHA-256 `314dd177ef6da18300c9e1090ab108a4464b03cba13db019561abe5303a271aa`).
This is a possible later recipe change, not part of the pooled population-only
experiment or evidence that regularization will meet the existing gates.

The fixed-seed regularized implementation now passes independent mathematical
review and 15 focused tests. It keeps raw accuracy separate from the regularized
objective, binds the same seed bytes through mass and tangent phases, and
preserves numerical-failure and timeout rollback. The declared coefficient
order is `2^-14`, `2^-16`, `2^-18`; selection uses training gates only, strongest
passing coefficient first.
The guarded source preflight passes with 1824 training rows, 16 prompt/head
groups and 48 diagnostic source groups. It verifies the active solver modules,
exact seed normalization, phase interfaces and runtime fingerprint without
fitting: 1.43 seconds, 181.80 MiB combined RSS, zero swap growth and full drain.
Independent source audit:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-pooled-tikhonov-source-terminal-independent-audit-v1.json`
(SHA-256 `16cc2086d897ef1a314ddd29424551140b48ab53a4566df93aa469a2ccd043c0`).

The first regularized fit completes at `2^-14`, so the declared strongest-first
schedule does not advance to weaker coefficients. The separate stored-payload
validation passes mass error 0.0499480379, aggregate RRMSE 0.0968337585 and
worst-head RRMSE 0.0970005480; anchor drift is 0.2024051266 against 0.375.
Fitting takes 183.59 seconds. All four guarded phases pass, with peak combined
RSS 884.80 MiB, zero swap growth and full drain. The trace records eight accepted
tangent steps. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-owner4-block2048-r341-pooled-tikhonov-alpha0-training-v1/campaign-receipt.json`
(SHA-256 `bd9492cbf4c41e531b0a9eb5aea931eea0d5135720962a9cb1815e4114222d83`).
Independent terminal auditing passes the source/plan/payload chain, all four
resource receipts, nine baseline/refinement optimality checks, and 21 mass plus
eight bias workers, all drained:
`/private/tmp/attention-matching-main7-r341-tikhonov-alpha0-terminal-independent-audit-v1.json`
(SHA-256 `20c2bb54f5cf2930fdd293e9c2a94eb43a180bac3d56ae1d5e9a6293d3a72bdf`).
The separate postfit diagnostic confirms original prompt mass 0.0499480344,
aggregate RRMSE 0.0966752169 and worst-head RRMSE 0.0970005480; case00 mass
0.0499480379, aggregate RRMSE 0.0969921845 and worst-head RRMSE 0.0969996200.
The stored value norm is 1085.7722 against seed 401.8816, with maximum component
change 60.0859375. This is substantially smaller than the unregularized norm
22122.3716 and maximum change 2484.03125. These are diagnostics, not additional
qualification thresholds. The check takes 1.20 seconds at 141.22 MiB, with zero
swap growth and full drain:
`/private/tmp/attention-matching-main7-r341-tikhonov-alpha0-postfit-result-v3.json`
(SHA-256 `b2a616847620b84a313f8639b37337999a3772d627d9b04f3fc7246ccb4a59e5`).
The standalone diagnostic guard-import path was corrected before execution.
This remains one-cell training evidence. Untouched development accuracy, native
parity, full-matrix and production qualification remain open.

Case02's exact executable closure and the training-only recipe selection both
pass independent review. A bounded ten-minute launch monitor does not obtain
the existing 64% free-memory headroom: observations remain in the 61–63% range.
It exits before creating the root launch receipt or any capture output. This
is an environment constraint, not a failed numerical test; case02 remains
untouched, and its reviewed output paths remain fresh.
A subsequent two-minute monitor after a brief 64% reading also exits without
stable headroom or a launch. No capture or model process remains active.

On resumption, free memory recovers to 67%; the reviewed launch starts after
two stable checks at 65%. Case02 coverage and full capture both pass. Capture
takes 392.87 seconds at 4000.42 MiB combined RSS, with zero swap growth and full
drain. Final source:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-development-case02-source-v1/final-source-receipt.json`
(SHA-256 `9cadb199f8134d7eae6400747726c1b8a00c0c6f0685224e0d98dd4d65183de7`).
The source audit verifies 36 cells and 361 files (729737695 bytes). Its explicit
role receipt classifies the capture as unseen nonheld development evaluation,
with training authority false:
`/private/tmp/attention-matching-main7-nonheld-development-case02-evaluation-source-role-v1.json`
(SHA-256 `3a7795f29b1bf6180ce16a61ed52fb183d8d94b48dcc2dc26fc8a9443b3e0931`).
The generic collector's internal `training` partition name does not authorize
using case02 to fit this candidate. A static validator correction keeps the
24 source-segment groups diagnostic while the declared eight whole-head
quality gates remain unchanged; both groupings still require complete finite
native reports and independent FP64 parity.

The subsequent frozen-candidate test completes with a clean numerical
rejection in 4.41 seconds. Metal reports mass error 0.8458900452, aggregate
RRMSE 0.1820881516 and worst whole-head RRMSE 0.3091352284. Independent FP64
reports 0.8458914298, 0.1820881414 and 0.3091351885 respectively. Both the
eight-head primary comparison and 24-source diagnostic comparison satisfy
1e-5 parity; maximum absolute delta is 1.3846619e-6. The source diagnostic
maximum is 0.42956460 and is not an extra quality gate. Resource limits hold
at 504.78 MiB maximum combined RSS, zero swap growth and full drain. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-r341-case02-alpha0-development-validation-v1/receipt.json`
(SHA-256 `562af509cdc8a966f77e812fdd2b9c3816f1474482173bd0bd315fcd8b242ada`).
Independent terminal audit:
`/private/tmp/attention-matching-main7-r341-case02-alpha0-development-validation-terminal-independent-audit-v2.json`
(SHA-256 `48bfea1318b820c2a0937eefda33bdcc0c2172324f1733401d68e1071e31ddf4`).
Case02 remains permanently excluded from fitting this recipe, and case03
remains untouched. The next work broadens independent training coverage and
examines training-only mass sensitivity and boundary margin. Coverage alone
is a hypothesis; the rejection does not uniquely distinguish insufficient
coverage, key/bias over-specialization, or representation capacity at 341 rows.

A separate training-only sensitivity diagnostic uses original+case00 and never
opens case02/03 queries. For `f(q) = log Z_compact - log Z_exact`, it evaluates
the exact FP64 gradient `scale * (E_compact[K] - E_exact[K])` and distances to
`log(0.95)` / `log(1.05)`. Both fitted training prompts pass, but minimum log
slack is only 4.95e-5. Median first-order local radii are 8.24 and 8.30 query
units; minimum radii are 0.000933 and 0.001658. Even paired fixed-study-prefill
queries from the two prompts have median distances of 12.2–23.3 units by head.
Pairing means the same source/head/ordinal, not identical token semantics.
These local radii are not curvature-certified robustness bounds. The check
takes 1.15 seconds at 160.72 MiB, zero swap and full drain. Result:
`/private/tmp/attention-matching-main7-r341-alpha0-training-sensitivity-result-v2.json`
(SHA-256 `18cd224e7b11b45d5ddf48c01608ea4765a3b2628c41937f42ad4286345dfbae`).
Terminal audit:
`/private/tmp/attention-matching-main7-r341-alpha0-training-sensitivity-terminal-independent-audit-v1.json`
(SHA-256 `23af27ef7eade0dd680aa03da99cdbe363399fa9657278ccef0ffcd80f68a57f`).

The controlled training experiment keeps the same two training prompts,
immutable original pooled seed, coefficient `2^-14`, key trust, deadlines and
1 GiB/zero-swap limits, while tightening the internal mass target to 0.00995
consistently through mass fitting, support repair, tangent and final stored
acceptance. The official release gates stay 0.05/0.10/0.10; passing those alone
does not count as success for this stricter experiment. Separately, a new
precommitted natural-question roster selects the first two geometry-eligible
questions: one new training population and one new internal-development
evaluation prompt. Neither is case02 or case03, and the second is never fitter
input. Case03 stays reserved until that new internal-development check passes.
The stricter-margin fit and first new-roster tokenizer measurement have now run;
their nonqualifying results are recorded below. No new-roster capture has run.

The strict-margin math successor passes eight synthetic/metadata tests and
independent review. Its transaction and both phase boundaries require exactly
`2^-14`; the weaker historical schedule entries reject. Internal acceptance
and official release disposition remain separate, and the executable validator
must recompute both from the stored payload. Review:
`/private/tmp/attention-matching-r341-fixed-seed-tikhonov-strict-margin-independent-review-v2.json`
(SHA-256 `57ca597d4eba8ebe695aa23c476207046bcd674e9cc689c2df5a6def0ff0b029`).
The new roster selector passes six tests and independent review. It preserves
completed measurements with zero, one or two selected prompts, and marks only
two as sufficient for the separate training and internal-development roles.
Neither review grants capture, numerical or production qualification.

The first strict-margin source preflight completes in 1.42 seconds at 204.11 MiB,
zero swap and full drain. Runtime, objective prior, geometry and controlled
phase interfaces match, but it remains held: its import-origin list omits the
worker because the checker used a name excluded by its own origin filter
(53 recorded origins versus 54 expected). This is an integration failure, not
a solver result. The successor must record the canonical worker origin and
require exact origin-list equality before publishing source evidence; the
first attempt is preserved and grants no fitting authority.
The immutable checker/wrapper successor then passes in 1.42 seconds at
163.31 MiB, with all 54 origins, runtime and objective prior exactly matching,
zero swap and full drain. Terminal audit:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-strict-margin-source-terminal-independent-audit-v2.json`
(SHA-256 `4b482ad984726e11e1bacd6bfda972c7e2db33869dec27a6b3d091a6d9f53acc`).
The fit supervisor successor stays in the standard library, avoiding concurrent
parent NumPy/SciPy RSS under the same 1 GiB combined limit. The validator also
correctly distinguishes an eligible candidate from a byte-identical original
seed returned after solver failure or timeout. It independently audits returned
bytes, requires all dispositions false after rollback, and rejects an invented
final-candidate audit. Nine integration tests and three actual serial
rollback-through-validator checks pass. The final V4 source preflight passes
in 1.75 seconds at 225.33 MiB, with the same exact 54 origins/runtime/prior,
zero swap and full drain. Terminal audit:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-strict-margin-source-terminal-independent-audit-v4.json`
(SHA-256 `911766b8a6c6e68d16f2d151510c7e0d463768f712ccc25959602c645586c710`).
These checks establish interfaces and provenance, not a numerical pass.

The strict-margin fit then terminates at the fixed 420-second key deadline.
Its guard passes at 419.07 seconds and 834.52 MiB, zero swap and full drain;
the serial transaction records 420.03 seconds and restores the original seed.
The last discarded checkpoint has mass error 0.02694342 against internal
0.00995, after 87 accepted steps and 97 LP attempts (96 completed, followed by
a deadline worker without a response). The LP consumes 416.57 seconds and
386,171 recorded iterations; maximum anchor drift is 0.21312 against 0.375.
No value fitting, conditional output phase, final validation or usable
candidate occurs. The discarded checkpoint is not a returned mass pass.
Terminal audit:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-strict-margin-terminal-independent-audit-v1.json`
(SHA-256 `c3df43555189667746efbb62a83797a2c41f45ea7b2b2ddad3fb0449d834aeb8`).

A metadata-only trace analysis finds that late constraint generation already
uses about 1,307–1,312 of 1,824 rows and needs only one or two rounds. The
line search shrinks to trial 8 (`2^-12`) but never re-probes larger steps after
sustained acceptance. The next single controlled change probes exactly one
canonical larger step after eight consecutive accepts at the same preferred
trial, then keeps the existing preferred/fallback order. Fresh LP witnesses,
stored acceptance, quantization, trust and all budgets stay unchanged. This
is a new prospective training recipe, not evidence that the tighter target
or unseen quality will pass. Analysis:
`/private/tmp/attention-matching-main7-strict-margin-key-trace-bottleneck-analysis-v1.json`
(SHA-256 `d5a960aa5e0dea0261642be1d33a1fd8be3eb739e205eae4c78e118540948c35`).

The first growth-probe package is permanently held before real-data execution:
its producer transiently edited handed-off source/test paths, then restored
their recorded hashes. Restoration does not repair that historical immutability
gap. Root observed transient test hash `d9beb9cd...` and preserved the restored
closure separately; the transient mass bytes were not retained. No model,
real-data fit or native run used this package. A fresh V2 handoff uses new paths,
discloses the incident, resets the streak after an active-residual acceptance,
and counts probe acceptance only after commit. Its six synthetic growth checks
and inherited eight strict-margin checks pass, followed by independent static
review. Handoff:
`/private/tmp/attention-matching-r341-strict-margin-growth-probe-temporary-handoff-v2.json`
(SHA-256 `916452f0bce7b6b427d13ad59ad732fb4644ff443eedcd22f756cd8142db9c9c`).

The first root growth source preflight is rejected before child creation:
internal disk headroom is 3,236 MiB against the unchanged 4,096 MiB floor.
No source arrays, numerical fit or native job run. The negative admission audit
is preserved separately:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-strict-margin-growth-source-admission-failure-audit-v1.json`
(SHA-256 `0eca928645037d5a06cd1ef0e4ffcb7e06e20e15cc811218c347907ae2e2303d`).

Further adversarial review holds the V2 math package and its source-preflight
preparations: preferred-trial/streak bookkeeping can advance before acceptance
commits. A deadline still returns the previous numerical payload safely, but
the reported last accepted trial or streak can describe an uncommitted proposal.
A fresh successor must commit this state alongside the payload after both
deadline checks and cover duplicate probes, stored fallback and checkpoint
timeouts. No handed-off files are edited, and no real growth fit has run.

The fresh V3 math package fixes preferred-trial/streak commit ordering, but is
also held when review finds that residual-acceptance counters still advance
before commit. Fresh V4 commits trial, streak and every acceptance-route count
alongside the payload after both deadline checks. It enforces
`accepted_steps == shadow_accepts + stored_base_accepts + fallback_accepts` at
runtime. Eight focused growth checks and eight inherited strict-margin checks
pass, including probe/residual checkpoint and precommit deadline exits.
Independent review passes the narrow change:
`/private/tmp/attention-matching-r341-strict-margin-growth-probe-independent-review-v4.json`
(SHA-256 `a584a2930e76d2cd35bb642efdb8f2a4f926af57a22284f35063e101ecbe1ef6`).
The earlier growth source preparations remain held. A subsequent outer package
also stays held after static review catches an inherited campaign schema;
no real-data run uses it. The fresh `growth-commit2` supervisor checks its
explicit campaign schema on every receipt write. Its source preflight passes
in 1.74 seconds at 190.53 MiB combined RSS, with zero swap growth and full
process drain. All 54 loaded origins, 86 root-reviewed edges, the immutable
pooled seed, objective prior and controlled phase interfaces match. It does
not invoke fitting or write scientific arrays. Independent terminal audit:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-strict-margin-growth-commit2-source-terminal-independent-audit-v1.json`
(SHA-256 `80278ed6a705001b5fe4330af048de0a7a4e6911d2212205c8b9f9be1522360e`).
This source/interface evidence is followed by a separately sealed fit. The fit
reaches the unchanged 420-second key deadline and restores the original seed.
Its resource guard passes in 418.94 seconds at 691.66 MiB combined RSS, with
zero swap growth and full drain. The last discarded mass is 0.02644339 against
0.00995, after 58 committed shadow accepts and 69 LP attempts; LP work consumes
416.19 seconds and 323,446 recorded iterations. All four growth probes are
accounted for: one accepts and three reject. No support/value fitting, output
phase, final validation or usable candidate follows. Independent terminal audit:
`/private/tmp/attention-matching-main7-owner4-b2048-r341-strict-margin-growth-commit2-terminal-independent-audit-v2.json`
(SHA-256 `7160406b606b5d74ce7c9cdc9a3b57334ea9cdfe584eed69040eb3409bec1496`).
Nested key-phase mass diagnostics describe discarded state before full seed
restoration; they are not metrics of the returned payload.
An independent standard-library ZIP check also verifies complete byte identity
of the returned keys, values and bias NPY entries against the immutable seed,
without decoding arrays or recomputing quality. The NPZ archive digests differ
because the seed additionally contains clustering metadata. Addendum:
`/private/tmp/attention-matching-main7-growth-commit2-rollback-entry-byte-identity-v1.json`
(SHA-256 `4f117038fdf88c3e8fe355dd7db37fe260be195c17123dbe905e6b06379b6acc`).

A metadata-only comparison finds identical accepted states through the first
29 commits. The accepted larger-step probe then changes the trajectory. At
58 commits the growth route has 8.05% lower mass, but its terminal mass under
the common time budget is only 1.86% lower, still 2.66 times the strict limit.
Later solves cost more time despite fewer total solves and iterations. These
diverged trajectories and single timed runs do not establish a solver speedup
or justify changing tolerances, basis policy, thresholds or deadlines. Report:
`/private/tmp/attention-matching-main7-strict-margin-growth-commit2-trace-comparison-v1.json`
(SHA-256 `40f64866759335a6256e58cc06f014f63834d9d4b06ddc18ee61b0bc407df026`).
Postfit/sensitivity diagnostics remain unavailable for this failed fit; the
next data step is the precommitted broader training/internal-development
roster, preserving the separate roles and all evaluation exclusions.

The first broader-roster tokenizer measurement completes in 32.55 seconds at
3,326.02 MiB, zero swap and full drain, starting at stable 69% free memory.
All six prefixes match, but question lengths are 52/49/55/58/55/54 rather than
the frozen 50-token target. It faithfully publishes a completed zero-selection
receipt with `selection_sufficient=false`; it grants no capture authority.
No model forwards or output-quality observations occur. A fresh roster may
make minimal natural wording changes based only on these lengths, retaining
first-two selection, separate training/internal-development roles and all prior
question exclusions. Terminal audit:
`/private/tmp/attention-matching-main7-broader-training-tokenizer-terminal-independent-audit-v4.json`
(SHA-256 `3e87032305995d602f900dab355433768e02ed129cba99af1714eb6770f830cd`).
The minimally corrected roster and tokenizer V5 pass seven tests and independent
review, with the six earlier raw question hashes additionally excluded. A first
admission check rejects at 51%/46% free memory without launching a child. After
the user frees RAM, the native measurement runs at stable 75%/75% headroom and
passes in 33.65 seconds at 3,347.81 MiB, zero swap growth and full drain. The
six question lengths are 50/50/48/53/52/48; the first two have exactly 8,160
prompt tokens, and every prefix matches. The receipt records sufficient
selection with rank0 training-only and rank1 internal-development-only, zero
model forwards and no observed output quality:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-broader-training-tokenizer-v7/receipt.json`
(SHA-256 `f4c10b30bb16ebcd79ed383cca6915d77208d283b7fa23741a2eae2cc630851f`).
The first terminal auditor rejects before publishing an audit because the
original root review lists two identical edge declarations twice (32 entries,
30 unique paths). The immutable auditor successor passes five controlled
checks, rehashes all 30 unique paths and the separate handoff/static review,
accepts only complete identical repeated declarations, and rejects hash or size
conflicts. Its actual terminal audit binds the 75%/75% admission, exact native
receipt/logs/resource and both selected roles:
`/private/tmp/attention-matching-main7-broader-training-tokenizer-terminal-independent-audit-v6.json`
(SHA-256 `92d446caaf660de2fadc7ea86fa0c1695ec7a09686720c4225ad8222ca379cc9`).
This establishes geometry selection; each role still needs its own reviewed
capture template and execution transaction.

The rank-specific capture preparation then closes the distinct native/ordered
request identity domains, tokenizer admission binding and operational resource
path contract. Earlier preparations remain preserved as held; V2 also has a
disclosed handed-off-file edit/restoration incident and cannot be qualified by
restoring its hash. V5's missing-parent-directory failure occurs before creating
configuration, template or model output. The fresh V6 preparation and actual
role package are independently reviewed before capture.

Rank0 training capture completes under the unchanged guards: native collection
takes 422.70 seconds at 4,566.14 MiB combined RSS, with zero swap growth and full
drain. Its 36-cell source passes an independent terminal audit; that audit itself
takes 2.23 seconds at 348.33 MiB under the existing 512 MiB metadata limit, with
zero swap and full drain. A separate role bridge binds the exact tokenizer
selection, admission, executable plan, source and request identities and grants
training-population-extension eligibility only:
`/private/tmp/attention-matching-main7-broader-training-rank0-source-role-independent-audit-v1.json`
(SHA-256 `585a42a97da85d90928c2b04974ef67c6c54b08842e1b5fb33d6ca5e0b441f14`).
The generic collector's training label alone is insufficient for fitting. Rank1
remains internal-development-only and never fitter input. These captures do not
establish a numerical recipe pass; the three-prompt pool still requires a fresh
seed, bounded fitting and separate evaluation.
Rank1 also completes: 433.04 seconds at 3,991.84 MiB native capture RSS, zero
swap growth and full drain. Its independent terminal audit passes in 2.54
seconds at 346.83 MiB under the 512 MiB metadata guard. The separate role audit
explicitly declines the generic collector's training authority and records
`training_source_authority=false`, `training_eligible=false`,
`evaluation_only=true` and `never_fitter=true`:
`/private/tmp/attention-matching-main7-broader-training-rank1-source-role-independent-audit-v1.json`
(SHA-256 `449571da3a3dd6ad70fd9c1d4f863ddffa733eeb8bb41a5cec590b65147c8e7d`).
No candidate quality evaluation has used this new internal-development prompt.

The three-prompt source/seed adapter passes all 16 guarded synthetic checks,
including exact NPZ members, dtypes, shapes, assignments, cluster sizes and
recomputed profile digest. It binds the audited original, retired case00 and
rank0 training sources into 2,736 rows, 24 primary groups and 72 diagnostic
leaves; rank1, case02 and case03 remain excluded. The first producer/profile
review is revoked after further review finds numeric booleans accepted as
review fields and raw stratum dictionaries passed to the centroid core's
integer-index interface. The immutable V2 producer enforces typed review
values and converts the complete strata partition before calling the unchanged
core. Its actual synthetic profile-to-core generation passes under the existing
512 MiB, zero-swap guard, with full process drain. The short run's sampled RSS
does not establish a memory bound for real source generation. Independent review:
`/private/tmp/attention-matching-main7-owner4-three-prompt-seed-producer-independent-review-v2.json`
(SHA-256 `5d5e8bf3b5c3ad9fd8f854f42e44ee3d1db09dec9de51723f74d19275e92ebb1`).
The V2 execution contract is subsequently held for a circular plan/review hash.
The immutable V5 producer uses an independently reviewed unauthorized template,
requires an exact three-field authorization delta, rejects numeric booleans
at every authority boundary, and proves the template's unauthorized controls
before creating that delta. Its on-disk plan/authorization tests pass. Existing
source-context bytes remain unchanged; no capture or source preflight is repeated.

The reviewed V5 execution generates the actual fresh three-prompt seed in 2.01
seconds at 207.23 MiB combined RSS, with zero swap growth and full process drain.
The payload retains 341 rows from all 512 source rows and has SHA-256
`17d6bf8a470c9be7f5d2963bdc7c18f8eed16b59119ddb0365a8322dd46f0ec1`.
Independent terminal auditing replays the exact source/seed binding, all five
NPZ members, dtypes, shapes, finiteness, assignments, cluster sizes and profile
digest. The audit passes in 0.62 seconds under the 512 MiB metadata guard,
with zero swap growth and full drain:
`/private/tmp/attention-matching-main7-owner4-three-prompt-seed-terminal-independent-audit-v1.json`
(SHA-256 `87a2c36a562f8574fa76119e778f3b2f23baa8b7321cffbf9a34b70243315661`).
This establishes fresh seed integrity and provenance. Bounded fitting,
independent numerical evaluation and measured physical savings remain open.

The three-prompt source/prior preflight then passes in 2.23 seconds at 190.50
MiB, zero swap growth and full drain. It recomputes the population metadata from
the authority loader's returned manifests before exposing training arrays. The
fresh value seed has squared norm 161683.82976633095; the unchanged `2^-14`
coefficient gives normalized rho `3.77496972568063e-10`. Independent terminal
audit binds the source, seed, build, resource and exclusion evidence:
`/private/tmp/attention-matching-main7-owner4-three-prompt-strict-margin-growth-source-terminal-independent-audit-v1.json`
(SHA-256 `81a187bcf6b3199585e39647e012fe4eda271b099a08189396c75f4445d4914e`).
An earlier concrete plan incorrectly named a population adapter as the tensor
loader; it is held before launch. The corrected root plan binds the exact loader.

The subsequent real worker-loading preflight rejects before optimization:
inherited population-identity helpers admit only the original and case00 prompt
names, so they reject the new rank0 training population. Training and seed arrays
load, but no LP, fitting, finalizer or final fit template runs. This is an
integration failure, with zero swap growth, full drain and no resource violation,
rather than a numerical rejection or memory failure. The failed artifacts remain
immutable:
`/private/tmp/attention-matching-main7-owner4-three-prompt-strict-margin-growth-worker-load-preflight-failure-v1.json`
(SHA-256 `127317fcf5f401be274033d5a68e1b04f31789b882bf61a3dd3a675ec3013986`).
The successor must admit exactly the three training identities across primary,
leaf and minimax interfaces, preserving regularization and arithmetic, then pass
the real worker-loading preflight before any timed fit.

The fresh V4 successor closes these interfaces. Four guarded functional tests
exercise the installed mass, value and conditional-tangent reporting routes,
verify 24 unique prompt/layer/head groups and 72 unique diagnostic leaves,
reject excluded populations and prove the identity helper's arithmetic unchanged.
The actual three-prompt worker-loading preflight then passes in 1.67 seconds at
186.17 MiB, zero swap growth and full drain, with all 56 helper origins matching.
It preserves the fresh seed bytes:
`/private/tmp/attention-matching-main7-owner4-three-prompt-strict-margin-growth-worker-load-boundary-v2.json`
(SHA-256 `079945fcdec71468de5692215c9a1df4efd2bc0d3796180996bd0d2678eedfda`).
The final worker/supervisor metadata and authorization roundtrip passes in 0.92
seconds at 98.83 MiB, zero swap growth and full drain. Its first fixture failed
because macOS temporary paths used both `/var` and `/private/var` spellings; a
fresh test-only successor canonicalizes the temporary directory without changing
the production validators:
`/private/tmp/attention-matching-main7-owner4-three-prompt-strict-margin-growth-fit-template-metadata-v2.json`
(SHA-256 `eec9f47ec86961e88584377231e807d0e2d47c8100bc34458564b23a46d965d2`).
These are fitting-interface checks, not a numerical recipe pass.

The actual three-prompt fit ends in controlled numerical nonqualification at
the mass-phase deadline. Its resource guard passes in 418.33 seconds at
989.22 MiB combined RSS, with zero swap growth and full process drain. There
are 43 accepted shadow proposals before complete rollback, 52 LP calls and
304,105 solver iterations; LP calls account for 415.07 seconds. All four
larger-step probes are rejected. The final discarded mass diagnostic is
0.04214102799017283, above the unchanged internal target of 0.00995. Although
that checkpoint is below the official 0.05 mass threshold, it is not eligible
for use: the fit exceeds its mass deadline, never reaches support/value/output
phases, and restores the original seed. The independent terminal audit passes:
`/private/tmp/attention-matching-main7-owner4-three-prompt-strict-margin-growth-terminal-independent-audit-v1.json`
(SHA-256 `e4d850fd0d4c587e54a1632616abe428cc936f03078181ad1f4e2d2c64a81786`).
Separate raw ZIP-member auditing proves that returned keys, values and bias
are byte-identical to the seed without decoding arrays:
`/private/tmp/attention-matching-main7-owner4-three-prompt-strict-margin-growth-rollback-entry-byte-identity-v1.json`
(SHA-256 `9cee3c66b4c663e805a34d5a0353f5cd9004c0fb29273b43c34f7d2af31bd136`).
No rank1, case02, case03 or held evaluation is consumed. The next experiment
must address solver cost under the existing deadline and memory cap; this
larger training population and fresh seed do not form a matched performance
comparison with preceding two-prompt fits.

The cold solver diagnostic uses the exact same 2,736-by-341 fresh-seed mass
matrix in two fresh processes, canonical 512-row constraint generation and no
warm hints. It changes only `highs-ds` to `highs-ipm`, adding explicit IPM
optimality tolerance `1e-9` while preserving `1e-9` primal/dual tolerances,
presolve and all original-coordinate witness/stored-float32 checks. Both
methods complete six restricted solves and agree on the objective to
1.53e-14, with identical stored bias bytes. Dual-simplex takes 2.76094 seconds
inside `linprog`; IPM takes 6.86335 seconds. Complete process times, including
source loading and startup, are 5.24090 and 8.33343 seconds. The guard passes
in 13.90 seconds at 512.89 MiB, zero swap growth and full drain. The additive
independent audit verifies the root result, unchanged dependency closure,
exact child receipts, finite objectives, identical matrix/origins and both
sets of witnesses:
`/private/tmp/attention-matching-main7-three-prompt-cold-highs-terminal-additive-independent-audit-v1.json`
(SHA-256 `4a9403483d5b3fbd7878ee4d0c0ce1bc4fea6aa8959551c96e6a084416ae147b`).
This rejects an immediate IPM substitution on the seed-matrix evidence; it
does not compare later warm problems or qualify a fit recipe. The documented
[SciPy 1.16.3 solver interface](https://github.com/scipy/scipy/blob/v1.16.3/scipy/optimize/_linprog.py)
supports both methods; speed remains problem-dependent. No evaluation data
is used and no method, payload or serving capability is promoted.

The active-dual hint diagnostic replays exactly eight canonical LP calls,
with only canonical solutions driving the optimizer. Alongside each call it
solves the same matrix using the prior canonical dual support above `1e-12`,
unioned with the unchanged 512 seed rows. The first seven proposal hashes
match the original three-prompt trace. All canonical solves and the first
seven shadows pass full witnesses and stored-float32 checks. The eighth
shadow exhausts the remaining 7.39 seconds of its predeclared diagnostic
horizon and drains. The guard passes in 113.08 seconds at 660.42 MiB with
zero swap growth and full drain, while `comparison_valid=false` remains
explicit. The cold control is byte-identical; subsequent raw weights and
dual arrays differ, and calls 2 and 3 also change stored bias bytes. No
complete paired speedup or equivalent-replacement claim is supported:
`/private/tmp/attention-matching-main7-three-prompt-active-dual-hint-diagnostic-terminal-independent-audit-v1.json`
(SHA-256 `3cbfefe719538d0261a9e8b910c6ae17fbb36537bebb8a1bbfeb405060c1a947`).
The probe stops before its eighth optimizer commit, returns no payload and
uses no evaluation data.

The next baseline removes the extra strict-margin research condition in a
new recipe identity. It uses internal mass target 0.04995 while retaining
official mass/output gates 0.05/0.10/0.10, fresh three-prompt seed
`17d6bf8a470c9be7f5d2963bdc7c18f8eed16b59119ddb0365a8322dd46f0ec1`,
coefficient `2^-14`, its fixed-seed prior, trust 0.375, existing CG and growth
policies, 420/560/600-second budgets, and the 1 GiB/zero-swap limits. Every
mass/support/tangent/serial boundary must use the new target consistently
and report `strict_margin_experiment=false`. It must refit from the seed;
the rejected strict fit remains rejected. Only a complete fresh candidate
passing independent stored gates, drift and deadline checks may proceed to
the predeclared rank1 evaluation. This directly tests the broader training
population before changing representation or native retention profiles.

Preservation snapshot V7 seals the completed evidence through the cold solver
diagnostic, including documentation digest `d590d3b2135ef16809fc4e890e5a48ad1d4231019e93883c68a7b63857c56c55`:
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-replay-metadata-snapshot-20261022-v7/manifest.json`
(SHA-256 `acb818c40a071422675704f259e7f91269c54819d68a3948c654dd136e1d6cb3`).
The streaming terminal audit verifies all 6,644 source/copy pairs and
191,651,493 bytes per side. It passes in 5.51 seconds at 114.23 MiB, zero
swap growth and full drain:
`/private/tmp/attention-matching-main7-metadata-snapshot-v12-terminal-independent-audit-v1.json`
(SHA-256 `ee48d8fb9487614f52184adb83c621994de3d68c21c59bba82218ff8d99a0910`).
Raw `.bin`, `.i64` and `.npz` tensors remain references only. Forty current
conflicts, V11's 41 historical conflicts, 158 missing references and the
deleted-cache replay gap remain explicit. The later warm-hint diagnostic and
this baseline proposal are not included in that snapshot. Preservation does
not establish replay completeness, numerical qualification or release readiness.

The unexecuted V3 successor remains permanently held after its producer edits
three paths after announcing hashes. Root preserves the observed changed bytes
and the incomplete pre-mutation history; restoration cannot repair that gap.
No V3 test, worker or fit runs, and the source, seed and prior remain unaffected:
`/private/tmp/attention-matching-main7-three-prompt-successor-v3-immutability-incident-v1/incident.json`
(SHA-256 `234e417f6c24b9570c0a35a2c6de10c2d3f211a4cd21594d3569a5c9b1f28293`).

An initial fallback study remains held because six of its seven source edges
came from the older main checkout. Its immutable V2 successor uses absolute
paths in this compaction worktree; all seven hashes are independently checked.
Packed storage already supports mixed exact/lossy spans and exact source
copies, while the profile, digest and installed contract still require uniform
owner widths and 36 lossy artifacts. The arithmetic proposal is
`512 + 11*325 + 2048 = 6135` owner4 rows: keep one block exact and compress
eleven more. Under explicit 16 KiB page and unchanged geometry assumptions,
it could preserve the uniform-341 allocation. This is a conditional calculation,
not measured savings or quality evidence for 325-row blocks. A coherent mixed
schedule needs versioned identities, separate source-cell/lossy-artifact counts,
exact-source binding, fresh budgets, a new build and full qualification. Study:
`/private/tmp/attention-matching-adaptive-retention-exact-fallback-feasibility-v2.json`
(SHA-256 `04a3d71c779ba0e9f7abd0a0fd442b9047cfc9d91ea0f70c51d3cef7c54c011e`).

Before scaling this recipe, the mass and serial Tikhonov helpers need a coherent
retained-count implementation: they currently accept only 341 rows, while
owners9 and 14 require 288. Falling back to the older unregularized 288-row path
would change the objective. A full-matrix successor must bind a fresh immutable
pooled seed and its own normalized prior for each coordinate, while retaining
the selected `2^-14` coefficient. Owners4/9 pool 1824 queries into 16 primary
groups; owner14 pools 6000 into 80 primary groups. The larger owner14 case needs
its own bounded feasibility run before a 36-cell campaign. The campaign should
use one parameterized adapter, seed campaign and serial harness, with exact
cell/source/payload provenance and separate per-cell versus campaign deadlines.
The unchanged one-cell guards and training pass do not establish that larger
geometry's memory, latency or accuracy.

Temporary successors now pass prospective review: the canonical36 pooled
adapter passes eight synthetic checks plus all 36 actual JSON manifest pairs;
the retained-count mass/serial implementation passes ten checks using the
pinned Python runtime. The 288-row path preserves its existing key/support
algorithm and changes only the value objective to the reviewed fixed-seed
regularization. Review catches and closes a hardcoded one-cell seed, a missing
deadline constant, and incomplete source/cell/seed binding. These helpers are
not executable qualification plans. A reviewed worker must derive seed context
from the validated adapter and enforce metadata, seed and plan cell equality
before exposing arrays; real owner9/14 canaries remain necessary.
Adapter review:
`/private/tmp/attention-matching-main7-full36-pooled-adapter-preparation-independent-review-v3.json`
(SHA-256 `564ce024ea40a18c84ddc08a13a992bf1ade191f143090de392f98473275a019`).
Retained recipe review:
`/private/tmp/attention-matching-retained-fixed-seed-tikhonov-temporary-independent-review-v4.json`
(SHA-256 `0151e5497f4056c5dd1308214267ed1d395ec0d397c2031bec1d60f11750bbb8`).
Adapter V2 remains invalidated after a review-time file mutation; its transient
bytes were not separately archived before restoration. V3 uses fresh immutable
paths. This historical gap does not affect the successful alpha0 run's frozen
executable closure or its preserved snapshot.

Further review holds adapter V3's seed-producer approval boundary: recursive
edge matching can mistake archived references for active approval. V4 replaces
that search with direct producer/profile fields, but its dictionary equality
still accepts numeric `1`/`0` in place of JSON booleans. Neither helper is
authorized for a new worker or campaign.

Fresh adapter V5 checks the exact review keyset and each field's type and value,
then validates the review before hashing the seed payload or loading source
metadata. Ten focused standard-library tests pass, including every boolean
field, numeric aliases, malformed values, archived references and positive
controls for rejection before payload access. The predecessor remains unchanged.
The inherited 12-check synthetic NumPy suite does not start: internal disk
headroom is 3,495 MiB against its unchanged 4,096 MiB floor. That admission
rejection is not a test failure or a V5 suite pass. Independent review passes
the narrow provenance fix and separately reruns the ten focused tests. A fresh
producer/seed/worker context and broader verification remain required before
integration. Handoff:
`/private/tmp/attention-matching-main7-full36-pooled-adapter-preparation-handoff-v5.json`
(SHA-256 `4aa4ce3a3544abd3ee4852f204cf6c8111ec9f04beadad7fb90af5f26ce5a5df`).
Independent review:
`/private/tmp/attention-matching-main7-full36-pooled-adapter-independent-review-v5.json`
(SHA-256 `2d30cc946ec57edb4686ec57fabaa804aaa64afe2ec584708f99e490f88b6ea5`).

After authorized idle-cache cleanup restores disk headroom, the inherited V5
suite actually passes all 12 checks: 2.55 seconds, 96.86 MiB combined RSS, zero
swap growth and full process drain. It reads source JSON and synthetic seed
arrays only; no real source tensor or model is loaded. The independent
verification addendum preserves the earlier rejected attempt and records this
later success:
`/private/tmp/attention-matching-main7-full36-pooled-adapter-v5-verification-addendum-v1.json`
(SHA-256 `b22d9af08ecc8a3a059448c8fd9c6d45e6cf2d184a99e9426dd054a3f1c2328f`).
This closes the focused code-verification gap, not full36 integration or
numerical qualification.

The successful regularized fit, postfit analysis and frozen selection are now
preserved with their temporary helper/configuration closure in
`/Volumes/Extreme/antfly/qualification/attention-matching-main7-replay-metadata-snapshot-20261022-v5`.
Its manifest SHA-256 is
`08429857127c7a8f4753faf4a55b60ae9e528af42c177e18fc7c466f31d9e916`:
5455 files, 104388046 bytes, all independently rehashed. The bounded copy takes
3.38 seconds at 97 MiB with zero swap growth and full drain. Large tensor and
binary artifacts remain exact durable references. The same five historical,
inactive conflicts remain recorded; broad replay completeness and qualification
remain false.

Main6 passes the full native build, compiled budget
probe, fresh training capture, all 36 native numerical comparisons, the
artifact comparison, a fresh audited held-prompt capture and its measured native
baseline. The corrected candidate also passes exact compiled budget admission
for all 36 artifacts without loading a model or consuming a held tuple. Fresh
installed held-out accuracy and measured 25% context
memory savings have not yet passed; serving performance and soak gates also
remain open. The previous Main5 installed one-shot fails with
`InvalidPayloadProvenance` before publication. It completes in 428.34 seconds at
3691.92 MiB combined RSS, with zero swap growth, full process drain and zero
reported leaks. Its reservation is durably aborted; the tuple
`e6e4ed9a4d7d7c7926c59a01dcfc8188fd5317ad6d4c40c760e7990a608f0cbb`
is permanently consumed. No installed numerical or 25% memory-savings pass can
be claimed. Terminal audit binding the native test log:
`/private/tmp/attention-matching-main5-installed-all36-terminal-root-audit-v2.json`
(SHA-256 `bdb2d597429b7d1f331f16d1a6c70df681c34b0b03f135f9387fc37db2afcc1e`).

Source and manifest review identify a definite admission defect: the preparer
sets both materialization and loader budgets to the 23,396,352-byte rounded
K/V/bias buffer total. The native loader additionally budgets its nonempty
`LoadedStoredArtifact` entry array. The coordinator requires the exact native
total and rejects this buffer-only value before constructing the loader. The
successor must use a compiled native budget receipt and check it at manifest
load time, before consuming another held tuple. That Main5 result did not establish
a numerical failure; Main6 subsequently reached numerical validation and rejected.
The historical terminal receipt also omits individual validation
reports, so it cannot supply numerical margins or a failing artifact coordinate.

An independent whole-layout review finds another admission mismatch that would
remain after fixing the budget: the frozen artifact/profile `span` is a lossy
block ordinal (0 through 11), whereas publication compares it to the physical
segment index (1 through 12 after the exact prefix). All 36 artifacts match
uniquely with ordinal interpretation and none with physical-index interpretation.
The repair must preserve the frozen span/profile identities, map them to actual
destination segments, and prove this bijection before reservation. Causal audit:
`/private/tmp/attention-matching-main5-installed-span-domain-causal-audit-v1.json`
(SHA-256 `16f1f67654d0c6c526f1d548320496f0fb344626b4af9482eb52bcb2a96434b4`).
The repository metadata validator `stored_loader_budget.py` now independently
checks the compiled receipt arithmetic, exact input and geometry digests, and
lossy-ordinal-to-destination mapping. Seventeen focused tests pass, including
omitted entry allocation, wrong destination indexing and changed geometry.
Independent metadata review passes:
`/private/tmp/attention-matching-stored-loader-budget-validator-independent-review-v2.json`
(SHA-256 `1edbff87336fca740501d20d7edadc2f7851a08a0354e88a3cce4f04a9692235`).
The real Main5 manifest also passes this metadata mapping check for all 36
coordinates, without reading tensor values or reserving a tuple. These are
metadata checks. The Main6 native repair now passes independent static review
and the full native build. It binds live owner layers, head dimensions and
KV-head counts before reservation, and preserves frozen lossy ordinals while
translating them to physical destination segments. A fresh installed evaluation
remains required.

Predecessor checkpoint (2026-10-21): Main6 selects 570 tests:
526 passed, 44 opt-in skips, zero failures and zero leaks. The unfiltered stripped
build takes 556.89 seconds at 3504.88 MiB peak process-group RSS, with full drain
and 338.56 MiB swap growth within the unchanged 512 MiB build allowance. This is
correctness evidence, not clean-swap runtime or performance evidence. All eight
changed source files match the reviewed contract, live worktree and sealed
7342-file source archive. Build receipt:
`/private/tmp/context-full-release-build-main6-local1/full-release-build.json`
(SHA-256 `7aea733b2cedc3f022004756d0fc68346c1b3030fa8da34c3fedd3f87225c0e8`).
Independent terminal audit:
`/private/tmp/attention-matching-main6-full-build-terminal-independent-audit-v1.json`
(SHA-256 `3775010ed91dee868924b59c774e344ab130cb3196fbd50925af12d86e736693`).

The exact Main5 manifest also passes the new compiled metadata-only probe. Its
36 loader entries occupy 7488 logical bytes and a 16384-byte physical allocation;
adding the 23396352-byte buffer allocation gives a required loader budget of
23412736 bytes. The probe truthfully reports that the historical declared loader
budget does not match, while its materialization budget does. All 36 destination
segments validate. The native test passes in 0.63 seconds at 279.89 MiB combined
RSS with zero swap growth, full drain and an unchanged consumed ledger. This is
historical calculation evidence only and grants no admission or release authority.
Native receipt:
`/private/tmp/attention-matching-main6-stored-budget-native-v1.json`
(SHA-256 `47bb05eceff7e0f4ab71854a76d4384cb03878d9465830e292f23f61611e7918`).
Bound execution audit:
`/private/tmp/attention-matching-main6-stored-budget-probe-audit-v1.json`
(SHA-256 `49db2f6cc0c84d1bf4fd8cc7bcd4652571a255bc4ec060e16f05380b25549a1b`).

The fresh Main6 candidate passes compiled admission with a 23412736-byte loader
budget and a 23396352-byte materialization budget. All 36 destination mappings
validate, the ledger is absent before and after, and the probe has zero swap
growth and full process drain. Candidate manifest:
`/private/tmp/attention-matching-main6-native-budget-candidate-v2/prospective-installed-manifest-v2.json`
(SHA-256 `af4d7c62379679aef63d96f98d5ce91c9ea70274d85a478d158f0f2341a648e6`).
Native receipt:
`/private/tmp/attention-matching-main6-stored-budget-native-admission-v2.json`
(SHA-256 `4d1d598f7fcff15bad3badf90004e71dfc863624b1238510ee21efe1e28bf608`).
Probe audit:
`/private/tmp/attention-matching-main6-stored-budget-native-admission-audit-v1.json`
(SHA-256 `0bb3d6e4110cd92aac182bbb47442779422b8db14816520d82b719051518fa88`).
The previous candidate conversion failed before output creation because it
omitted the audited prompt length before layout validation. Preparer V4 now
copies that length before validating either the candidate or final manifest;
four real-geometry regression tests pass. The failed attempt is preserved,
and the native binary, frozen fit and numerical thresholds are unchanged.
This admission check establishes loader accounting only, not the final context
memory reduction or held-out accuracy.

The first Main6 fresh training-capture attempt stops before model load under the
unchanged live-memory admission policy: 4186895320 requested bytes exceed the
3693671874-byte capacity after the 6442450944-byte reserve, a 493223446-byte
(470.37 MiB) shortfall. The native report is `ResourceTemporarilyUnavailable`;
the collector subsequently reports `invalid coverage driver report`. Coverage
ends after 3.15 seconds at 105.42 MiB combined RSS with zero swap growth and full
drain. No numerical capture, held evaluation or reservation is produced. The
failed attempt remains preserved at
`/Volumes/Extreme/antfly/qualification/attention-matching-main6-training-source-operational-v1.json`.
This environment constraint does not invalidate the completed build and
metadata probe, and it does not supply model-quality evidence.

After RAM was freed, the unchanged Main6 training capture succeeds on fresh V2
paths. The native capture takes 421.86 seconds at 5295.05 MiB combined RSS,
with zero swap growth and full process drain. Independent terminal auditing
verifies all 36 canonical cells and 361 graph files (729717391 bytes), the exact
Main6 binary/source identities, and the preserved failed-attempt lineage.
Final source receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main6-training-source-v2/final-source-receipt.json`
(SHA-256 `5aba8c08f510c4fdb6539b5a53d87f28da234e2129c5de707fad873c3764a441`).
Independent audit:
`/private/tmp/attention-matching-main6-source-terminal-independent-audit-v4.json`
(SHA-256 `9a4a1c792b613c950d6e8e2df31759853f2a35329096ebf126bfdf65ab0ba1f9`).
The audit passes in 3.13 seconds at 381.34 MiB combined RSS with zero swap and
full drain. This establishes fresh training-source provenance only; native
numerical parity, fresh installed held-out validation and measured memory
savings remain separate required gates.

The subsequent Main6 native training comparison passes all 36 cells in 51.15
seconds. All 72 preparation/device guards pass with a maximum 319.91 MiB combined
RSS, zero swap growth and full process drain. Maximum mass error is
0.0499503613 (limit 0.05), aggregate output RRMSE is 0.0994895175 (limit 0.10),
and worst-consumer RRMSE is 0.0999264941 (limit 0.10). Maximum absolute difference
from the FP64 reference is 0.00000231498 (limit 0.00001). These narrow training
margins do not establish held-out accuracy. The frozen Main3 fitted payloads and
the fresh Main6 captures retain distinct provenance. Raw comparison receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main6-native36-training-parity-v2/receipt.json`
(SHA-256 `3390fc67fe6982727476e83e2f614ed728630ed768968e2f4815695a5245b09e`).
Independent terminal auditing also passes, checking the exact native reports,
per-consumer reference counts, source/build ancestry and all 72 guards:
`/private/tmp/attention-matching-main6-native-training-parity-terminal-independent-audit-v2.json`
(SHA-256 `022ae2c4680ea9a8c423675f86add7d43280312c2d1d488af8a8a56325501dbf`).
The 51.15-second diagnostic comparison is not a serving-latency measurement.

The Main6 artifact comparison then passes for all 36 coordinates, confirming
byte-identical raw source K/V and frozen payload members while preserving the
historical fitting provenance. It verifies 456 file dependencies and completes
in 7.36 seconds at 374.02 MiB combined RSS with zero swap and full drain:
`/private/tmp/attention-matching-main6-artifact-parity-bridge-actual-v4.json`
(SHA-256 `9076ed6f98a7291d8eea1e981259ea15256e182c6158d0d0881e032312f7c976`).
This comparison retains all numerical gates and grants no installed, serving
or release authority. Fresh held-out validation and memory measurement remain
required.

The fresh held-question tokenizer check passes in 16.74 seconds at 3310.75 MiB
combined RSS with zero swap growth and full drain. Its independent terminal
audit verifies all six candidates, identical first 6656 prompt tokens and zero
model forward steps. The predeclared first-eligible rule selects candidate 02:
8160 prompt tokens, with its 50-token question at offsets [8105, 8155).
Audit: `/private/tmp/attention-matching-main6-held-tokenizer-terminal-independent-audit-v6.json`
(SHA-256 `6af314760894b85901d17b8ea1c5c3d3ad1a08aa895e3b7b48294e809908fa06`).
This is tokenizer geometry evidence only. The first subsequent shared-capture
launch rejects a mismatched coverage-resource output path before creating its
operational receipt or spawning a native process; no held tuple is consumed.
That orchestration failure is preserved separately from model-quality results.

The corrected V2 shared capture succeeds on the selected held question. Native
capture takes 421.78 seconds at 3632.05 MiB combined RSS with zero swap growth
and full drain. The independent terminal audit also passes, retaining the
complete build, command, resource and 36-cell source-graph checks:
`/private/tmp/attention-matching-main6-fresh-held-source-terminal-independent-audit-v3.json`
(SHA-256 `d300d0230538357f7735a5668e7abb88c82ed881ab16d61cae5a916fa86e0938`).
The separate held-authority seal then passes in 1.21 seconds at 118.91 MiB,
with zero swap and full drain. Source and authority:
`/Volumes/Extreme/antfly/qualification/attention-matching-main6-held-source-v2/final-source-receipt.json`
(SHA-256 `e15e4ce8c04eece0834b63049c4713ac2144cf2fdb99f14b644d624863ef8931`),
`/Volumes/Extreme/antfly/qualification/attention-matching-main6-fresh-held-authority-v1/fresh-held-authority.json`
(SHA-256 `6d4ae29e9aedd34427262a58b4b6e763d146050ac7cdfadf6026f4fd972f1061`).
This establishes fresh held-data provenance, not a numerical acceptance result
or a measured context-memory saving. The frozen payloads have not been refit.

The fresh Main6 native baseline measurement and its independent audit pass.
The run takes 116.79 seconds at 4018.55 MiB combined RSS, with zero swap growth
and full drain. It measures 58035696 physical context bytes and 58035808 tracked
live/peak bytes across three global and twelve sliding owners. The unchanged
25% savings gate therefore requires at most 43526772 physical context bytes
after compaction. No stored manifest or ledger is used for this measurement.
Envelope: `/Volumes/Extreme/antfly/qualification/attention-matching-main6-installed-baseline-preflight-v1.json`
(SHA-256 `d3a2626869af822a6c4ff3c1497ea8ca93610def81085a976790dc7a6d8edf99`).
Audit: `/private/tmp/attention-matching-main6-installed-baseline-terminal-independent-audit-v1.json`
(SHA-256 `6640bca96201dd094dafcaae5f4041dd670ee1f99ebed0d3a4e4f6eb19feb07d`).

The installed diagnostic uses the full qualified 8192-token retained-row limit;
it does not qualify a 6144-token serving cache cap. The frozen owner-4 recipe
retains 512 exact prefix rows, twelve 352-row compacted blocks and 1504 exact
tail rows, totaling 6240 rows before additional decode growth. This exceeds
6144 independently of the physical-memory savings gate. Production admission
and publication must enforce the requested retained-row cap. A successful
installed numerical and memory result therefore still needs an explicit
compatible serving profile and cap qualification; the diagnostic must not be
reported as evidence that this recipe fits the 6144-token serving policy.
The generic `Policy.retainedRows` calculation currently assumes 256 retained
rows per 512-token block, whereas this frozen profile uses 352/288/288 by owner.
Serving admission must account for the selected profile's actual per-owner
retention and maintenance boundary before enabling it. The packed diagnostic
session itself currently requires an exact policy with a full logical-token
retained limit. These are separate serving-integration gates, even if the
installed diagnostic later passes.

Previous fully built checkpoint: Main5 fixes semantic `FileSpec` equality
and adds a parsed-JSON regression plus an opt-in, no-model loader test. The
unfiltered build passes 564 selected tests: 521 passed, 43 opt-in skips, zero
failures and zero leaks. It takes 675.54 seconds at 3925.94 MiB peak process-group
RSS, with full drain and 65.03 MiB swap growth within the unchanged 512 MiB build
allowance. This build is correctness evidence, not performance evidence. Its
source archive differs from Main4 in exactly the caller and explicit test-root
discovery. Build receipt:
`/private/tmp/context-full-release-build-main5-local1/full-release-build.json`
(SHA-256 `1ac9d93055fba5887159441db12c6d60e45b6b47bb171053db4584b4c0b4cdf8`).
Independent terminal audit:
`/private/tmp/attention-matching-main5-full-build-terminal-independent-audit-v3.json`
(SHA-256 `4dc0a6ae4bf91ad90cfb6bacbaef8e60e90bc5b4210e4db27c0e3042de8b6b80`).

The Main5 native load-only test accepts the exact historical Main4 manifest,
opens and releases its immutable artifacts, and leaves the ledger empty. It
passes one selected test in 0.91 seconds at 30.94 MiB combined RSS, with zero
swap and full drain. The outer wrapper subsequently rejects the successful
runner's abbreviated summary because it expected explicit zero-failure/leak
fields; that postcondition-format failure is retained separately from the
native result. Resource evidence:
`/private/tmp/attention-matching-main5-exact-manifest-load-only-v1-resource.json`.
The independent terminal audit accepts the native success while retaining the
wrapper-format failure:
`/private/tmp/attention-matching-main5-exact-manifest-load-only-terminal-independent-audit-v1.json`
(SHA-256 `ca5c96c33cc1b22be3f058d0f245d1659c78dc01dd2455dea512861f6305e4a9`).
This confirms the loader repair without model execution or tuple consumption.
Fresh Main5 numerical, memory, serving and soak evidence remains required.

The fresh Main5 training-source capture completes on the corrected binary in
415.84 seconds at 4103.84 MiB peak combined RSS, with zero swap and full drain.
Its independent terminal audit verifies all 36 cells and 361 graph files
(729,697,735 bytes), including the current binary, build, runtime and source
identities. It supplies training captures only; it is not fitted-payload parity
or a held-out result. Source receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main5-training-source-v1/final-source-receipt.json`
(SHA-256 `e3ec8bc0ae102b4f74b369393c84687014eafa383d5aacf189543178a40486b5`).
Independent audit:
`/private/tmp/attention-matching-main5-source-terminal-independent-audit-v1.json`
(SHA-256 `a878c787a9ada7580754c938463191d7af28bb6349c57d0b511dc0d3c3aae212`).

All 36 fitted payloads also pass native training parity on Main5 against the
fresh captures and FP64 reference. The run takes 48.86 seconds at 297.28 MiB
peak combined RSS, with zero swap and full drain. Maximum mass error is
0.04995036, aggregate output relative RMSE is 0.09948952, worst-consumer relative
RMSE is 0.09992649, and native-to-reference absolute difference is 2.315e-6.
These pass the unchanged 0.05, 0.10, 0.10 and 1e-5 gates. The fitted bytes retain
their historical fitting provenance; this is current-build training validation,
not held-out accuracy, installed savings or serving performance. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main5-native36-training-parity-v1/receipt.json`
(SHA-256 `5fe9fbb04616feabf1d7e43e1250c424cdfe19980a7633f786a5bea32af0b174`).
Independent audit:
`/private/tmp/attention-matching-main5-native-training-parity-terminal-independent-audit-v1.json`
(SHA-256 `2389bf0291851ee92b61ec31b1103c1862b8dc8cc46aaa7f7d4b1ffcea0341b7`).

Main5 tokenizer-only preparation selects a fresh 50-token question in an
8160-token prompt, with the first 6656 tokens byte-identical to the training
prompt. The first attempt finds no exact-length candidate and is preserved as
a failure. A one-word adjustment based only on token counts succeeds in 20.97
seconds at 3336.94 MiB, with zero swap and full drain. No model forward step or
answer was observed. The prior Main4 tuple remains unconsumed; its exposed
question is nevertheless excluded from this successor. Tokenizer audit:
`/private/tmp/attention-matching-main5-held-tokenizer-discovery-terminal-independent-audit-v1.json`
(SHA-256 `7ab3515a159d1c55b73708ae8f27f58c64e0cf4400f057c19456a21bf60c9094`).
The historical-to-current artifact bridge also passes all 36 rows, preserving
distinct fitting and current-build identities:
`/private/tmp/attention-matching-main5-artifact-parity-bridge-actual-v1.json`
(SHA-256 `c80ce5c13245e32b889ccd87e59c33c56a6991b7ee4468418b3980f868c30e70`).
Neither result supplies held-out numerical or installed-memory evidence.

The fresh Main5 held-question source capture subsequently passes in 418.80
seconds at 3659.61 MiB peak combined RSS, with zero swap and full drain. Its
terminal audit verifies 36 cells and 361 graph files totaling 729,697,375 bytes:
`/private/tmp/attention-matching-main5-fresh-held-source-terminal-independent-audit-v1.json`
(SHA-256 `10809ed06a037d44034c420a4e38b4138c390d9b8abcd72a1995cd0c78697b53`).
Source receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main5-fresh-held-source-v1/final-source-receipt.json`
(SHA-256 `72ccbbdcaece8574438904c9a5b506aae1fa6dad1f0f6144fe043b627ade555a`).
This supplies fresh evaluation inputs; it is not a held-out numerical pass.

The Main5 installed baseline measures 58,035,696 physical context bytes for
the fresh 8160-token prompt, with 3 global and 12 sliding owners. The unchanged
25% savings gate therefore requires at most 43,526,772 committed context bytes.
The native measurement passes in 140.93 seconds at 3496.48 MiB combined RSS,
with zero swap and full drain. The V1 wrapper then rejects its digest because
it compares a native 32-byte JSON array against a hexadecimal string. That
wrapper failure is preserved; independent audit accepts the native measurement
without repeating it. Audit:
`/private/tmp/attention-matching-main5-installed-baseline-terminal-independent-audit-v1.json`
(SHA-256 `0c7f8a0de752e56f1f117ca1a4176769a21be0f77b7a71134ed91d8b35dd90de`).
The corrected wrapper is validated against the actual receipt and mismatched
digest fixtures, rather than obtaining a replacement native measurement.

The new Main5 external manifest binds that baseline, the fresh held capture,
the current build and the unchanged fitted bytes. All 129 file specifications
rehash. Its actual no-model loader test passes with an empty ledger before and
after, 33.48 MiB peak combined RSS, zero swap and full drain. Independent review:
`/private/tmp/attention-matching-main5-installed-manifest-load-only-independent-review-v1.json`
(SHA-256 `524ce15f02374d1bc183c20f0b3ab565f4c7a6d9a02816f9ea57bdf454a6dda9`).
This confirms manifest compatibility; installed held accuracy and committed
memory savings require the separate one-shot evaluation. Once reserved, its
held tuple is terminal regardless of the outcome and must never be retried.

Previous qualification blocker: the first Main4 installed run
rejects the external manifest before reservation. It exits in 32.30 seconds at
3355.08 MiB combined RSS, with zero swap and complete drain; no success receipt
exists and the ledger remains empty. The held tuple is unconsumed. Independent
review identifies a native loader defect: `std.meta.eql` compares the slice
pointers in `FileSpec.path`, while strict JSON parsing allocates identical path
strings separately. The loader must compare path bytes, size and SHA instead.
A parsed-JSON regression and a no-model, no-reservation load of the exact
manifest are required before another installed run. This source correction
requires a fresh build and new build-bound evidence; Main4 results below remain
evidence for their original binary. Failure audit:
`/private/tmp/attention-matching-main4-installed-all36-failure-terminal-independent-audit-v1.json`
(SHA-256 `4258772a3a825d7c8c2ac6466066a88c2c6c4c0d868515e985464fb70ead3d0f`).
Causal review:
`/private/tmp/attention-matching-main4-installed-manifest-slice-equality-cause-independent-review-v1.json`
(SHA-256 `fad57b915288302c31befbad13b7e28b3a663a50e7b695ea965ca7fd039cd487`).

Previous source checkpoint (2026-10-21): Main4 includes the independently
reviewed installed-baseline repair and the packed-reducer broadcast optimization.
The baseline comes from the live allocation tracker before a held tuple can be
reserved; global and total owner counts are separate, and failure receipts retain
incomplete outcomes. The strict Main4 build and current-binary synthetic Metal
checks pass; model-level accuracy, physical savings and performance remain open.
The first Main4 build was deliberately stopped after finding that the context
target's name filters excluded 18 stored-compaction regressions. Twenty-one test
names now carry the explicit context prefix (three already matched via their
module name). The second build exposed a synthetic coordinator fixture missing
newly required prompt and measured-baseline fields. That fixture is corrected
without changing runtime checks. The third full build passes: 562 selected,
520 passed, 42 opt-in skips, zero failures and zero leaks. All 18 newly selected
regressions and the repaired fixture actually execute and pass. The run takes
539.47 seconds at 5343.59 MiB peak process-group RSS, with zero swap and complete
drain. The independent strict audit verifies the sealed binary, all 7,340 source
files and the eight frozen canonical changes. Build receipt:
`/private/tmp/context-full-release-build-main4-local3/full-release-build.json`
(SHA-256 `0c6e13208c66da19ea887f2792f43468876b2aead885efff717554ba0d7c70c9`).
Terminal audit:
`/private/tmp/attention-matching-main4-local3-strict-build-terminal-independent-audit-v1.json`
(SHA-256 `1b46415ec2c417d801f16d08c035be399490e6bc4ae89a1fcd3eb4e3b9b88712`).
Main4 source freeze:
`/private/tmp/attention-matching-main4-root-source-freeze-v3.json`
(SHA-256 `a66a9959c214259ad4376906ffbcb5802b123f3887161aec098cf7ccac823c16`).
The new binary also passes all 24 synthetic 8K split-attention combinations
(dimensions 256/512, query rows 1/4/8, splits 4/8/16/32), comparing against serial
attention with FP64 checks. Maximum absolute error is 2.2817403e-8 and maximum
relative RMSE is 1.5147857e-5, within the unchanged 3e-4 and 5e-4 limits. Metal API
validation is enabled. The run takes 7.75 seconds at 177.06 MiB peak RSS, with
zero swap and complete drain. Audit:
`/private/tmp/attention-matching-main4-split-8k-terminal-root-audit-v1.json`
(SHA-256 `5f9fa6b9d384103a3c62f61ec03a1a7cb6454dace7f2ad7689d47063f542ad0d`).

The standalone reducer comparison also passes all 180 cases across operations
5/6, six head dimensions, five split counts and three finite/empty/nonfinite
input patterns, with byte-identical outputs and identical status flags against
the original shader. Metal API validation exposed an undersized unused buffer
in the previous standalone harness; its successor supplies the reflected buffer
size without changing the canonical shader. The corrected run takes 0.66 seconds
at 32.78 MiB sampled peak RSS, with zero swap and complete drain. Audit:
`/private/tmp/attention-matching-main4-packed-reduce-api-validation-v4/terminal-root-audit.json`
(SHA-256 `5afe380a3a862a54e050c652a7c5b073e30d3206459c2356138ca073c7dbf124`).
This is differential kernel continuity, not an independent CPU oracle or a
model/serving performance result; its timings remain diagnostic.

The external Extreme drive was temporarily unavailable, then reconnected with
its expected volume UUID verified. Fresh Main4 E2B/E4B baseline checks pass on
the sealed binary. Both direct packed replay probes have zero logit error,
zero KV readback, no migration/fork copying and released leases. Each model also
passes all eight generation routes. All four probes record zero swap and full
drain. Model, tokenizer/template manifest, binary and source identities are
rehashed. Campaign:
`/Volumes/Extreme/antfly/qualification/context-main4-installed-model-baseline-v1/campaign-terminal.json`
(SHA-256 `d4f5bc055ee7c76df4154be60d30d27a4de86318f2a1d96f3ac8266c6dc47cf0`).
Independent audit:
`/private/tmp/attention-matching-main4-installed-model-baseline-terminal-independent-audit-v1.json`
(SHA-256 `a51ca966b0e6fe61a8d06d37a4ca9cde421bf011d7229d1e8506fd255ecd38db`).
This is baseline evidence only. Fresh source-capture preparation rejected an old
`context_model_test.zig` fingerprint in a transitive helper. An immutable Main4
successor now binds the complete execution chain, including the outer execution
sealer and review-bound runner. Nine focused checks and independent review pass.
Review: `/private/tmp/attention-matching-main4-source-capture-helper-successor-independent-review-v2.json`
(SHA-256 `92f0499806cd0675dcaf0dab1a309be6847b2102cfc3ec5c12204abf25ed84e7`).
No missing evidence has been recreated or relabeled.
The fresh Main4 capture subsequently passes: 446.09 seconds, 3633.83 MiB peak
combined RSS, zero swap and complete drain. Independent terminal audit verifies
all 36 cells and 361 graph files (729,697,735 bytes), retaining the current build,
binary and source identities. Source:
`/Volumes/Extreme/antfly/qualification/attention-matching-main4-training-source-v2/final-source-receipt.json`
(SHA-256 `8963901e59bcfbcf307464698143a9bbf68fa689180561f6088876be628f2451`).
Audit: `/private/tmp/attention-matching-main4-source-terminal-independent-audit-v1.json`
(SHA-256 `b678c19a5500b4f29a1fd411c1e214f795ca69df747bd9c4aa8032e4d80fc638`).
A separate streaming comparison finds all 36 source K/V pairs byte-identical
to their historical Main3 fitting inputs (37,748,736 bytes per source). Receipt:
`/private/tmp/attention-matching-main4-source-kv-byte-comparison-root-v1.json`
(SHA-256 `a0491c64e378a0b4e155962968a2bfb5dc6c1d685826f3e4bb2bb3aa9e5c6071`).
Current-binary native training parity also passes all 36 historical fitted
payloads against these fresh captures. It takes 55.52 seconds, peaks at 287.05 MiB
combined RSS, records zero swap and drains every phase. Maximum device/FP64
discrepancy is 2.31497335e-6 (limit 1e-5); native mass error is at most
0.04995036125, worst-consumer relative RMSE 0.09992649406 and aggregate relative
RMSE 0.09948951751. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main4-native36-training-parity-v1/receipt.json`
(SHA-256 `5bdfd23067599a7386826a4752674e5cb85b9c63e277622d07861b2888dc86af`).
Independent terminal audit:
`/private/tmp/attention-matching-main4-native36-training-parity-terminal-independent-audit-v1.json`
(SHA-256 `c4f75b0a94acee0308593fb5a5ed31d41fcb97bbdf4baaea6a70e4cb8fef9eb7`).
This closes source-byte continuity and current training parity; fresh held
installation remains required for combined numerical and memory qualification.

A genuinely new enrichment/backend question passes native tokenizer discovery:
50 question tokens at [8105,8155), an 8160-token prompt, and exact causal-prefix
continuity through token 6656. Selection uses token geometry only, with zero
model-forward steps or observed answers. The first batch was too short; its
successor only lengthens a question before any model evaluation. The passing
run takes 36.39 seconds at 3369.86 MiB peak RSS, with zero swap and full drain.
Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main4-held-tokenizer-discovery-v2/receipt.json`
(SHA-256 `002ffc84f2ccb74ded11fd403764a87f3e853a6f2a732c8a8a3a12b60c1fa478`).
Independent audit:
`/private/tmp/attention-matching-main4-held-tokenizer-discovery-terminal-independent-audit-v2.json`
(SHA-256 `4b4b092b718c9f9a43063c21ebc18a0cae9aaf782220bbf7074ff6caac657795`).
This permits preparing a fixed-question capture; it does not establish held
accuracy or authorize a fitted production profile.
The fixed-question Main4 capture subsequently completes in 427.11 seconds at
4778.25 MiB peak combined RSS, with zero swap and full drain. Its independent
audit verifies all 36 cells and 361 graph files (729,697,375 bytes), the exact
question and tokenizer geometry, and the current build/runtime/model closure.
Source:
`/Volumes/Extreme/antfly/qualification/attention-matching-main4-fresh-held-source-v1/final-source-receipt.json`
(SHA-256 `ede3d5b833f009809e2ab24886006538a779b43cd6ff1cc0f3bb71e20cfb2b57`).
Audit:
`/private/tmp/attention-matching-main4-fresh-held-source-terminal-independent-audit-v1.json`
(SHA-256 `6a40a3dba4c1062cc770fe186b09d9c875e72df2c3eb449e030dfb5210b5b963`).
The generic collector still marks this intermediate `held=false`; it does not
replace training authority or permit fitting on the held question. Separate
held sealing and installed numerical/memory measurement remain required.
The real-graph provenance bridge also passes independent review: all 36 source
K/V pairs are byte-identical, while historical Phase-A payload provenance remains
distinct from current Main4 capture and native parity evidence. Bridge:
`/private/tmp/attention-matching-main4-artifact-parity-bridge-v9-test-output.json`
(SHA-256 `d98202687dbb996f096c393058929ac5b826d93c9f7ab4661d1220109d40ec85`).
Review:
`/private/tmp/attention-matching-main4-artifact-parity-bridge-independent-review-v9.json`
(SHA-256 `78ae8c145466a877fa9da3dbc33cf6ce1e7bce80b2f79a4d6e2caffa829c3c6f`).
The native baseline measurement completes on this same fresh prompt in 120.10
seconds, at 3859.27 MiB peak combined RSS, with zero swap and full drain. It
reports 58,035,696 physical context bytes, 58,035,808 tracked live/peak bytes,
zero replay control bytes, three global owners and fifteen total physical owners
(twelve sliding owners). Thirty-six is the fitted block count, not the physical
owner count. The preliminary launcher postcondition correctly rejects its stale
expectation of 36 owners after the native receipt is written; installation stays
blocked until the metadata contract is corrected and independently verified.
No provider reservation or held-tuple ledger is touched by baseline mode.
Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main4-installed-baseline-preflight-v1.json`.
Independent terminal audit accepts the native measurement and holds the installed
configuration until its owner-count contract is corrected:
`/private/tmp/attention-matching-main4-installed-baseline-terminal-independent-audit-v1.json`
(SHA-256 `4a4b218d3e5f7959fde307b5b645d19171f9b1121876bfff6b850dd3b26b4653`).
The measured 25% reduction target is at most 43,526,772 physical context bytes;
post-installation savings have not yet been measured. Earlier launch attempts
did not run the native test: the guard is a Python library, not a CLI. The actual
API wrapper has behavioral tests that require one guarded launch and both
resource and native receipts, rejecting silent no-op completion.
The corrected metadata preparation then passes on the exact graph. Its concrete
manifest binds 108 unchanged K/V/bias files, 20 evidence copies, the measured
58,035,696-byte baseline, and the verified 3-global/15-total owner layout. The
training and fresh-held sources remain distinct. The fresh tuple is
`f03244a9ab6145f4b26f97699e9a74c06c338d8173dfe385c6b1b3ce7b4e5a14`,
and its ledger is empty at preparation audit. Manifest:
`/Volumes/Extreme/antfly/qualification/attention-matching-main4-fresh-held-installed-binder-v1/installed-external-manifest-v2.json`
(SHA-256 `381c522b0f28988da461838d661f1870952063570cea1f5f2a7c448abe4bedf0`).
Terminal preparation audit:
`/private/tmp/attention-matching-main4-installed-manifest-preparation-terminal-independent-audit-v1.json`
(SHA-256 `c6b046dd5e00c4a2051a5a46addf6d357046207788098728068e9fee549d0415`).
Preparation takes 1.98 seconds at 281.73 MiB combined RSS with zero swap/full
drain. This is not installed numerical or memory-savings evidence.
These source changes require fresh build/runtime-bound evidence. Main3 results
below remain historical. Actual installed 25% context savings and production
qualification are not established.

Most recent completed numerical campaign (Main3, 2026-10-21) passes all 36
original-seed training cases with the same frozen recipe. Separate-process
stored-payload validation passes for every case. Across the campaign, maximum
mass error is 0.04994809379, worst-consumer relative RMSE is 0.09992643804,
maximum aggregate relative RMSE is 0.09948953547, and maximum anchor drift is
0.22574955668. The uninterrupted campaign takes 2569.48 seconds (42.82 minutes),
peaks at 1002.00 MiB combined fitting RSS, records zero swap growth and drains
every worker. The unchanged fitting cap is 1024 MiB: observed headroom is
22 MiB, not a guarantee for other inputs. The independent terminal audit
passes all 36 source/seed/payload and worker-origin checks, and scientific
runtime provenance matches before and after execution.

Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-full36-confirmed-parent-lifetime-v1/campaign-receipt.json`
(SHA-256 `0b6ed0b50617f917a7106a503f048e2af2a7d980001a86a900c26a7ec6e36ef6`).
Terminal audit:
`/private/tmp/attention-matching-main3-full36-confirmed-parent-terminal-independent-audit-v2.json`
(SHA-256 `9a8349370abe821984f9f4e622e1cf73f9a8340b9b5536de3e4f57b7387cde3f`).
This closes the complete training-fit milestone for this artifact and recipe.

All 36 stored payloads also pass native Metal training parity on the unchanged
Main3 binary. The largest absolute device/FP64 discrepancy is 2.314974e-6
(limit 1e-5); maximum native worst-consumer relative RMSE is 0.09992649407,
aggregate relative RMSE is 0.09948951751 and mass error is 0.04995036126.
The campaign takes 43.36 seconds at 180.14 MiB peak combined RSS, with zero
swap and complete drain. Runtime provenance and the independent terminal audit
pass, including all 8/8/40 consumer groups for owners 4/9/14. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-confirmed-parent-training-parity-v1/receipt.json`
(SHA-256 `573d6374dcd3d42094a034fffdcb9f201447def1372fa6f63b8c827ccf6adb10`).
Audit:
`/private/tmp/attention-matching-main3-confirmed-parent-native-training-parity-terminal-independent-audit-v2.json`
(SHA-256 `de04add1d046dd846d15cb34b4fd8802780a0dcf63ae338f8572787b53566a88`).
This validates the stored payloads on Metal, not the native fitter algorithm.
The reviewed normalizer and Phase-A exporter subsequently complete for all
36 payloads with exact source/recipe/parity ancestry, zero swap and complete
drain. Export receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-confirmed-parent-stored-phase-a-v1/export-receipt.json`
(SHA-256 `7c79f547a3b6613541a6bf029acd3ca52ea8fabac190671a9fc2c25f88045d6e`).
The artifact manifest digest is
`800f8320baee317cb920a7623f4fb31335a4efdba4a5e3aec62a89e135a69cf8`;
the profile digest is
`8b62f44c7413a7fceb6eb24241a0377cb45b895add547329d7b3726d7d595f5a`.
The exported bundle grants no held, installed, serving or release authority.
An independent export audit verifies all 108 K/V/bias files byte-for-byte
against their source NPZ members, including dtype, shape and C-order headers:
`/private/tmp/attention-matching-main3-confirmed-parent-phase-a-export-terminal-independent-audit-v1.json`
(SHA-256 `b17535d8cb5b3fa61b2de87935f8c16c58a0f9ef65347edf605101f1b00f182c`).
Fresh held inputs and runtime measurement of physical context bytes remain
separate requirements; a predicted baseline is not a memory-savings result.
Native tokenizer discovery now fixes a fresh question at exactly 50 tokens
(positions 8105 through 8154), with an 8160-token prompt and byte-identical
training prefix through token 6656. Selection uses token geometry only: both
candidate reports record zero model-forward steps and no observed answers.
The guarded run takes 28.91 seconds, peaks at 3334.89 MiB process-group RSS,
records zero swap growth and drains completely. Discovery receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-held-tokenizer-discovery-v8/receipt.json`
(SHA-256 `6f0d1ada1ec4f819c6d22fe5c44edd017b2d8998e879bb546e4f514189bda0f2`).
Independent audit:
`/private/tmp/attention-matching-main3-held-tokenizer-discovery-terminal-independent-audit-v8.json`
(SHA-256 `a4054df9bafb04d1ae8a9a48e506c67476246ef5f71e7ef607d9c7f6623a50ad`).
This establishes the fresh question and unchanged causal prefix, not held-out
numerical quality. The subsequent native capture and independent terminal
audit pass: all 36 cells and 361 graph files (729,697,375 bytes) are rehashed.
The native query capture takes 436.37 seconds at 4344.78 MiB peak combined RSS,
with zero swap and complete process drain; the full collection and sealing
sequence takes 588.24 seconds. Fresh source receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-fresh-held-source-v1/final-source-receipt.json`
(SHA-256 `af5325a3f2d2eafb27846fc4e783eda1059c08e9900a2234f7b69e2e50539842`).
Independent audit:
`/private/tmp/attention-matching-main3-fresh-held-source-terminal-independent-audit-v1.json`
(SHA-256 `c48901ebabec635f2851727af2016172d0c0a5979f44210d5069b0ea925dfd02`).
This is a fresh-held intermediate only. The shared collector's legacy
`training_source_authority` field does not replace the frozen training source
or authorize fitting. Held sealing subsequently passes after correcting
owner-relative token and descriptor paths. Independent audit verifies exact
K/V bytes for all 36 blocks and distinct fresh question and generated responses:
`/private/tmp/attention-matching-main3-fresh-held-authority-terminal-independent-audit-v2.json`
(SHA-256 `2c27bf606e3f4536a810627bba963c86a50d45ad20d8d16bd2001dc7a9f2bd22`).
Preparation also passes after deriving the 8160-token prompt plus 32-token
reserve from the hash-bound native coverage receipt. The complete binder run
takes 0.86 seconds at 97.31 MiB combined RSS, with zero swap and complete drain:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-fresh-held-installed-binder-v3/preparation-result.json`.
The installed V2 run subsequently rejects `InvalidPayloadProvenance` after
durable reservation and before precommit: 115.60 seconds, 3582.98 MiB peak RSS,
zero swap, complete drain and no leaks. Its ledger records an abort and retains
the tuple claim. This held tuple is consumed and must not be retried. The
success-only external receipt is absent; that absence does not establish an
unused tuple. Independent terminal audit:
`/private/tmp/attention-matching-main3-installed-all36-terminal-independent-audit-v2.json`
(SHA-256 `35f15888b09065c885f98a5acaaca712597a33e6dd522d051185f03012935ba2`).
Installed numerical evaluation and actual physical savings remain unqualified;
the owner-count rejection is now explained: the runtime reports 3 global plus
33 sliding-window owners, while the manifest expects only the 3 global owners.
The predicted byte baseline also uses planned storage accounting, whereas the
runtime requires the full allocation-tracker delta. The exact runtime byte
baseline was not persisted, so a byte mismatch is not independently measured.
The Main4 repair distinguishes global and total owners, binds a measured baseline
before reservation, and persists incomplete failure outcomes. Its compiled
regressions pass in Main4; installed validation is pending. The consumed tuple
remains unavailable for retry.
The post-capture preparation helper now validates the exact native child,
capture command, source identities and resource receipts, and recomputes the
coverage memory summary from its raw sampled evidence. Seven focused checks
pass, including rejection of another capture's resource receipt and altered
summary counters. The helper grants no serving or release authority.
The installed diagnostic checks the baseline identity before publication,
then measures the 25% physical-reduction target after atomic publication and
ledger commit. A failure at that point is `committed_but_receipt_failed`; it
cannot be reported as rollback, successful qualification or serving authority.

The completed fit/parity audits and immutable helper closure are preserved in
`/Volumes/Extreme/antfly/qualification/context-main3-full36-training-success-20261021/manifest.json`
(2187 files, 38,236,913 bytes; SHA-256
`d95c3621f527fb9df501688bf936bdbf5ffa6099a011678a7618f1db7fd00c67`).
The subsequent export and pure-Zig test evidence is also preserved in
`/Volumes/Extreme/antfly/qualification/context-main3-export-success-20261021/manifest.json`
(2227 files, 38,485,912 bytes; SHA-256
`9e8203e903e7ba6484041ae3856f68915f05676b2648e447439ac3273b4d83db`).
The fresh-held capture, installed abort, timing diagnostics and their source
closure are preserved in a new additive snapshot:
`/Volumes/Extreme/antfly/qualification/context-main3-fresh-held-installed-failure-20261021-v1/manifest.json`
(7664 files; SHA-256
`9d6a8172d32bb428b21fd1e6cda033229bf2bdf7fe6d442f1124d372bd4056f9`).
Previous snapshots remain unchanged.
New additive Main4 snapshots preserve the successful build/source/kernel
evidence (184 files, 257,315,365 bytes) and the separate native HiGHS restricted
fixture graph (135 files, 20,013,646 bytes). Both bounded copies and independent
destination rehashes pass with zero swap. Manifests:
`/Volumes/Extreme/antfly/qualification/context-main4-build-source-kernel-snapshot-20261021-v1/manifest.json`
(SHA-256 `405bb46e6bbe1b569f2343f0626357902dbe1a313d9f3299aaa3ddda42989d30`) and
`/Volumes/Extreme/antfly/qualification/context-main4-native-highs-restricted-fixture-snapshot-20261021-v1/manifest.json`
(SHA-256 `099f1ac6adca1bc5f45065cbaf5513c536cc8187a47d357e3c8f2633f0e81054`).
These are preservation snapshots, not release authority; later capture and
parity evidence remains separate.

Independent held evaluation, actual committed-context savings, native fitter
equivalence, serving performance and soak remain open.
The 42.82-minute reference fit is not acceptable serving latency. Public
attention matching remains disabled and the release registry remains empty.

An isolated draft of the native FP64 mass/support problem assembly and witness
audits compiles with pinned Zig 0.17 and passes all nine ReleaseSafe tests in
9.46 seconds, at 450.73 MiB process-group peak RSS with zero swap and full drain.
It also passes eight groups of compiled synthetic comparisons against the
frozen Python implementation, including coefficient/bound bits, signed zero,
KKT dispositions, overflow rejection and stored-log rounding. That comparison
takes 9.53 seconds at 451.97 MiB peak with zero swap and complete drain; both
Zig modules use ReleaseSafe and all five numerical thread limits equal one.
Terminal audit:
`/private/tmp/attention-matching-native-mass-lp-v4-synthetic-differential-terminal-audit-v1.json`
(SHA-256 `09d0e25faf5d73493d1cc763a4c44aaafecdda5892263b31049c2be3b040cd82`).
The draft remains unapplied and has no solver or serving hook. Solver and
constraint-generation integration, model-data parity and cancellation/deadline
qualification remain required. Tiny positive support references whose ratio
overflows are conservatively rejected by Zig; current cluster-size references
do not exercise that domain. This is not complete native fitter parity.

The isolated solver-neutral constraint-generation controller also compiles
and passes all 12 ReleaseSafe tests in 11.06 seconds at 407.08 MiB combined RSS,
with zero swap and complete drain. Tests cover two-round constraint addition,
full-dual zero-fill, the single tighter witness retry, cancellation, deadlines,
and result cleanup. Terminal audit:
`/private/tmp/attention-matching-native-mass-cg-v4-compiled-tests-terminal-audit-v1.json`
(SHA-256 `5d8cd602fa5f7391d82ee8126810eaf9e16dded623d8ca196ecaa73a0e61eb3f`).
The successor adds allocation-failure sweeps for the accepted two-round path
and the rejected two-attempt witness retry. All 14 tests pass in 8.68 seconds
at 408.61 MiB combined RSS, with zero swap and complete drain:
`/private/tmp/attention-matching-native-mass-cg-v5-compiled-tests-terminal-audit-root-v1.json`
(SHA-256 `faddb16f6f90a0b5ca6ece1ace2e1bd71ca35efa2b7638586020f392d2daad8b`).
These controller tests use an injected synthetic solver. Full constraint
generation with a native optimizer and serving integration remain unverified.
The earlier isolated HiGHS adapter draft passes all seven injected-API ReleaseSafe
tests: 16.39 seconds, 407.23 MiB combined RSS, zero swap and full drain. These
cover row-dual signs, callback cancellation, controller deadline mapping and
allocation failures. At that checkpoint no HiGHS library was linked. Receipt:
`/private/tmp/attention-matching-native-highs-adapter-v3-fixture-tests-terminal-receipt-v1.json`
(SHA-256 `8a5667f00dcbaa3d5b6d05f8cf3071cc5809b28bd834a23bdb3524f54bcd060e`).

The pinned HiGHS v1.8.0 static library and standalone Zig driver now build and
link successfully. Actual C API execution exposed an adapter mistake: HiGHS
returns IEEE positive infinity, which the draft rejected. Adapter V4 accepts
that sentinel, retains invalid-value rejection and passes eight ReleaseSafe
tests, including exact bound signs, callback/deadline cleanup and allocation
failures. It does not change numerical acceptance thresholds. Tests/link audit:
`/private/tmp/attention-matching-highs-v180-adapter-v4-tests-driver-v5-link-terminal-independent-audit-v1.json`
(SHA-256 `3ed6a97d39911c2177b770e11cbe135273554a619967420399c1b09cbd1f6ac1`).

The real standalone driver then passes the frozen 1128-by-288 restricted LP
regression. At 1e-9, all original weights and alpha/beta vectors are byte-equal
to the saved Python result; objective 0.10230579205433875 and 1424 iterations
match, and the witness rejects as expected. Exactly one fresh 1e-10 retry passes
the witness, with matching objective 0.10230579202447294 and 1451 iterations.
Both solver instances are destroyed. The run takes 2.65 seconds at 111.20 MiB
peak RSS, with zero swap and complete drain. Result:
`/private/tmp/attention-matching-highs-v180-real-fixture-v5/result.json`
(SHA-256 `dd50f2893e0a4b8c1044afe212ae989feeb3ca18f84c37b4b991fc36108af9f8`).
The historical retry has no saved vector oracle; only its scalar metrics and
witness can be compared. This proves one intermediate restricted LP, not full
constraint generation, fitted-payload equivalence, serving latency or release.
Its approximately 0.102 mass error is an intermediate LP result, not a passing
compaction-profile mass error.
Independent terminal audit rehashes the complete fixture and output vectors
and recomputes the primal constraint violations: approximately 1.21256e-6 for
the rejected first attempt and 1.28787e-10 for the passing retry. Audit:
`/private/tmp/attention-matching-highs-v180-real-fixture-v5-terminal-independent-audit-v1.json`
(SHA-256 `203039a1d9f688683703881a95fcf7816d26185c01817712bdd442bcb0b9455d`).

The first full constraint-generation comparison now executes on the frozen
3000-by-288 original-seed matrix for owner 14/block 2048. It takes 3.38 seconds
at 226.78 MiB combined RSS, with zero swap and full drain, but remains a failed
strict comparison: six intermediate worst-violation scalars differ by
2.22e-16 to 1.78e-15. Solver trajectory, final selected rows and final float32
bias bytes agree; terminal raw weights and alpha/beta vectors are byte-identical.
Failure audit:
`/private/tmp/attention-matching-native-full-cg-rounding-failure-audit-v1.json`
(SHA-256 `37df5cfe59ffffb1206e86bdf076de0d24718bd3f249d4774aa505f3b4411442`).
A separate successor is being tested to require exact per-attempt solver vectors
and prove that reduction rounding lies below both threshold and selection-cutoff
margins. The failed result is preserved and has no equivalence or release
authority. This remains one seed-matrix diagnostic, not the full fitter, a
passing compaction profile or a serving performance result.
The separately reviewed V6 successor subsequently passes its two actual Zig
comparator tests and the full six-round diagnostic. Each restricted matrix and
per-attempt raw-weight/alpha/beta vector, status, iteration count, objective,
tolerance and retry branch matches the Python oracle exactly. Final selected
rows and float32 bias bytes match exactly. Only the per-round worst-violation
scalar may differ under a derived binary64 reduction bound, requiring verified
nonnegative finite inputs and margins that prove threshold/cutoff stability.
Strict binary64 equality remains false and visible in the result. The native
run takes 3.94 seconds at 226.09 MiB combined RSS with zero swap and full drain;
all six solver instances are destroyed. Independent streaming audit recomputes
the matrix/vector hashes and the rounding bounds:
`/private/tmp/attention-matching-native-full-cg-bounded-terminal-audit-v1.json`
(SHA-256 `5f9260b5558ce2160e36b245c09aae61a827dbe984a90f14617b952833d2a944`).
The proof is limited to this one original-seed mass matrix. It does not establish
full-fitter equivalence, a speedup, model quality or serving qualification.

A portable native mass-CG worker now has compiled tests for the three required
matrix geometries, warm hints, bounded streaming provenance checks, exact link
arguments, cancellation and owned results. V3 exposed Zig 0.17's EOF error
semantics; corrected V4 passes all eight tests in 10.01 seconds at 411.89 MiB
with zero swap and full drain. Its actual executable link then catches an
optional-slice error-union conversion in the warm-input reader. That failure
remains open for the successor; unit tests alone do not prove the executable
works. Resource records:
`/private/tmp/attention-matching-native-dynamic-cg-v4-test-resource.json` and
`/private/tmp/attention-matching-native-dynamic-cg-v4-link-resource.json`.
The separate CG V6 controller records exact ordered row identities for Python
integration without changing selection or solver policy. All sixteen compiled
tests pass in 9.65 seconds at 407.92 MiB, with zero swap and full drain:
`/private/tmp/attention-matching-native-cg-v6-row-trace-test-resource-v1.json`.
The dynamic worker, Python integration, full fitted-payload equivalence and
performance still need actual end-to-end qualification.

The V5 dynamic worker fixes the warm-input conversion and adds executable-main
reachability coverage. All ten compiled tests pass in 11.47 seconds at
496.02 MiB, with zero swap and full drain:
`/private/tmp/attention-matching-native-dynamic-cg-v5-test-resource.json`.
Independent review then catches scientific and process-drain defects in the
Python adapter. The immutable V3 adapter successor restores the frozen 1e-9
weight bound, checks distinct solver objective and primal `t`, derives its mass
comparison bound from reduction dimension, and unconditionally reaps a killed
child. CG V7 and driver V6 carry the existing `t` through their result without
changing solver policy. Static review and ten Python behavioral tests pass;
the subsequent driver V6 run passes ten actual Zig tests in 14.90 seconds at
497.92 MiB, and its actual link passes in 5.23 seconds at 295.59 MiB. A separate
CG V7 controller run passes sixteen tests in 10.54 seconds at 409.88 MiB. All
three runs have zero swap and full drain. The linked binary is arm64 Mach-O
with only libSystem as an external dynamic dependency. Resources:
`/private/tmp/attention-matching-native-dynamic-cg-v6-test-resource.json`,
`/private/tmp/attention-matching-native-dynamic-cg-v6-link-resource.json`, and
`/private/tmp/attention-matching-native-cg-v7-row-trace-t-test-resource-v1.json`.
The real native-adapter equivalence call subsequently passes on the frozen
owner-14/block-2048 original-seed fixture: all six rounds preserve exact control
fields and row decisions, and the returned Python arrays match the reference
byte-for-byte. Per-round worst-violation differences satisfy the previously
reviewed reduction bound and decision margins. The guarded call takes 4.74
seconds at 272.09 MiB, with zero swap and a fully reaped worker in the guard's
process group. Result:
`/private/tmp/attention-matching-native-dynamic-cg-adapter-equivalence-v5.json`
(SHA-256 `2b8bb755eac88a7cc5dccea2289ac253ebd0215ba0c0a00b0b5096aac256cee8`).
This establishes one real adapter boundary; three-profile warm/retry/rollback
coverage, full fitting and serving integration remain. No speedup is established.
Review:
`/private/tmp/attention-matching-native-dynamic-cg-row-trace-independent-review-v3.json`
(SHA-256 `71d2447f4c99d49e4554df8148d397afcd5031ad5c7dd434a1c336811d73bd2a`).

Partial timing of the hardest training case completes mass fitting in 164.28
seconds. Its 19 workers spend 147.49 seconds in 71 HiGHS calls and 5.37 seconds
in imports/startup. Worker time is contained in phase time, not additive.
This indicates that removing Python/process overhead alone cannot deliver the
required fitting speedup. The following output phase fails an instrumentation
origin-identity check, so the run does not establish complete fitting parity
or end-to-end performance. Partial evidence:
`/private/tmp/attention-matching-main3-timing-origins-v2-partial-mass-profile-v1.json`
(SHA-256 `3cbf837e03b0848ef40a3de2690bf1adbe7348a9b8e3ac188f0e40a1e252fbf8`).

After repairing the instrumentation origin identity, a subsequent full timing
attempt reaches the unchanged 560-second deadline and restores the exact
original-seed K/V/bias member bytes. Independent audit confirms zero swap,
complete worker drain, unchanged input/runtime pins and a maximum phase RSS of
1017.28 MiB under the 1024 MiB limit. It is a nonqualifying terminal result:
19 mass workers and 34 of 39 expected bias workers completed, so it establishes
neither full fitted-payload parity nor end-to-end performance. Other CPU-heavy
processes were active; timings are not a controlled comparison. Terminal audit:
`/private/tmp/attention-matching-main3-o14-b6144-timing-recorder-v2-terminal-audit-v1.json`
(SHA-256 `9b053c0c6e5468c830c082946fdb34c64bb369d8b72917ccf2dc9cfbeea75c7b`).

Earlier single-case result (2026-10-21): owner 14/block 6144 passes a fresh
original-seed run with confirmed proposal ordering and reduced temporary
lifetimes. Separate-process validation reports worst-consumer relative RMSE
0.09992643804, aggregate relative RMSE 0.09793386909, mass error 0.04994803721,
and maximum anchor drift 0.11076738567. The full fit takes 365.11 seconds;
peak combined RSS is 1006.06 MiB, with zero swap growth and complete worker
drain. All three numerical disposition flags are true. The runtime provenance
comparison passes before and after execution. This is one training case;
the revised recipe still requires a fresh all36 campaign, including its
coordinator memory overhead, before native parity or held evaluation.
Evidence:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-o14-b6144-confirmed-parent-lifetime-v6/owner14-block6144/campaign-receipt.json`
(SHA-256 `bd6658c04d411be6ac10f3e02acb3d763525adc182c8cf03c7d21c93fba9dd6f`).
The payload member digest is
`0d2521d5a28b4ca0a9467d98c483b031f97f64c0d8e52f3466fcd2e64bd80d44`.
The terminal independent audit passes, including all 75 trace events, worker
origins, the confirmed ordering policy and runtime identity:
`/private/tmp/attention-matching-main3-o14-b6144-confirmed-parent-lifetime-terminal-independent-audit-v1.json`
(SHA-256 `1c185a25c8d5dcc5389e60a0e1dd8cb68d1f885be3157f3728624753a2fa123c`).
Production qualification, native recipe parity and public serving remain open.

Earlier checkpoint (2026-10-21): the current Main3 build passes 502 tests with
42 opt-in skips, both installed Gemma4 exact-generation baselines pass, and a
fresh 36-cell E2B training-source capture is sealed. Fresh seeds also complete
all 36 cells, and the corrected hardest-case pilot passes its stored-payload
numerical and fitting-memory gates. These runs have zero swap growth and clean
teardown. The corrected full fitting campaign passes 27 cells, then stops at
owner 14/block 2048 after the LP primal/dual witness rejects a candidate. The
failed cell restores its original payload, without swap or a resource breach.
A fresh diagnostic with a bounded, tighter solver retry now completes that
cell's mass phase in 94.54 seconds at 789.16 MiB peak combined RSS, with zero
swap growth and complete worker drain. Stored mass error is 0.04994802, within
the unchanged 0.04995 fitting gate. Its provisional worst-consumer output error
is 0.10462555, so output refinement and separate-process validation are still
required; the diagnostic saves no accepted payload. The retry changes solver
precision only after the exact primal/dual-witness rejection and preserves the
original acceptance checks and shared deadline. Evidence:
`/private/tmp/attention-matching-main3-o14-b2048-witness-retry-mass-probe-v1/diagnostic.json`
(SHA-256 `7060e8a3a72af1616e29e88ad5261148b9bf086d111b8b0d0ad5de6f3afdf8db`);
resource receipt SHA-256
`c39692b1cd1279a20a9b412ac1882a8f487bbe57d25bc363ff6deef2f78642dd`.
The subsequent complete fresh retry run also passes output refinement and
separate-process stored-payload validation: mass error 0.04994801799,
worst-consumer relative RMSE 0.09676715792, aggregate relative RMSE
0.08911799849, and maximum anchor drift 0.10781744903. Fitting takes
98.92 seconds; peak combined RSS across its guarded phases is 927.23 MiB,
with zero swap growth and complete drain. Telemetry records one successful
tighter retry. The stored payload digest is
`85a309424a4fa6b71eeeba84ac32fd9081effb1edd82b46967c8f8ec8b3308ab`.
Its terminal receipt is
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-o14-b2048-witness-retry-fullcell-v3/owner14-block2048/campaign-receipt.json`
(SHA-256 `d3300a45440f5cd8e8965c926c816f2ab32f557cb53bfc3262c93a8d916c5450`).
Seven additional fresh cells, owner 14/blocks 2560 through 5632, pass
the revised recipe's numerical, deadline, memory and process-drain gates.
The hardest case (block 4096) returns the same stored payload as the earlier
successful pilot: worst-consumer relative RMSE 0.09916792079, aggregate
0.09568456753, mass error 0.04994804408, and drift 0.16231071192. Its fresh
run takes 259.65 seconds at 962.73 MiB peak combined RSS with zero swap.
These eight fresh cells do not constitute a full 36-cell campaign, and must
not be stitched to the 27-cell prefix from the earlier recipe.

The final suffix diagnostic, owner 14/block 6144, fails its unchanged output
resource guard at 1040.97 MiB versus the 1024 MiB limit. Source and mass phases
pass; mass takes 209.42 seconds at 907.45 MiB. Output refinement is terminated
after 207.00 seconds with zero swap growth and complete process drain. The
last accepted internal refinement step still has worst-consumer relative RMSE
0.10346157, above 0.10. No accepted payload, result or validation is published;
the original seed remains intact. Failure audit:
`/private/tmp/attention-matching-main3-witness-retry-suffix-cell35-terminal-failure-independent-audit-v1.json`
(SHA-256 `385770c8e0eb5326e492e0b18db20e71da0d28dfdff6f3dcc54ec956652216c7`).
The full campaign remains inactive. A proposed in-place LP constraint builder
preserves exact matrix bytes, including signed zero, while removing dense
temporary arrays. Its benefit requires a fresh original-seed run under the
same numerical, memory and global deadline gates.

The fresh in-place LP diagnostic subsequently completes all four guarded
processes with zero swap and complete drain. Its output peak is 1019.39 MiB,
below the 1024 MiB cap but with only 4.61 MiB headroom. It remains numerically
nonqualifying: the last internal accepted step reaches worst-consumer relative
RMSE 0.10294692, then the 560-second global deadline expires. The returned
payload is restored to the original seed; all three numerical disposition
flags are false. Operational completion is not numerical qualification.
Evidence is under
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-o14-b6144-bias-memory-v4/owner14-block6144`.
The scientific runtime matches before and after execution, covering 1370
wheel-recorded Python sources, 136 bundled native libraries, and the Python
executable/framework; OS-provided dynamic libraries are outside this sidecar.
Comparison SHA-256:
`7ef771443295ce8e0a3833eb1288896a87af381abae98bc8aab997fb75b51007`.
The independent terminal audit verifies exact original-seed NPZ member bytes,
all 57 trace events, and drained closest-bias workers, including the final
worker stopped before producing a response. Audit SHA-256:
`36df6280986580eaf7cf734dc8acf2d500a1d197ad182b3d2e6b3118898c516a`.
An obsolete file-size fault-test process was found consuming a CPU core and
fully drained during late refinement. This run is not a controlled timing
comparison. Next work targets repeated rejected line-search proposals and
additional dead-array lifetime reductions without loosening acceptance gates.

A separate memory-lifetime experiment releases raw capture queries and audit
temporaries while retaining separate, read-only solver inputs. On the same
hardest case it produces exactly the same payload and numerical metrics.
Validation peak falls from 328.00 to 296.30 MiB, but overall peak rises from
962.73 to 969.88 MiB. This single matched diagnostic does not establish a
peak-memory improvement; the experiment remains outside the selected fitting
path. Paired audit:
`/private/tmp/attention-matching-main3-memory-lifetime-hardest-paired-terminal-independent-audit-v1.json`
(SHA-256 `17cb0aef296599ce97322ac0e446a2afdbae497678439b649ab1d14a4079cbd3`).
Fitting below 1 GiB is a solver resource gate; it does not prove the separate
requirement for at least 25% committed-context memory savings in serving.

A separate metadata-only coordinator experiment compares the current and
file-backed receipt handling in A/B/B/A order. All terminal evidence agrees,
but running coordinator RSS ranges overlap (30.89–33.27 MiB current versus
30.69–32.11 MiB file-backed). This does not establish a memory benefit; the
current coordinator remains selected. The diagnostic does not execute fitting
or model inference.

The unapplied packed-attention reducer optimization passes 90/90 standalone
Metal cases with exact output bytes and status flags using packed operation 6,
eight query heads and 256 work items. All ten bounded timing geometries are
faster in this diagnostic, including 49.6% and 52.9% lower reducer time at
32 splits for dimensions 256 and 512. These single-sample kernel timings do
not establish full-model or serving performance; repository integration and
matched whole-frame profiling remain required. Audit:
`/private/tmp/attention-matching-packed-reduce-metal-diagnostic-v3-independent-audit.json`
(SHA-256 `2d763942016f7ff4ff70babf7d167ba86b37963149857fe3ceca4d31db976eba`).

Completed diagnostics and immutable helpers are preserved in
`/Volumes/Extreme/antfly/qualification/context-main3-witness-retry-progress-20261021/manifest.json`
(1692 files, 25,263,574 bytes; SHA-256
`688ebbfe233236907eae79421b612bf2dc199446b7a24c57883c6a2927fcecec`).
This snapshot precedes the paired memory-lifetime result; external campaign
outputs remain at their original durable paths.
The newer completed snapshot includes that result, suffix cells 31–35, the
nonqualifying in-place LP diagnostic, runtime provenance and cleanup evidence:
`/Volumes/Extreme/antfly/qualification/context-main3-bias-memory-progress-20261021/manifest.json`
(1758 files, 26,754,628 bytes; SHA-256
`da86981ebe5cbdf249fd33459f733adf802741ec2f85dd2acfe4120bde704372`).
Full fitting, native recipe parity, independent
held evaluation, measured committed-context savings, matched serving performance
and soak qualification remain open. Attention matching is disabled for public
serving; its release registry is empty. Nothing is staged, committed or pushed.

Migration completed on 2026-10-20: this uncommitted work now lives on
`codex/gemma4-compaction-production`, based on fetched upstream `main`
`0c3b99c9f25207661a313fb5f13cdc3cbe418c9f`, in
`.worktrees/codex-gemma4-compaction-production`. No Qwen branch commits were
carried over, and nothing was staged, committed or pushed. The original
`qwen38-flash-next-metal` checkout's complete Git status and all 162 dirty files
match the verified backup. Its dirty files and
2576 qualification scripts/receipts were backed up and verified before transfer:
`/Volumes/Extreme/antfly/qualification/compaction-main-migration-20261020/uncommitted-checkout-and-qualification-tools.tar.gz`
(SHA-256 `72af22f5f8d09aff757dd808fd96c8cfbd515aecf31023d750adc1e4d19fdfe0`).

All full58/full60 results below belong to the pre-migration source and retain
that scope. They do not qualify the new main-based worktree. The fresh full60
seed campaign completed all 36 cells before migration; fitting had not begun.
The port removes the Qwen-only shared Metal dependency in favor of
`backends/metal_generated_kernels.zig`, preserves current-main APIs and
regenerates clients from the source specification. New compilation and
qualification evidence must bind the resulting source identity.

Current-main migration validation passed before the ownership fix described
below. The final stripped ReleaseFast Metal
context suite selects 540 tests: **498 pass, 42 skip, zero fail and zero leak**;
`check-openapi` also passes. The guarded run takes 496.68 seconds at
3762.30 MiB combined peak RSS, with zero swap growth and complete child drain.
The skipped opt-in/hardware cases do not establish installed-model, serving or
soak qualification. Resource receipt:
`/Volumes/Extreme/antfly/qualification/compaction-main-migration-20261020/main-validation/compaction-main-context-resource-v16-final-full.json`
(SHA-256 `84a87da75df6bfa28c34c5e4ccc7830f3f992679279a0a60340897404cea7d5f`).

All six final numerical/lifecycle regressions execute in both the focused and
full suites. The focused run passes 15 tests without skips, leaks or swap
growth. Explicit test-root imports prevent narrow filters from silently omitting
these regressions. The port retains current-main graph-buffer growth behavior,
while testing oversized requests against actual physical allocation. It also
preserves callback ownership through frame completion and accounts for completed
GPU work independently of a callback failure. All 69 canonical source paths are
unique and present, including capture coverage and immutable file-snapshot
verification. The final source snapshot verifies 160 source/generated inputs.

Generated Go, TypeScript, Python and Zig clients match the source specification;
Go SDK tests, TypeScript typechecking, and 62 Python provenance/build-evidence
checks pass. These migration checks establish local correctness and artifact
consistency. Fresh installed-model capture, numerical fitting, held evaluation,
measured context savings, matched performance and serving/soak qualification
remain required on this main-based source.

Post-migration qualification on 2026-10-20 found a cleanup failure in the
installed E2B generation test. Full/chunked logits matched exactly; the existing
paged route differed by at most 0.008272171 (limit 0.05). All eight generation
routes completed, but the allocator reported two leaked 600-byte tensor wrappers
and two 16-byte shapes. That run failed qualification despite correct outputs.
The decoder now releases embedded inputs borrowed by direct sampled/logits
execution and transfers ownership before entering the consuming canonical
fallback. Logits readback also releases device ownership on error. Independent
static ownership review passes; exact attribution of the observed leak and the
installed-model fix still require a fresh model run.

The focused readback success/failure regression passes: 9 selected, 9 passed,
zero skips/leaks, 15.47 seconds, 460.28 MiB combined peak RSS, zero swap growth,
and complete process drain. It does not replace compilation and model execution
of the changed decode paths. Three debug-symbol build attempts were stopped by
their resource guards (11.75 and 129.94 MiB swap growth under zero-swap guards;
874.69 MiB under the established 512 MiB build-only allowance). The user then
selected lightweight checks; full builds and model runs were deferred at that
checkpoint. Native
inference and fitting retain their zero-swap requirement.

Current-main strict-build recording now uses schema V3, removing the obsolete
ONNX build option while retaining historical V1/V2 validation. All 176 Python
release/provenance tests pass. Main1 source-capture successor V2 and all-36
execution binder V4 passed independent review (6 and 5 synthetic checks,
respectively). These tools reject old-checkout authority and preserve the frozen
numerical/resource limits. Bootstrap/execution preparation V3 also passes
independent review and all 5 synthetic checks. Its review envelope binds the
exact template hash, and the sealer pins the reviewed execution helper before
creating outputs. Regressions reject reuse of a review for a different template
and reject helper mutation. The two earlier review holds remain preserved;
future executable review must bind the V3 handoff and review together with the
generated template envelope, plan, authorization and command.

Preparing these tools did not produce new captures, fits, held results, measured
savings, serving or release authority. The subsequent resumed qualification is
recorded below. Lightweight evidence is preserved under
`/Volumes/Extreme/antfly/qualification/context-main-lightweight-qualification-20261020`.

After the user resumed qualification, the fresh Main1 stripped V3 build passed:
**541 selected, 499 passed, 42 opt-in skips, zero failures and zero leaks**.
The unchanged resource guard records 614.11 seconds, 4683.31 MiB peak combined
RSS, zero swap growth, minimum reported free memory 44%, and complete child
drain. The strict build adapter validates the pre-build source archive, unchanged
source closure, compiler profile, toolchain and executed binary. Receipt:
`/Volumes/Extreme/antfly/qualification/context-full-release-build-main1/full-release-build.json`
(SHA-256 `5baec5e8d0c60ccc8a7de1192f689cea71e3c4dba213b9691d239612d454dfd3`);
binary SHA-256
`87354460f5c2a3c56ac83a39bbdddef7a4e439833fe070afb2b14c61d3949644`.

Both installed-model baselines now pass on that binary, including all eight
generation routes and allocator teardown. E2B passes 2/2 tests in 20.12 seconds
at 585.27 MiB peak group RSS; E4B passes 2/2 in 24.60 seconds at 497.78 MiB.
Both have zero swap growth, complete process drain, exact full/chunked logits,
and zero KV readback. Original paged-route maximum logit differences remain
within the unchanged 0.05 limit: E2B 0.008272171 and E4B 0.015484333.
Artifact hashes were reverified before execution. These runs confirm that the
previously observed E2B cleanup leak no longer occurs on the default path.
Evidence and binary/artifact bindings are under
`/Volumes/Extreme/antfly/qualification/context-main-installed-model-v2`.

The first Main1 capture preflight passed, but the persistent coverage driver
rejected the V3 build receipt before loading the model
(`InvalidCoverageBuildReceipt`). The native receipt parser still accepted only
V1/V2 although the current-main recorder and Python validator already supported
V3. The parser now applies the same strict compiler-profile checks to V2/V3,
retains V1 behavior, and rejects unknown future schemas. The failed coverage
evidence remains at `context-main1-source-coverage-v1`; it has no training-source
authority. A fresh strict build and fresh capture are required after this fix.
The parser regression passes all six tests in both Debug and stripped
ReleaseFast, with zero swap growth and complete process drain. The 39 related
Python build/coverage tests also pass.

The replacement Main2 strict V3 build passes **543 selected, 501 passed,
42 opt-in skips, zero failures and zero leaks**. Its unchanged build guard
records 593.89 seconds, 4897.38 MiB peak group RSS, zero swap growth, minimum
reported free memory 54%, and complete child drain. Receipt:
`/Volumes/Extreme/antfly/qualification/context-full-release-build-main2/full-release-build.json`
(SHA-256 `8af6a490fb73d14cabcea58ab09d0e48034cfdcde289a9b29da285f1560cee34`);
binary SHA-256
`e4f8fdc44199792619dba58be1b4ede62101aace1e9f8961ff793731f6509fc7`.
The receipt-parser fix is outside the canonical compaction algorithm inputs, so
the semantic implementation identity remains unchanged while executable and
full-source-archive identities change. Capture evidence must bind Main2's new
binary and build receipt.

Main2 coverage advances past receipt validation but aborts in native teardown
before producing a coverage report. Symbolizing the stripped binary from its
matching cached object identifies `MetalKvStorage.hookDeinit` through
`KvStorageRuntime.deinit`: migration had lost the owner-tracked constructor in
the Metal compute provisioner. The hook was created with the backend allocator;
owner-scope binding then rejected its missing accounting, and error cleanup
freed it through the request allocator. The provisioner now uses
`createTracked(storage.ownerAllocator(), ..., storage.ownerAllocationTracker())`
again, and hook teardown uses its stored creation allocator. This requires new
regression, strict-build and capture evidence. The failed Main2 attempt has zero
swap growth, no resource-limit violation, complete child drain and no accepted
training data. Diagnosis:
`/Volumes/Extreme/antfly/qualification/context-main2-source-coverage-v1/native-allocator-failure-diagnostic.json`
(SHA-256 `c9b40fec7a38d3adcbc91dc336956806e04cfacbf882c43485ddf3aec4f72bcd`).
The focused real-Metal provisioning regression passes (10 selected, 10 passed,
zero skips/leaks, 88.95 seconds, 1183.13 MiB peak group RSS, zero swap growth).
It uses different backend and request allocators, verifies exact owner
accounting and ledger restoration on hook removal, and checks detached-hook
cleanup when the caller supplies a different allocator. The test is explicitly
imported into the context test root so focused filters cannot omit it. A
targeted comparison against the original checkout found no additional missing
production compaction ownership or admission wiring. Main3 strict qualification
is required for these changes; the passing Main2 build predates them.

The first Main3 attempt stops before compilation when the external `Extreme`
drive disconnects during source-archive creation (`OSError: [Errno 5]
Input/output error`). No build receipt is sealed and no qualification process
remains. The failed campaign path is preserved; it supplies no build evidence.
A fresh local attempt at `/private/tmp/context-full-release-build-main3-local1`
passes **544 selected, 502 passed, 42 opt-in skips, zero failures and leaks**.
The unchanged resource guard records 646.65 seconds, 4833.33 MiB peak group RSS,
zero swap growth, minimum free memory 47%, and complete child drain. Receipt
SHA-256 is `fe9a95a685bb5e3d42a166da4e6a339b7a709efc630c3cab6c1477f3479c4e76`;
binary SHA-256 is
`02b33465e5be7cd7affb10aeaf18043d87bf58599f091fa3fed2c45b2d8568aa`.
The source archive binds the ownership fix and its regression. The external
USB/APFS volume subsequently remounted; the prior evidence hashes and the E2B
artifact hash revalidate. Fresh capture preparation binds this local build
bundle and the new implementation identity
`2219a5f0c263ed8698f0d3d55e1152a4fe2e47fc2d514eeb2d347aa79caf072a`.
Preparation alone supplies no installed-model or numerical qualification.

Main3 installed-model coverage subsequently completes with return code zero,
clean native teardown and the expected 8160 prompt tokens plus 32-token reserve.
The operational coverage phase takes 144.35 seconds with zero swap growth.
Its outer supervisor receipt measures 132.78 MiB, while the separately guarded
native process group peaks at 3880.05 MiB. Their conservative sum is 4012.83 MiB,
below 8192 MiB; the outer receipt alone does not represent whole-operation RSS.
Both scopes drain. A successor acceptance check must bind both receipts and
the native resource journal and enforce the summed peak. The next capture
stage fails before native launch because the helper expects top-level `held`
and `publication_authority` flags that the template only supplies under
`expected`. The successful coverage evidence remains at
`/Volumes/Extreme/antfly/qualification/context-main3-source-coverage-v1`;
the overall operational run is failed and produces no accepted training source.
Immutable successor capture tooling is required; the native build is unchanged.

The immutable Main3 V2 retry subsequently succeeds. Coverage passes with a
3592.91 MiB conservative composite peak (3494.08 MiB native plus 98.83 MiB
supervisor), zero swap growth and complete drain. Native capture passes its
single selected test in 456.74 seconds at 5188.70 MiB combined peak RSS, with
zero swap growth and no surviving child processes. The final source contains
all 36 canonical cells and binds the current binary, build, source archive,
loaded model, capture inputs and composite coverage evidence. Terminal receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-source-v2/final-source-receipt.json`
(SHA-256 `b612756a434b57d287d6ff11a78c772cdabaa391e7da84923a9622a6de282e0b`).
The operational receipt SHA-256 is
`22594053098137659dc4e93860fa4e2b0e19e162d10edbf7bcb729ada2aaf80e`.
Independent terminal review passes, including a streaming rehash of all 361
source-graph files (729,697,087 bytes), exact canonical cell order and the
capture/candidate/final-authority transition. Audit SHA-256:
`4feec5e8a641267809b15470971cba87796d7bdf9c261f451fadbf176d6ab3ac`.
This grants training-source authority only; it does not establish numerical
qualification, held evaluation, savings, serving or release authority.

The seed-stage compatibility check finds that the older exact evidence schema
rejects the new `coverage_composite` edge. Immutable successor seed/fitting
adapters must require and validate that edge without changing geometry, fitting
math, retention or resource limits. Frozen predecessor scripts and failed
attempts remain preserved. The absolute-path `/private/tmp` scripts are local
qualification tooling; a successful recipe still needs reproducible repository
tooling and a qualified native implementation before release.

The cross-bound Main3 seed successors pass independent review and reject a
valid memory receipt spliced from another source. The reviewed V2 execution
then completes all 36 fresh seeds in 133.16 seconds. All 37 cell/final-validation
resource receipts pass: maximum combined peak RSS is 649.73 MiB, swap growth
is zero, and every process group drains. Seed campaign receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-fresh-seeds-v1/campaign-receipt.json`
(SHA-256 `20240fe7298fb341bbf45a604611810c1870402fd74f7225308ed50d0f00515a`).
All seeds are frozen before downstream fitting. This does not yet qualify the
numerical recipe, held quality or actual committed-context savings.
Independent seed-terminal audit also passes (SHA-256
`8c6ba533ef9e4f41146a57de92fa9d8f215e952796caadcbd613552414bf6d9d`),
including all 36 result/source edges, 23,134,272 payload bytes, final validation
and every guard receipt without decoding the payload arrays.

The first current-source hardest-case pilot reaches the mass phase but fails
before invoking the solver: the orchestration adapter includes `owner_layer`
in a payload whose frozen recipe requires exactly `original_seed` and
`training`. Source loading passes; the failed mass process peaks at 206.02 MiB,
has no guard violation or swap growth, and drains completely. No fitted
candidate or numerical result is produced. The adapter correction must preserve
the recipe's strict input contract, stored arrays and numerical/resource gates.
Failed-terminal audit SHA-256:
`7a48f8364e3dcc3beba03db68929c91c59ce36e910a8f9593b31dc03df01e53b`.

The corrected immutable adapter passes the actual frozen mass/output boundary
and rollback regressions. Fresh pilot V3 then passes for owner 14/block 4096,
retaining 288 rows: stored mass error is 0.0499480441 (limit 0.05), worst-consumer
output RRMSE is 0.0991679208 and aggregate RRMSE is 0.0956845675 (both limit
0.10). Maximum anchor drift is 0.162310712 (limit 0.375). Independent stored
recomputation agrees. The fit completes in 274.66 seconds; all four phase guards
pass with zero swap and complete drain. Output-stage combined peak RSS is
1011.86 MiB, only 12.14 MiB below the unchanged 1024 MiB cap. This narrow measured
margin is not an all-case memory guarantee. Evidence:
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-hardest-fit-pilot-v3/owner14-block4096`.
All 36 cells still require a fresh complete campaign, followed by native parity,
independent held quality and measured committed-context savings.
The first full campaign stops before its first solver invocation: plan 00
records 41 helper origins, while source-authority validation lazily loads the
42nd, the already-bound coverage-composite validator. Later plans and pilot 31
contain all 42. This is an order-dependent manifest construction bug; no cell
passes in that campaign and no numerical gate fails. The binder successor must
load the complete authority closure before freezing a common origin list and
reject order-dependent differences. The failed campaign remains preserved at
`/Volumes/Extreme/antfly/qualification/attention-matching-main3-full36-fit-v1`.

Binder V9 fixes that construction bug by loading the complete source-authority
closure before freezing origins. All 36 plans now bind the same ordered 42
origins; fresh-process checks and mutation regressions pass. The independently
reviewed V2 campaign starts from fresh seeds and outputs, without reusing the
pilot or failed campaign. It terminates after 27 passing cells and one failed
attempt, owner 14/block 2048. The mass solver rejects a candidate with
`invalid LP primal/dual witness`; the transaction restores the exact original
seed payload. The supervisor's top-level `mass phase evidence missing` error
obscures this more specific cause, which remains recorded in `mass.json`.
The failed mass process takes 20.47 seconds at 723.73 MiB combined peak RSS,
with zero swap and complete drain. The campaign lasts 1794.93 seconds and
does not grant all-36, native-parity, held, publication or release authority.
Independent terminal audit revalidates the passing prefix and failed snapshot
(SHA-256 `da9a9798541f663822d99e9c319411259587f6fdddc04a29624c28bfe36ab506`).
The next numerical investigation must preserve the rejected solver's original
coordinate residuals before considering a solver change; acceptance tolerances
remain unchanged.

The bounded failure replay reproduces the rejection and exact original-payload
rollback in 18.04 seconds at 707.72 MiB, with zero swap and full drain. The saved
1128-by-288 restricted matrix fails only the upper primal-feasibility witness:
its residual is 1.21255605e-6 against the unchanged 1e-6 limit. All other
recorded primal/dual, complementarity and stationarity checks pass. A separate
restricted-matrix comparison reproduces the original failure; tightening the
solver's own primal/dual tolerances from 1e-9 to 1e-10 passes every unchanged
witness check, with maximum primal residual 1.28786801e-10. Disabling presolve
does not fix the rejection. This comparison takes 6.50 seconds at 329.34 MiB,
with zero swap and full drain. It supports testing a deadline-bounded tighter
retry, but does not qualify a changed recipe or supply a reusable fitted
payload. Evidence: `/private/tmp/attention-matching-main3-lp-residual-variants-v1`.
Failure and diagnostic evidence is preserved at
`/Volumes/Extreme/antfly/qualification/context-main3-fit-failure-followup-20261021`
(manifest SHA-256 `bcb9beada4e4dc6244e61df8d34143c828defe4db98b3088591860d2ee8feff6`).
Campaign: `/Volumes/Extreme/antfly/qualification/attention-matching-main3-full36-fit-v2`.
The post-fit native parity runner also passes static review and real producer
boundary checks after correcting its batch/cell schema expectations from V5 to
the producers' V4. Actual parity execution requires a successful terminal
all-36 campaign; preparing the runner does not satisfy that gate.

Both installed exact-generation baselines also pass on Main3's fixed binary:
E2B is 2/2 in 22.39 seconds at 570.02 MiB peak group RSS; E4B is 2/2 in
32.58 seconds at 553.94 MiB. Both complete all eight generation routes, have
zero leaks/swap growth, drain all children, and match full/chunked logits exactly.
The original paged-route maximum differences remain 0.008272171 and 0.015484333,
respectively, within the unchanged 0.05 threshold. Both report zero KV readback;
owned-route hooks restore and rejected requests clear correctly. Model hashes
revalidate before and after execution. Source/build/model-bound evidence is at
`/Volumes/Extreme/antfly/qualification/context-main3-installed-model-v1`.

A separate forced reference-decode/direct-ownership diagnostic completes the
external-context routes, then fails on packed sampled decoding with
`ContextFramedExecutionUnsupported`, zero allocator leaks and zero swap growth.
It is retained as a failed diagnostic, not a passing alternate-route gate.
Full fitting, native candidate parity, independent held
evaluation, actual memory savings, matched performance and serving/soak remain
required. In particular, Python fitting cannot qualify the different native
bounded-coordinate-descent solver; general serving requires a separately
identified native implementation of the qualified recipe and parity evidence.

The native-port plan also has an unresolved latency feasibility gate. The
historical hardest-cell mass and output phases total 226.54 seconds, before
the remaining 35 cells, query generation, validation and publication. The
560-second recipe deadline is a containment limit, not a serving budget.
Release still requires attention-matching p95 TTFT at most 1.25 times exact,
decode lower confidence bound at least 0.90 times exact, and committed savings
lower confidence bound at least 25%. Historical packed-exact measurements also
miss their own latency gates. A language port alone has no measured basis for
the necessary speedup. After fresh numerical qualification, first measure the
current-build serving floor with a matched exact / packed-exact / stored-payload
triplet, excluding fitting but including real publication, first-callback TTFT,
decode and committed-byte accounting. This is diagnostic evidence only. If
that floor fails, fix the serving path before porting the expensive fitter;
if it passes, compare a native mass/support prototype with the remaining TTFT
budget. No latency or numerical gate is relaxed by this sequencing decision.

Static review further narrows the first performance measurement to balanced
current-build exact/packed diagnostic pairs. Record all phase fields: packed
session construction currently falls under `multimodal_prepare`, even for
text-only input. Explicitly control inherited Metal validation and trace flags.
The stored-payload path remains test-only and still performs prompt-specific
question, fit-study and gate-study replay before loading, validating and
publishing 36 artifacts. Excluding the numerical solve therefore does not
exclude query/replay or staging/publication costs. Those costs need separate
measurement before a native fitter port can be assessed against the remaining
TTFT budget.

The first Main3 standalone exact/packed diagnostic exits before any observation.
Its collector discarded the native exit status, so the exact signal is unknown.
The old collector's 16 MiB file-size ceiling with default `SIGXFSZ` handling is
a plausible cause. An immutable diagnostic successor keeps that ceiling, makes
overflow return `EFBIG`, and preserves native return code/signal and cleanup
evidence. Its bounded overflow and synthetic signal regressions pass. This
tooling fix is not yet incorporated into repository release automation.
The standalone capacity probe also omits the serving execution lease; its
packed setup can construct runtime/kernel resources rather than borrowing
resident ones. It therefore cannot establish a warm persistent-serving floor.

A subsequent Main3 diagnostic completes both native runs with identical prompt,
output hash and all 128 output token IDs, zero KV readback, zero swap growth and
clean process drain. On the repeated-token 8192-prompt/128-output workload,
automatic execution takes 172.26 seconds (167.70 seconds to first callback;
4.56 seconds decode), while packed serial-reference execution takes 298.32
seconds (227.81 seconds to first callback; 70.50 seconds decode). The
conservative sum of separately sampled native and collector peak RSS is
4382.81 MiB, within the 8192 MiB bound. This single cold pair identifies a
substantial serial decode cost; it does not measure the optimized split/flash
route or qualify serving performance.

The collector fails after both native successes because the packed observation
marker is concatenated to the Zig test prefix. The original failed receipt is
preserved. A strict parser successor accepts that exact prefix and rejects
duplicate or unrelated embedded markers; four regressions pass. A separate
immutable diagnostic recovery verifies the raw logs, native exit codes, output
identity and resource evidence without rerunning or promoting the failed
collector. Recovery receipt:
`/private/tmp/attention-matching-main3-exact-packed-pair00-recovery-v1.json`
(SHA-256 `a361abe83141eb05d17b0434eed7c450dabbd8e82abfd9cb0dbebf0f2bc8fd9a`).
Other limits remain explicit: the 16 MiB file-size ceiling can affect cache
behavior, the probe has no serving execution lease, and lightweight metadata
work overlapped the run. The next diagnostic should measure the optimized
packed route before repeating the serial timing pair.

That optimized diagnostic now completes both native runs and the collector.
Automatic execution takes 164.12 seconds, versus 150.29 seconds for packed
split-decode/flash-prefill execution. First callback is 159.68 versus 140.97
seconds; prefill is 159.68 versus 140.69 seconds. Packed decode still takes
9.30 seconds versus 4.44 seconds for automatic execution. Thus the observed
prefill improvement offsets a remaining decode regression in this workload;
the result is not a passing production performance campaign. All 128 output
token IDs and output hashes match, KV readback and swap growth are zero, and
both native processes plus the collector drain. The packed run records 4445
split-decode dispatches and 896 flash-prefill dispatches. Both loaded route
identities match the expected current-build profiles. Conservative peak RSS
including the outer collector is 4574.64 MiB, within 8192 MiB.
Evidence: `/private/tmp/attention-matching-main3-exact-packed-8k-optimized-pair-v1`.
The separate terminal checker initially rejects the collector's semantically
identical reserialized plan because it compares its byte hash to the original
plan's hash. Its successor verifies both file hashes and semantic plan equality,
pins the two expected implementation identities, and validates before creating
an output file. Four regressions and the completed diagnostic pass without a
model rerun. Terminal diagnostic SHA-256:
`9027df85fb0f864111bf313fbd7f9f26b8aef3a4e6794420b7fe9d1940c4e9e2`.
Cold-process,
repeated-token, file-size-ceiling and overlapping metadata-work limitations
still apply; warm serving and balanced matched release measurements remain open.

A subsequent bounded GPU-stage profile pair preserves identical output tokens,
zero KV readback, zero swap growth and complete drain. It samples 16 decode
frames per route: mean GPU frame time is 36.21 ms for automatic execution and
69.08 ms for packed split/flash execution. The attention bucket grows from
8.78 ms to 49.69 ms, while sampled FFN and tail times decrease. This locates
the main sampled regression in the attention block, but that bucket includes
projection and KV append/copy work as well as attention dispatches. It does not
isolate split softmax. Source inspection confirms the packed attention and
precomputed tail share the same active, unsubmitted GPU frame, so the observed
regression does not justify attributing an extra submission or wait to that
boundary. Finer substage instrumentation is needed before selecting a kernel
change. Evidence:
`/private/tmp/attention-matching-main3-exact-packed-8k-stage-profile-pair-v1/diagnostic-v2.json`
(SHA-256 `1a2d0a053bf303dfe1a0b3c2c0dabc22215d24a5a02ab65f62438687315ead2e`).
This remains a cold, repeated-token diagnostic with the same standalone-probe
limitations; it is not a matched production performance result.

Current release status: **exact generation remains available; attention matching
is not production qualified and its release registry is empty.** The v18/v19 work
below adds admission, provenance, measurement and serving-isolation machinery.
It does not supersede earlier numerical failures with a compression or speedup
claim. Historical checkpoint sections retain the evidence and limitations of
each run.

The runtime separates positional admission from retained KV limits. Exact
policies are enforced by native generation, HTTP single and batch admission,
and the local and server CLI paths. Internal qualification hooks now exercise
request-owned packed ingestion, repeat-prefill replay, adaptive per-head arenas,
publication and continuation. Loaded artifact/tokenizer/template provenance and
separate host/backend reservations protect that lifecycle. Public approximate
HTTP/CLI modes remain disabled pending qualification of the implemented
loaded-model preflight owner and the complete release evidence.

Shared/exclusive execution barriers preserve ordinary exact batching while
isolating approximate requests after memory admission. The fixed half-block
E2B/E4B probes still fail numerical validation. High-retention development
profiles pass narrower FP64/Metal checks with small projected savings; they do
not establish the required total-context savings or model quality. The v19
framed split-decode/flash-prefill candidate preserves observed exact output,
but packed decode still misses performance gates. No artifact is
production qualified.

### Pre-migration qualification evidence and remaining gates (2026-10-20)

| Gate | Current evidence | Remaining requirement |
| --- | --- | --- |
| Complete context build | Strict stripped full-release build 60 passes: 450 selected, 431 passed, 19 opt-in skips, zero failures/leaks and zero swap growth. It includes the compensated native validation reduction and its regression. Strict build adapter passes: 487.10 seconds, 3052.03 MiB peak group RSS. | Fresh installed-model full36 execution and serving gates remain separate. Opt-in skips do not supply model, held-out, serving or soak evidence. |
| Current-source capture | Full60 V11 capture and sealing pass for all 36 cells after correcting the native/collector provenance boundary. Exact coverage verifies 8160 prompt tokens plus 32 reserve. Capture takes 500.58 seconds at 3908.30 MiB combined peak RSS; sealing takes 17.47 seconds at 683.97 MiB. Both have zero swap growth and complete child drain. The seed authority's stale owner-14 validation count is corrected to the native contract's 960 rows; independent review and 12 focused checks pass. | Execute fresh seeds and the complete 36-case fitting campaign. The geometry correction changes no training rows, numerical recipe, retention or resource limits. |
| Fresh seeds on full60 | All 36 seeds complete in 177.4241 seconds. All 37 cell/final-validation guards pass; peak combined RSS is 667 MiB, swap growth is zero and every child group drains. The campaign remains training-only and performs no downstream recipe fit. | Preserve as pre-migration evidence. The main-based source requires a new build, capture and seed authority before fitting. |
| Failure handling | Standalone Jinja suite: 69 passed; focused collector/deadline suite: 43 passed. Focused corrected-failure and tool-provenance run: 13 passed, zero skipped, zero new swap; the full context build also passes. | Real serving failure injection and lifecycle checks. |
| Gemma4 numerical fit | E2B 8K: 36 exact controls pass. The hardest owner-14/block-4096 training case passes the lifetime-cleanup V14 recipe: 9.9168% worst-consumer output error, 9.5685% aggregate error and 4.9948% mass error. Independent stored-payload recomputation passes. Stored K/V/bias bytes and numerical metrics are identical to V12. That exact payload also passes native Metal parity on full60, including all 40 training consumers; aggregate FP64/device discrepancy is 9.38e-9. Mass/output stages take 171.21/55.33 seconds, with zero swap and complete child drain. Peak fitting RSS is 981.203125 MiB, 42.796875 MiB below the unchanged 1 GiB cap. | Use fresh current-binary training captures and pass all 36 stored-payload gates with this frozen recipe; then native parity, fresh held evaluation and actual context savings. One-case headroom is measured, not an all-case memory guarantee. |
| Memory savings | Uniform r288 projects 28.2780% request-context savings; supported per-owner tuples (320,288,320) and (352,288,288) project 25.5768% and 25.5487%. Historical owner-4 r352 seeds were frozen before fitting; the current-source campaign requires new seeds for all 36 cells. Uniform 320/352-row retention misses the 25% gate. | Passing numerical budgets and measured post-publication request-context committed physical bytes. |
| Quality and payload provenance | Frozen source corpus and exact-token packing tools prepared. Historical tool-call/response schema 2 landed after independent review: 10 standalone Zig tests and 33 integrated Python checks pass. Fresh full58 installed-model coverage verifies the expected E2B identity and an exact 8160-token rendered prompt plus 32-token reserve, with zero new swap and clean teardown. The materializer now acquires and pins a model on demand. | Verify the corrected quality materializer with the installed model, then independent quality runs at 8K and 32K for both artifacts. Generated tool invocation remains outside this evidence. |
| Performance | Earlier packed decode misses gates. Focused synthetic 8K split replay diagnostic passes all 18 cells: maximum absolute error 2.28e-8, maximum relative RMSE 1.51e-5; no new swap. D512 split16 changes one-row host timing from 63.43 ms to 4.58 ms and eight-row timing from 61.24 ms to 21.89 ms. | Repeat from the full release artifact and establish matched whole-model production timing under unchanged limits. Diagnostic timings include Metal validation and do not establish production latency. |
| Serving and soak | Approximate serving remains disabled; no release profile is registered. | Qualified numerical payload, route/rollback tests and four-hour persistent soak per artifact. |

### Stored-payload device validation and fitting memory

The new diagnostic `Kernels.validateStored` accepts caller-owned f16 keys and
values plus f32 bias and measures training mass, validation mass, aggregate
output error and every consumer's output error on Metal. It does not construct
a publishable `Candidate`. Inputs remain caller-owned; scratch/status allocation,
alias checks, cancellation and the bounded 32-byte report retain their native
contracts. Training mass is actually evaluated, including a regression where
held metrics pass but training mass must reject the payload.

The focused native run passed all nine selected tests in 13.61 seconds at
397.05 MiB combined RSS, with zero swap growth. Receipt:
`/private/tmp/context-stored-native-focused-v2/validation.json`
(SHA-256 `440e0d165df8d3d3259cdb0cb910e34608d2d45950220fcf4ef5923f454c3ef4`).
A subsequent build also compiles the opt-in sealed stored-payload training
parity bridge: nine tests pass and that opt-in test skips without its input.
That bridge deliberately puts the complete training corpus in the device's
validation slot and labels its results as training parity, never independent
held-out evidence. Its SHA-bound input and exact caller-buffer checks cannot
grant publication or serving authority.

The first native stored validate-and-copy step is now implemented behind a
`builtin.is_test` boundary. It prechecks destination ranges/alignment/aliasing
before validation dispatch, validates the real training and held queries,
rechecks the same refs before synchronous GPU copy, and marks a generation
span filled only after success. Focused ReleaseFast Metal tests pass 11/12,
with only the sealed real-payload opt-in test skipped: 14.67 seconds,
406.81 MiB combined peak, zero swap and clean drain. Receipt:
`/private/tmp/context-stored-native-focused-v4/validation.json`
(SHA-256 `ea49a6950c7b87cb6d5d4d7267d502242022320c300e1b505cd6d0c54ed82305`).
The subsequent multihead correction preserves local fitter coordinates
`kv_heads=1, kv_head=0` for one-head source slices, while independently using
the destination generation's head index. The head-1 Metal fixture verifies
head-0 bytes/completion state remain unchanged and rejects wrong coordinates
before validation. The corrected fixture includes the required exact tail.
The focused successor passes 12 tests with one opt-in skip, zero swap and
clean drain. Receipt:
`/private/tmp/context-stored-native-focused-v5/validation.json`
(SHA-256 `806dcc649e63f032729a144f445f4aa8b111e159c2c6f56825cfbfb10c42d123`).
This first step is per-head unpublished staging proof. Fresh held corpus
qualification and actual all-36 publication measurements remain required.
The subsequent stored-loader and all-owner diagnostic path passes 21 focused
ReleaseFast Metal tests with one sealed-payload opt-in skip. It verifies loader
budget admission before uploads, every partial-allocation cleanup path,
release accounting with unrelated live allocations, consumed-state handling
after a post-release proof error, and query/digest identity. A two-owner,
two-head fixture rejects a bad second owner without changing either original,
then uses a fresh campaign to commit both owners through one Batch. It checks
the exact committed K/V/bias bytes and complete tracked teardown. The fixture
retains 480 of 512 uniform rows with bias `log(512/480)`, ensuring real physical
reduction while preserving its exact constant output; the original 511-row
fixture correctly failed the unchanged allocation-reduction admission. This
is synthetic native correctness evidence, not installed-model qualification.
The passing run takes 17.07 seconds at 479.484375 MiB combined RSS, zero swap,
and complete child drain. Receipt:
`/private/tmp/context-stored-native-stage2-focused-v5/validation.json`
(SHA-256 `9941681cdee0060cd5a2caefaeaabd634f69130ac5d58339391cd21723f473ef`).

The all36 coordinator/provider extension is under diagnostic validation. Its
first focused build stopped on two invalid method calls to the provider cleanup
callback, before tests execute. The bounded build uses 525.765625 MiB combined
RSS for 16.39 seconds, with zero swap and complete child drain. Resource receipt:
`/private/tmp/context-stored-native-all36-focused-v6/resource.json`
(SHA-256 `841ca55aab083f457ff4330de0cdb728823acc24aee157115f6c7c5edc0208fd`).
The cleanup calls and a later fixture type error are fixed. The provider now
releases tracked materialization after authenticated loader upload and before
publication, verifies the exact scoped release, and retains its identity for
precommit verification and durable commit. Stable corpus/row and content claims
prevent held-query reuse through a changed reservation nonce. The coordinator
uses actual tracked baseline and peak bytes, and records irreversible commit
before any later receipt failure. Focused V8 executes 26 tests: 25 pass and the
sealed-payload opt-in test skips. It takes 18.12 seconds at 502.453125 MiB combined
RSS, with zero new swap and no remaining children. Receipt:
`/private/tmp/context-stored-native-all36-focused-v8/validation.json`
(SHA-256 `79d9b68f44521aecb8d7e45f93eb1bf279e81640b03496180fae83565459c34f`).
This covers provider lifecycle, admission arithmetic, coordinator authority
rejection, and the existing all-owner fixture. This V8 result predates the successful synthetic full36 coordinator fixture
described below; installed-model publication remains unqualified.

The next focused V9 build compiles and executes the new 36-artifact fixture,
but that fixture is rejected with `CompactionQueryProtocolMismatch` before its
intended precommit fault. It supplies invented study bytes and 20 response
tokens while the coordinator requires the compiled fit/gate studies and 256
responses. The other 25 tests pass, one opt-in test skips, and no leaks are
reported. This failed run uses 537.328125 MiB combined RSS for 26.69 seconds,
zero new swap and complete child drain. Its failure receipt is
`/private/tmp/context-stored-native-all36-focused-v9/failure.json`
(SHA-256 `a3399ffdfe414cdf62b3a2a6f428cba02b2b23831e73514f84776ca35f11e358`).
The successor must use the real compiled study contract; the production
protocol check remains unchanged.

The corrected canonical-study V10 build reaches the study runner, which then
rejects the null optional token-capture buffer with `InvalidStudyGeneration`.
The fixture incorrectly equates capture authority with observer capture mode:
stored-diagnostic mode passes no observer output buffer. Its successor must
compute its deterministic generated-token digest regardless of optional capture.
The run again reports 25 passes, one skip and one fixture failure with no leaks,
using 509.734375 MiB for 22.07 seconds, zero swap and complete child drain.
Failure receipt:
`/private/tmp/context-stored-native-all36-focused-v10/failure.json`
(SHA-256 `a427e86174951b1b92d66f58e17ebadd8267bf542756f71f1983ad0fc0093d18`).

The V11 fixture reaches storage admission but omits the one-row continuation
reserve from its retained-row budget, correctly producing
`ContextRetainedBudgetExceeded`. V6 of the fixture derives its budget from the
live plan and preflights the actual allocation, including the continuation row.
Focused V12 then passes: 27 selected, 26 passed, one sealed-payload opt-in skip,
no failures or leaks. It takes 26.46 seconds at 513.609375 MiB combined RSS,
with zero new swap, complete child drain, and unchanged source hashes after
execution. Receipt:
`/private/tmp/context-stored-native-all36-focused-v12/validation.json`
(SHA-256 `51c2af81da3c3a86eb1e5ec4450822968fb8da9dd9bfc44696aff5473b9d491d`).
This runs all 36 synthetic artifacts through the actual three-owner
coordinator. It verifies precommit rollback without changed handles or epochs,
one atomic all-owner commit, tracker-derived savings of at least 25%, and
tracked teardown. Its constant synthetic queries and values do not qualify
trained Gemma4 payloads. Fresh held evaluation, installed-model all36
publication and physical measurement remain separate work.

The subsequent installed-invocation adapter V4 is applied and passes focused
native compilation and synthetic execution in V14. It adds a test-only generation-pipeline method over
an already-prefilled packed session and the real campaign runner, with frozen
model/prompt/source/recipe/artifact identities. A rejected reservation retains
durable abort and nonreuse evidence; failures after publication retain the
committed outcome. The synthetic fixture now exercises this adapter for both
rollback and success. Static review, formatting, and AST checks pass. Review:
`/private/tmp/context-stored-installed-invocation-independent-review-v4.json`
(SHA-256 `49ec4392f7189fabf2e3bbd61a6825c9fd5b0879a6961cf4c81bc3f9e5cdf8ab`).
Focused V14 executes 30 tests: 29 pass and the sealed-payload opt-in test
skips, with no failures or leaks. It takes 33.93 seconds at 570.15625 MiB
combined RSS, with zero swap, complete child drain, and unchanged source
hashes after execution. Receipt:
`/private/tmp/context-stored-native-all36-focused-v14/validation.json`
(SHA-256 `185591dd61bea72abe61b0007f5fcfd161b97c11434023b091b0abc817ce0440`).
This verifies the adapter with synthetic inputs. The installed-model caller V2
is now applied after independent static review. It loads a frozen external
manifest, obtains model identity from the actual model load, reuses the capture
input loader and packed prefill, and invokes the same adapter before writing
an exclusive, synced receipt. The first candidate's downstream-failure
double-free was fixed by transferring eligibility allocation ownership once;
allocator-backed failure fixtures are included. Seven static checks and
formatting/AST checks pass; the subsequent native execution is recorded below.
Review:
`/private/tmp/context-stored-installed-caller-independent-review-v2.json`
(SHA-256 `cd89802555645570695e28dc15480b5455e2095c2722f71048d0656089e56f54`).
The V15 native run compiles the installed-model body and executes 41 selected
tests: 39 pass and two opt-in tests skip, with no failures or leaks. The added
loader eligibility cleanup fixture passes both downstream failure cases.
The native child exits zero after 262.01 seconds at 3614.90625 MiB combined
RSS, with zero swap and complete child drain. The wrapper exits nonzero only
because it expected 33 selected tests: importing the installed-model path
also includes eight additional root `test_0` declarations. Its immutable raw
results are preserved. A separate metadata-only postvalidation verifies all
41 unique indexed results, unchanged statuses for all 30 previous tests, the
11 exact added names, all 46 current source hashes, and the original resource
limits. It explicitly retains the original wrapper failure and does not rerun
or rewrite the successful tests. Postvalidation:
`/private/tmp/context-stored-native-all36-focused-v15-postvalidation-root-v1.json`
(SHA-256 `38a2ac8e7b6ed36dffac398f323ecb2e73ef854a1e73bbf0aa3935737eae192a`).
Resource:
`/private/tmp/context-stored-native-all36-focused-v15/resource.json`
(SHA-256 `e84539da5adff879de0384e94a2dd05aee25a15bfff953cde583f3a180b5d9fa`).
Test log SHA-256:
`54c543fa9f978fde5569a69b407fafe4487311c7a92fd8fb592ecc89d29c6bdd`.
All36 trained artifacts, fresh held queries generated after the artifact
freeze, and measured installed publication remain required. The caller does
not independently establish that chronology or enable serving authority.
Its source changes also change the runtime implementation identity. Final
qualification inputs must bind the final binary and source manifest; old
training captures cannot silently acquire the new runtime authority.


Repeated identical real LP solves identify fitting-process RSS accumulation.
Explicit garbage collection alone and a fresh single-thread HiGHS run both
preserve the numerical result but do not eliminate RSS growth. These negative
diagnostics are preserved at
`/private/tmp/attention-matching-lp-allocator-gc-only-v3` and
`/private/tmp/attention-matching-lp-allocator-single-thread-v4`.
The V19 pilot isolates each complete constraint-generation solve in a
short-lived worker. Its parent and all workers share the guarded process group,
so the unchanged 1 GiB limit includes their simultaneous residency. The real
owner-14/block-4096 pilot completes in 266.42 seconds at 789.53 MiB, with zero
swap and all 40 workers drained. After 34 accepted key steps, stored training
mass error is 4.999808%; legacy value fitting remains above the output gate at
16.202147%. This bounds that continuation experiment's fitting memory, not
its numerical qualification or a complete fresh recipe. It starts from the
V6 refined payload (`4defbd3e…`), already 0.1636065 from the original centroid
anchor, while the newer V22 recipe starts from centroid payload `13a89eed…`
with zero initial drift. The historical V3 and V6 preparation stages took
19.76 and 106.90 seconds respectively. Therefore the 789.53 MiB continuation
result and the fresh V22 memory failure are not matched runs, and their
difference cannot be attributed to the tighter mass margin alone. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-shadow-v19-cg-subprocess-owner14-b4096-v2/campaign-receipt.json`
(SHA-256 `c684107882f002d4b43b8150a4c84e3b575d45e1b1c9f55f646332ea4cf37e53`).

The fresh serial owner-4 r352 campaign completes all twelve blocks in 540.52
seconds at a maximum 785.77 MiB combined RSS, with zero swap. All twelve pass
stored training mass; ten pass worst-consumer output error. Blocks 2048 and
3584 remain at 12.076433% and 13.366781%. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-r352-owner4-serial12-v6/campaign-receipt.json`
(SHA-256 `f64f29b2b3f0df4f083ba19ffe36c610ecff40dafb198c84ff04bea5161f0cdd`).

The sealed native training-parity bridge now runs real payloads. The earlier
owner-4/block-5120, owner-9/block-4608, and owner-14/block-1536 payloads all pass
Metal's unchanged numerical gates, including all 40 shared-owner consumers in
the last case. The largest absolute metric difference from the FP64 oracle is
`9.7017562e-6`, within the declared `1e-5` parity bound. Receipt:
`/private/tmp/attention-matching-stored-training-parity-v3/receipt.json`
(SHA-256 `8ce73ce868f6afa6c5d1c1dacb151fd5da620b3424a02748a1acab78863c814e`).

The broader native check of all twelve r352 payloads produces eight native
training passes and four rejections. It reproduces the two known output
failures, and additionally rejects blocks 3072/4096 for device mass errors
`0.05000030994415283` and `0.05000007152557373`, although their FP64 stored
errors are below `0.05`. The maximum observed mass-metric difference is
`2.8658354e-6`; thus the current approximately `2e-6` fitting margin is
insufficient. The release threshold remains unchanged. A stricter internal
mass target must be part of the next frozen recipe, including support and
output refinement. Receipt:
`/private/tmp/attention-matching-stored-training-parity-serial12-v4/receipt.json`
(SHA-256 `b7a28820cc0ae210961436377b7c4b92621ff502715d6a09404e677fd3cfab46`).
A subsequent bias-only finalizer blends positive weights toward the same-key
terminal key-phase witness, keeping stored K/V unchanged. Its raw target is
`0.04994`, stored verification target `0.04995`, and release mass limit remains
`0.05`. Actual Metal training parity passes all twelve comparisons, with all
12 device mass errors at most `0.049942851066589355`. Ten meet every training
quality gate; blocks 2048/3584 still fail output at 12.168562%/13.412188%.
The largest CPU/device metric difference is `7.0788954e-6`. Preparation and
serial native checks total 5.58 guarded seconds at 253.22 MiB peak combined RSS,
with zero new swap and all children drained. This proves the headroom repair
for these payloads, not the fresh unified recipe or independent evaluation.
Receipt: `/private/tmp/attention-matching-stored-training-headroom-v5/receipt.json`
(SHA-256 `dc7dc9bba82cadb3722470105ea3557b793fb0d1483a4e7b00d2a93f3d22a5fa`).

A separate output-refinement pilot for owner 4/block 2048 now passes CPU
training checks: ten accepted steps reduce worst-consumer output error from
12.076433% to 9.595166%, with mass error `0.04999804281243625` and maximum
original-anchor drift `0.14174676746580714`. Independent stored-payload
recomputation passes. The fit takes 113.40 guarded seconds at 1000.875 MiB
combined RSS, zero swap, and complete child drain; the full pilot plus
validation takes 115.23 seconds. That leaves only 23.125 MiB under the 1 GiB
fit limit. The pilot starts from the terminal mass-fit payload and uses the
older mass margin; it does not establish a fresh combined mass/refinement
recipe, native headroom, or held quality. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-tangent-r352-owner4-b2048-v7/campaign-receipt.json`
(SHA-256 `4c9ba831f4a949aca00eaf489c1df6b0821c5f8834e43510c0c1870a9efcde12`).
The earlier V5 source-preflight failure is retained; its loader expected old
centroid array member names and never launched fitting. The corrected V7
loader checks the actual `assignment_i64`/`cluster_sizes_i64` seed schema.

The corresponding owner-4/block-3584 pilot fails the unchanged 1 GiB combined
RSS guard at 1027.8125 MiB after 129.72 seconds, with zero swap and full child
drain. Its last nonqualifying checkpoint reaches 12.168585% output error after
12 accepted steps; no final payload is accepted. This motivates isolating the
repeated closest-bias LP calls in same-process-group workers before retrying
from the original terminal mass-fit input, not resuming the failed checkpoint.
Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-tangent-r352-owner4-b3584-v3/campaign-receipt.json`
(SHA-256 `be27c2d1072ce738dd72ba85bfa9d592f7177a813012423139c95194d60dc704`).

The isolated-bias-worker retry completes fitting in 246.41 guarded seconds at
695.796875 MiB peak combined RSS, zero new swap, and complete child drain.
Fourteen accepted steps reach 12.145381% worst-consumer output error before
the ranked and tangent proposals are exhausted; mass remains
`0.04999805406246538`, with original-anchor drift `0.1161508334734404`.
Thus this run clears the fitting-resource limit but still misses the 10%
output target. Its independent validation fails because sorted JSON changes
the recursive ordering of worker records relative to the emitted ordered
list. The failed campaign remains unchanged; a separate corrected validation
now independently verifies its terminal diagnostic evidence and all 106
same-process-group worker lifecycles using explicit semantic order. This does
not promote it into a quality pass or authorize checkpoint continuation.
Sidecar: `/private/tmp/attention-matching-b3584-bias-worker-v6-semantic-sidecar-v2/validation-sidecar.json`
(SHA-256 `e97d32d1d47103fda61abb84d86d76c61cd917bf468fbe56f8b80bc7547d63c3`).
Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-tangent-r352-owner4-b3584-bias-worker-v6/campaign-receipt.json`
(SHA-256 `3d76b3300953c6d9f8b4a40c8a65e347611e075e25984fb2cf39adc8b15089d1`).
The successful fit-resource receipt is `resources/fit.json` within that root
(SHA-256 `3f245caca47d0d47d573f6305df85aa0232e58f0caebd58e4cd2f530ead7b8e8`).
The final outer iteration exposes a more specific numerical limit: all six
tangent proposals converge in FP64 but their value solutions overflow f16,
so none has an admissible stored candidate. The original-anchor radius is not
exhausted. The next numerical experiment should test the complete key-and-bias
envelope gradient and representable value proposals, preserving the existing
stored mass, per-consumer output, primal/dual-gap, drift, time, and memory
checks. A continuous gradient helper passes full key/bias finite differences,
parity with the existing key gradient at stored bias, and metric-descent/gauge
tests. A separate tangent-fallback integration passes nine tiny tests; it has
not yet been evaluated on model tensors. A clip-only finite-value proposal
also passes its focused stored-gap and monotonicity tests and remains the
first causal experiment. Both preserve the existing acceptance thresholds.

The fresh unified owner-4/block-2048 V7 pilot completes the mass fitter but
stops before output refinement because its caller reads worker diagnostics
from the top level instead of the public result's `key_mass` member. It exits
after 92.79 seconds at 582.765625 MiB combined RSS, with zero new swap and
no published payload. The preceding V6 stopped earlier on an obsolete worker
record schema. Both failures are preserved; neither supplies numerical
qualification or resume authority. V7 campaign receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-unified-fresh-o4-b2048-workers-pilot-v7/campaign-receipt.json`
(SHA-256 `f04f14290ffadc243b5ce46b68b365a159a18f57fc960d279cdf85b36bed4b6f`).

The corrected fresh unified V10 pilot passes owner 4/block 2048 from the
original 352-row seed, with a single load-inclusive absolute deadline and the
tighter `0.04995` internal mass target. Worst-consumer output RRMSE is
`0.09600773230048844`, aggregate output RRMSE is `0.09599248567785351`, and stored
mass error is `0.04994802470820692`. The fit takes 215.59 guarded seconds at
732.875 MiB combined RSS, with zero new swap and all workers drained. Independent
stored-payload verification passes. This establishes a complete fresh mass and
output fitting path for one difficult cell; it does not combine earlier
isolated pilots into full coverage. All-36 coverage, fresh held evaluation and measured publication savings remain
open. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-unified-fresh-o4-b2048-workers-pilot-v10/campaign-receipt.json`
(SHA-256 `64cbe857117b1d1b10ecf9f6c2d21c4afe28ba716395ca7200dbb9a3224139fd`).
The same stored payload now passes actual Metal training parity: device mass
error `0.049950242042541504`, worst-consumer output RRMSE
`0.09600723534822464`, and maximum CPU/device metric difference
`2.2173343359396802e-6`, below the unchanged `1e-5` parity bound. The tighter
internal mass margin survives device evaluation. This remains training-only
validation, with no publication. Receipt:
`/private/tmp/attention-matching-stored-training-parity-unified-v10-v5/receipt.json`
(SHA-256 `2308e62d89aaf5b7e92e2ae49f1b6fc880c75fde88f4d65342a24c2122a61368`).

The finite-f16 proposal V3 experiment passes resource and terminal-integrity
checks but fails numerical qualification by timeout. It reaches 25 improving
steps and 10.225586% worst-consumer output error before the unchanged 420-second
internal deadline; the complete returned payload is restored byte-for-byte to
the original mass-fit input. The guarded fit takes 420.95 seconds at
695.375 MiB combined RSS, zero swap, and complete child drain. The last
checkpoint remains discarded and cannot seed a later attempt. This is evidence
that representable value proposals remove the prior stall, not a quality pass.
The next isolated experiment uses the reviewed complete key-and-bias tangent
direction from the same original input. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-tangent-r352-owner4-b3584-f16-bounded-v3/campaign-receipt.json`
(SHA-256 `5769d6ee0156601c4599ce23e9769db443f7f5fb4d5282d3b4a9088c8886606a`).

The joint key-and-bias tangent successor reaches the output target in 21
accepted steps from the original terminal mass-fit input. Worst-consumer
output RRMSE is `0.09463316197796276`, stored mass error is
`0.04999802101586748`, and maximum original-anchor drift is
`0.143280364508104`. Independent stored-payload verification passes. Guarded
fitting takes 274.10 seconds at 702.015625 MiB combined RSS, with zero new swap
and all 129 closest-bias workers drained. This passes the isolated numerical
experiment without checkpoint continuation; a fresh combined run with the
tighter mass target and native parity remains necessary. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-tangent-r352-owner4-b3584-joint-gradient-v1/campaign-receipt.json`
(SHA-256 `462a5ae9d02e5f8d9775c5f7e48af6ed0181b344aae664a2542c0b2fd70ea4ae`).
The first fresh V22 block-3584 harness attempt stops in source preflight,
before fitting, because imported-module origin enumeration differs from the
actual `__main__` entry point. Its source child uses 117.25 MiB combined RSS
for 1.24 seconds, with zero swap and clean drain. The failed receipt and source
stderr remain intact; a corrected successor must test the actual command-line
entry point and include every active helper in its provenance closure.

The corrected V2 command-line harness now completes the fresh V22 block-3584
recipe. Independent stored-payload verification passes with worst-consumer
RRMSE `0.09575981672904847`, aggregate RRMSE `0.09575266963438459`, mass error
`0.0499480395531392`, and original-anchor drift `0.13765145464189457`. The
guarded fit takes 247.86 seconds at 712.53125 MiB combined RSS, with zero swap
and all 108 mass/bias worker processes drained. This starts from the original
centroid seed, includes both mass and joint output refinement under one
deadline, and uses the tighter internal mass margin. A complete fresh 36-cell
run of the same frozen recipe remains required. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-unified-fresh-o4-b3584-v22-pilot-v2/campaign-receipt.json`
(SHA-256 `f29189bb0ab97401a34c749d11bd01aacef776aced2d8e5a24b900fbc7db4542`).

The fresh block-3584 stored payload also passes actual native Metal training
parity across all eight consumers: maximum output RRMSE
`0.09575985372066498`, mass error `0.0499497652053833`, and maximum
CPU/device metric difference `1.7256522442526956e-6`. All native failure flags
are zero. Receipt:
`/private/tmp/attention-matching-stored-training-parity-unified-b3584-v22-v1/receipt.json`
(SHA-256 `81487d2642302a885afcc3ab4593d8f9bf3cf26ab6693bbcb52046bcb3948978`).
This is training parity, with no held-out or publication authority.

The fresh V22 owner-14/block-4096 attempt is resource-rejected at 231.79 seconds.
Combined supervisor/child-group RSS
reaches 1,079.296875 MiB against the unchanged 1,024 MiB limit, with zero new
swap and no live children after termination. The child group contains two
processes; this receipt alone does not identify the active worker, distinguish
individual allocations, or prove the cause of growth. No terminal cell or
qualifying payload is emitted. The failed run remains immutable and cannot
be resumed as qualification evidence. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-unified-fresh-o14-b4096-v22-pilot-v2/campaign-receipt.json`
(SHA-256 `d6ad7eecab2f3fcefe2543fe03c11db19fc163a8f55d7354083fac6d2e0c227a`).

The fresh instrumented V3 retry localizes a later peak: all 29 mass
constraint-generation calls finish before the failure, and the final active
worker is the closest-bias projection used by output refinement. Its request
is bound to the fitter PID and a 3000-by-288 matrix. Combined RSS reaches
1,029.03125 MiB after 229.77 seconds, with zero new swap and complete child
drain; no qualifying payload is emitted. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-unified-fresh-o14-b4096-memory-instrumented-v3/campaign-receipt.json`
(SHA-256 `7ba1d410cf2680e1614afba27e18d4a3af759d822760e6cfbcde1d48f77ac0c3`).
The metadata-only localization receipt is
`/private/tmp/attention-matching-o14-b4096-memory-localization-v1.json`
(SHA-256 `d89d03e90aa9083079cb3ff502e3dec220ca6d466520a945625e2ad064da09d3`).
The next lifecycle repair separates mass and conditional output fitting into
serial fresh processes under the same pre-load 560-second absolute deadline
and 1 GiB combined guard. The hypothesis is that releasing the mass phase's
retained heap before the closest-bias worker will remove the overlap. This
does not change the numerical algorithm or establish that the repair passes.

The first serial-phase V2 pilot completes the mass fitter but fails while
preparing its original-seed rollback audit. That audit reuses a helper whose
precondition requires an already-fitted mass-passing payload; the original
centroid does not satisfy that precondition. Output refinement never starts.
The mass phase takes 178.04 seconds at 888.015625 MiB combined RSS, with zero
new swap and complete child drain. The failed campaign remains immutable:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-serial-phase-o14-b4096-v2/campaign-receipt.json`
(SHA-256 `7440579365799572e19beaccaf3da6fd632f8a7c096958cea9f47c85409a7d94`).
The next harness must measure arbitrary stored-payload metrics, including a
mass-failing original seed, while retaining the unchanged acceptance checks
for every candidate. It must start a fresh run rather than reuse this run's
intermediate mass payload.

The corrected rollback-audit V4 run completes mass fitting and its audits in
179.54 guarded seconds at 838.78125 MiB, zero swap and complete child drain.
The provisional stored mass error is `0.04994802258297293`; worst-consumer
output remains `0.15318737374167665`, requiring output refinement. The
supervisor stops before that phase because it looks for mass worker evidence
in `diagnostics.phases` rather than the provisional result's singular
`diagnostics.phase`. All 29 recorded workers actually match the guarded mass
process group. This is a supervisor schema defect, not a worker escape or a
passing campaign. The failed run cannot be resumed. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-serial-phase-o14-b4096-v4/campaign-receipt.json`
(SHA-256 `87c55d5c50af90d44ee0b0009a749bdeebcfff8521a20add8208366be661a534`).

The V5 successor fixes the provisional worker schema and completes the
mass-to-output handoff. Mass fitting takes 183.21 seconds at 891.09375 MiB
combined RSS. The fresh output process nevertheless reaches 1041.875 MiB
after 59.20 seconds and is stopped by the unchanged 1024 MiB guard. Both
phases have zero swap growth and complete child drain. Serial phase isolation
alone is therefore insufficient; this run emits no qualifying payload and
its provisional mass payload cannot be resumed as qualification evidence.
The terminal output resource receipt is
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-serial-phase-o14-b4096-v5/resources/output.json`
(SHA-256 `13dca89591b4117d088f5ebf7d34a407e90bed73697786d0b3dd104f07a3c4d0`).

Metadata localizes the final V5 worker to closest-bias repair. Immediately
before that worker starts, the scientific process's RSS increases by roughly
207 MiB. The tangent projection creates multiple 3000-by-3000 Gram arrays,
each about 68.7 MiB. The reviewed V4 projection successor retains one full
matrix, multiplies contribution products in 128-row tiles, and scales in
place. It preserves the objective and acceptance thresholds, and checks its
dual quadratic against the independently computed correction norm. Twelve
small equivalence, nonmutation, and deadline tests pass. Review:
`/private/tmp/attention-matching-linearized-mass-projection-inplace-independent-review-v4.json`
(SHA-256 `d55966b848eefb93718b84d9e48ff10a3d7ad3e84f46d783e31585a421434285`).
These tests do not establish actual fitting RSS or a successful Gemma4 recipe;
a fresh original-seed run under the unchanged guard is required.

The fresh V9 run of the tiled projection completes mass fitting in 197.66
seconds at 827.609375 MiB. Output refinement remains within the fitting cap,
peaking at 974.859375 MiB with zero new swap, but terminates after 120.91
seconds on `ValueError: invalid warm dual`. All children drain and the frozen
source hashes remain unchanged. It produces no qualifying terminal payload.
The traceback reaches the warm-start dual lower-bound check after a tangent
proposal; normalization of clipped dual weights is being investigated for
rounding below the positive lower bound. This is not evidence of a complete
memory or numerical pass. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-serial-phase-o14-b4096-inplace-projection-v9/campaign-receipt.json`
(SHA-256 `362afe59810c755da6e32a4e35a4f43d6150f4c4f82cc093b7560b4219edf910`).

Small reproductions confirm that clipping dual weights to `1e-12` and then
normalizing their total can round an active weight below that same floor. The
reviewed successor reserves the total floor mass, normalizes only the excess,
and assigns the floating-point sum residual to the largest coordinate. It
recomputes the weighted least-squares solution and stored-payload witnesses
after normalization. The floor remains unchanged; the sum is checked within
four float64 epsilons, rather than claimed bitwise exact. All 45 focused
normalization and propagation checks pass. Review:
`/private/tmp/attention-matching-lower-bounded-dual-simplex-independent-review-v1.json`
(SHA-256 `f10411bc0351ce784a816cc414c4edaf80d5a6a24735b4c185896a882cf18092`).
Actual numerical qualification still requires a fresh run of this successor.

The owner-14/block-4096 V10 simplex run completes mass fitting in 191.84
seconds at 864.640625 MiB combined RSS. Output refinement runs for 367.65
seconds, peaks at 1005.5 MiB, and has zero swap growth and complete child
drain. It fails while validating a closest-bias worker lifecycle: the adapter's
timeout path omits the nested `schema` field required by the unified validator,
although its success path includes that field. No terminal cell or optimizer
trace is serialized, so this run supplies neither a numerical pass nor evidence
of the final accuracy trajectory. The elapsed time is consistent with the
unified deadline, but that inference does not replace the missing diagnostics.
Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-serial-phase-o14-b4096-dual-simplex-v10/campaign-receipt.json`
(SHA-256 `d4520f60a46d3d8d07339616b4efd6d0d81966c6c79f82113c86feb752d4aae7`).
Output resource receipt SHA-256:
`149c18b5a012c2c097765410868b4cb768dcd6ea78c9ec92e21974bd58b1c8c0`.
The immutable V10 wrapper is diagnostic-only: root explicitly rehashed the
independent simplex review before launch, but the executable plan omitted that
review dependency. The unexecuted V11 authority successor repairs that edge.
A fresh numerical successor must also emit canonical timeout records and
preserve raw tangent diagnostics before envelope validation. No failed payload
or provisional checkpoint may be resumed as qualification evidence.

The root joint-first successor freezes tangent V18, unified V28 and serial V8.
It tries the joint key-and-bias proposal before the ranked key-only fallback,
while preserving the 5% release mass gate, 10% output gate, 0.04995 internal
mass margin, 0.375 anchor radius and 560-second unified deadline. Per-trial and
per-outer callbacks preserve route chronology; accepted outers carry a
nonterminal disposition, exhausted outers carry their resolved terminal cause,
and finite monotonic timestamps are checked before sink delivery. Callback
failure stops the run, callback mutation cannot change optimizer evidence, and
callback time crossing the deadline restores the input payload. The actual
successor scripts pass 11 trace tests, 21 unified tests and 12 serial tests.
Independent review:
`/private/tmp/attention-matching-joint-first-core-independent-review-root-v2.json`
(SHA-256 `477b2626b89176b6e21f1819a66ac60912c0d754e50eef89fc0b2499563847f9`).
These are focused implementation checks. The actual executor must still persist
incremental events and final raw diagnostics before envelope validation, retain
failure traces, and bind the reviews in its executable dependency graph. No new
accuracy or fitting-resource result is implied by these tests.

The fresh root V12 pilot completes successfully from the original owner-14,
block-4096 r288 seed. Mass fitting takes 185.84 seconds at 759.65625 MiB;
output fitting takes 62.73 seconds at 1023.96875 MiB. Eight first-choice joint
steps reduce worst-consumer output error from 15.3187% to 9.9168%. Independent
stored-payload recomputation reports 4.9948044% mass error, 9.5684568% aggregate
output error, and 0.1623107 maximum anchor drift. Every guarded stage has zero
swap growth and complete child drain; all input/review hashes remain stable.
Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-serial-phase-o14-b4096-root-joint-first-v12/campaign-receipt.json`
(SHA-256 `17fd874947e48dd0a35be4edab7e40f8e813ed5144d814fbbd70223279cf5a3a`).
Independent stored validation SHA-256:
`31230b536e6b938468839f1bacd1cddcad56dab6cda2028f86612e163160fd87`.
Independent metadata/resource review:
`/private/tmp/attention-matching-full36-serial-phase-actual-result-independent-review-root-v12.json`
(SHA-256 `7f65e4b35427884ba478828519f71a4751adc43d1a02e866279da46a6f0a8565`).
This is one measured training-case pass. Only 32 KiB of RSS margin remains,
so robust fitting headroom is not established. A lifetime-only successor is
being prepared to release dead temporary matrices without changing numerical
calculations. The successful payload is preserved, and no recipe, native
parity, fresh held validation, all36 coverage or production saving is inferred
from this single case. Actual request-context savings still require accepted
native publication and the tracker-derived 25% gate.

The lifetime-only V14 successor completes from the same original seed with
byte-identical stored K/V/bias arrays and exactly equal mass, aggregate and
worst-consumer errors. Mass fitting takes 171.2063 seconds at 899.390625 MiB;
output fitting takes 55.3338 seconds at 981.203125 MiB. All guarded stages have
zero swap and complete child drain. Peak fitting RSS is 42.765625 MiB below
the prior V12 observation, leaving 42.796875 MiB under the unchanged 1 GiB cap.
The wall times are single-run observations, not a matched benchmark speedup.
The arithmetic, solver order, deadline, rollback and gates are unchanged;
only dead temporary references are released after their last use. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-serial-phase-o14-b4096-root-lifetime-v14/campaign-receipt.json`
(SHA-256 `b4de69ac9460910c0b1a17d32ba17887ee222793d0ec75edf1368c350725f199`). Exact stored-byte/resource comparison:
`/private/tmp/attention-matching-lifetime-root-v14-actual-byte-comparison.json`
(SHA-256 `7bb05301397da838e0ef864c3bd6ab45ec95d49332036a1eec0f9e551cfda545`).
This establishes a passing training recipe with measured fitting headroom for
one difficult case. It does not establish all36 performance, fresh held
quality or actual post-publication context savings. The full59 source capture,
fresh original seeds and complete same-recipe campaign remain necessary.

The native parity follow-up exposed a long-reduction accuracy defect. On full59,
the exact successful payload passes the 5% mass and 10% output gates, but its
aggregate FP64/device discrepancy is 1.0346927e-5, exceeding the unchanged 1e-5
parity gate. The failing receipt is preserved. The device reduction was summing
1,536,000 nonnegative squared terms in plain f32. Compensated f32 accumulation
for aggregate and per-consumer numerator/denominator preserves the metric,
term order, candidate bytes and gates; the generated pipeline uses safe math.
A deterministic regression is included in full60, which passes 431 tests with
19 opt-in skips, no failures/leaks and zero swap growth.

On full60, the identical AMVTR001 input (SHA-256
`c3dabdd99399597f845f7e95e21549513b3cc7556f3d8c487a00fdd52e49f6b2`)
passes strict native parity: aggregate discrepancy 9.3796381e-9, maximum-consumer
discrepancy 1.3920884e-10, and maximum mass discrepancy 1.0654716e-6. The native
report evaluates all 40 training consumers, has zero failure flags and preserves
all input bytes. Preparation and native execution have zero swap and full drain.
Receipt:
`/private/tmp/attention-matching-stored-training-parity-serial-o14-b4096-root-v12-v3/receipt.json`
(SHA-256 `7b84209420402acd0db3fc0adb865bb938f61a41bd60879824244848703dbbc6`). This proves training parity for the exact payload on
the final binary, not fresh current-binary source capture or held-out quality.

All these native measurements use the original training corpus; none supplies
fresh held-out validation or post-publication memory evidence.

### Full58 training-only V11 experiment (2026-10-19)

The immutable V11 package passed independent review and 14 focused tests,
including the generated child command, resource receipt, actual training loader
and all-36 synthetic campaign path. Its real source preparation passed all
36 historical training-byte/geometry checks in 2.53 seconds at 306.69 MiB
combined parent/child RSS with zero new swap. No gate numeric files were opened.
The generated plan SHA-256 is
`bea91e2785891e6996e98709732aca274640e9ce7026301b8987f64a4bdce39b`.

The real r288 LP-envelope experiment is incomplete: 25 terminal rows were
recorded before the global zero-swap guard stopped the next cell. Owner 4
improved from 2/12 to 7/12 training-mass passes; owner 9 has 11/12. Five owner-4
misses exhausted 12 improving key steps; the owner-9 miss stopped at a quantized
LP-envelope stall with 5.6759% stored mass error. Owner 14/block 512 returned a
terminal numerical failure (`candidate LP failed: invalid LP primal/dual
witness`). Thus the partial matrix has 18 training-mass passes, six mass misses
and one numerical failure. These are training results, not accepted profiles.

The interrupted owner-14/block-1024 attempt remained below its process budget
at 906.44 MiB combined RSS, but system swapouts grew 3,881.125 MiB. It was
terminated and drained to zero child RSS/live processes. Host power logs show
coincident clamshell sleep and low-battery hibernation, followed by an AC-power
wake. This identifies a host event, not per-process swap attribution. All 25
completed rows had passing resource receipts and zero new swap; their maximum
combined RSS was 957.83 MiB. The failed campaign, partial checkpoint and source
identities are preserved. A separately bound resume must validate the original
25 rows and complete the remaining 11 under the same recipe and cumulative
limits; neither partial coverage nor a checkpoint is an accepted substitute.

Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-serial-training-v11-campaign/campaign-receipt.json`
(SHA-256 `ae78e06e81b5e681502eab992ffa230306217e8475fe985d87f03710a4584258`).
Interruption assessment:
`/private/tmp/attention-matching-full58-serial-training-v11-interruption-assessment.json`
(SHA-256 `bcd38a1f252f9d59361a2d1f8048000ad19e073802d1bcff7e5f30eb88e1e316`).
Operational launch provenance binds the extra live resource helper to Full58;
its post-run source rehash passed. No production performance is inferred.

The separately reviewed resume V2 preserved all 25 original terminal rows and
restarted only the 11-cell complement from original V6 seeds. Owner 14/block
1024 completed with 4.999804% final stored training-mass error; its maximum
training consumer output RRMSE was 10.3016%, a diagnostic rather than a release
result. This brings the observed terminal prefix to 26 cells and 19 mass passes.
The next cell, owner 14/block 1536, exceeded the unchanged 1,024 MiB combined
parent/child limit at 1,027.734375 MiB after 305.27 sampled seconds. Swap growth
was zero and the process group drained fully. Its tenth-step checkpoint remains
nonqualifying (6.2463% mass error) and is not a terminal numerical result.

This second interruption is a fitting-workspace failure, not another swap
incident. No unchanged dense-LP retry is planned. Focused diagnostics may proceed
from the exact partial evidence to repair the solver workspace; this supersedes
the earlier complete-matrix prerequisite only for those diagnostics, without
converting the partial matrix into qualification. Prospective changes include
reuse of accepted LP witnesses, warm ordering of the same six line-search steps,
bounded stall candidates and LP constraint generation. The latter must verify
every original row and the full primal/dual witness before returning a solution.
All error, fitting-memory and publication-memory gates remain unchanged.

The exact partial-evidence decision is
`/private/tmp/attention-matching-full58-v11-partial-memory-failure-decision-v1.json`
(SHA-256 `b3dab3a738f49e45d0ef66954dec5971093ba6c42f992522d5b275cd9f0a1447`).
It binds the original campaign, failed resume, successful terminal row, failed
resource receipt and nonqualifying checkpoints. The resume's operational helper
rehash passed; held numeric tensors remain unopened in these experiments.

The focused LP witness probe V5 completed in 148.12 sampled seconds at
930.59 MiB combined RSS, with zero new swap and clean teardown. The predeclared
first candidate after six accepted key steps passed all original-coordinate
witness checks under the original solver defaults, tighter solver feasibility
tolerances, and tighter tolerances with column scaling. All three produced the
same stored-f32 mass error (28.9440%) for that rejected large-step candidate.
The historical witness failure was **not reproduced**, and this sampled matrix
is not established as the historically failing trial. The result therefore
does not justify a solver-tolerance change or claim a numerical fix. The next
constraint-generation pilot retains the original solver settings and must
establish its own memory and numerical outcome. Its independent probe decision
review is `/private/tmp/attention-matching-mass-lp-probe-decision-review-v1.json`
(SHA-256 `34c4dd91612d59d7d155600fe91cb6a67425a834307e08d7c1997cbb981d9d35`).

The subsequent three-cell constraint-generation pilot completed all three
terminal rows under the unchanged resource guard (zero new swap, complete
teardown). Owner 4/block 6144 reached 4.999804% stored training-mass error in
24 accepted steps at 737.48 MiB peak combined RSS. Owner 9/block 4608 improved
to 5.577573% but stalled, still above the 5% gate. Owner 14/block 1536 stopped
after three accepted steps with an invalid LP primal/dual witness, at
922.86 MiB peak RSS. Its early failure cannot establish the memory cost of a
completed shared-owner fit. The pilot took 370.67 sampled seconds and supplies
one training-mass pass, not a complete numerical recipe. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-32-outer-cg-pilot-v2/campaign-receipt.json`
(SHA-256 `2f9eca4bdce03c10b30724d674c6ff11549b1c85ba4d2adf0a04b38146404b90`).

A separate fixed-key/bias value-only pilot completed three cells in 5.20
sampled seconds, with peak combined RSS below 351 MiB and zero new swap.
None of its eight-iteration consumer-minimax fits improved the incoming
stored-f16 values. Maximum training consumer output RRMSE remained 15.5275%
for owner 4/block 5120, 14.0729% for owner 4/block 1024, and 10.3016% for
owner 14/block 1024. The fixed stored keys and biases retained their passing
training-mass results. These output diagnostics motivate an unregularized
fixed-attention capacity check before extending optimization; the regularized
floating-point dual values are not proof certificates. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-consumer-minimax-value-pilot-v3/campaign-receipt.json`
(SHA-256 `57384f12633bf93392208bbc9ba1b6df436e2cccd31ad05a006affe1a9ffd0cf`).

The follow-up unregularized SVD audit completed all three cells in 4.05
sampled seconds at 309.88 MiB maximum combined RSS, zero new swap and no
payload publication. For the two owner-4 cells, residual-adjusted fixed-dual
weighted-error diagnostics remain 12.7217% and 11.4504% at all three declared
rank thresholds. The corresponding arbitrary rank-288 prediction relaxations
are only 0.7907% and 0.5199%. This supports investigating output-aware key
geometry rather than extending value-only optimization or immediately spending
more retention memory. These floating-point, training-only diagnostics do not
certify a minimax bound or prove an implementable attention representation.
Owner 14's fixed-dual diagnostic is 7.4008%, but its actual stored maximum
consumer error is 11.8565%; it also remains unqualified. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-fixed-p-unregularized-svd-v1/campaign-receipt.json`
(SHA-256 `1619057592556e29f54f6b3c5f4d2f516d8f430f9f69678d87908255760f5cf6`).

The exact CG-failure probe V3 then reproduced owner 14/block 1536's witness
failure after verifying all three accepted steps and the sealed step-3 key
and bias bytes. The default solver's restricted-matrix primal violation was
4.23365e-6, exceeding the unchanged 3.66357e-6 witness tolerance. Explicit
1e-9 solver feasibility tolerances reduced the violation to 7.52e-14 and
passed every original-coordinate witness check on the same matrix. Column
scaling also passed, but was unnecessary for this case. This justifies testing
tighter solver precision while preserving the independent acceptance checks;
it does not yet establish a completed fit. The diagnostic took 73.81 sampled
seconds, peaked at 894.50 MiB combined RSS, used zero new swap and drained
fully. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-cg-failure-probe-v3-final/campaign-receipt.json`
(SHA-256 `7f24510dc16719e2277e095f012b3a7a615e87ed25d5d0b27da90e6e65356c91`).
The failed V2 launcher attempt remains preserved separately and supplies no
numerical evidence.

The ensuing full owner-14/block-1536 retry applied explicit 1e-9 feasibility
tolerances to both mass and support LPs. It progressed through 21 accepted
key steps, reaching 5.429410% stored training-mass error, before the unchanged
combined-memory guard terminated it at 1,026.25 MiB (79.25 MiB parent and
947.00 MiB child) after 223.97 sampled seconds. Swap growth was zero and
the process group drained fully. No terminal numerical cell or accepted
payload was produced. This establishes progress past the former witness
failure, but not completion or compliance with the 1,024 MiB fitting budget.
The next memory-only change defers heavyweight supervisor imports while
preserving resource accounting for fitting and final validation. Resource:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-tight-solver-o14-b1536-v3-final/resource.json`
(SHA-256 `f352be7770aafe9664d073208bf0466ede846892c45c60f1f89a9e7467ff2e4e`).

The light-supervisor V7 retry subsequently completed its bounded diagnostic
and independent result validation with a passing resource receipt: 420.86
sampled seconds, 954.0625 MiB combined peak RSS, zero new swap and full
teardown. It accepted 29 key steps before the unchanged 420-second key-fitting
deadline, with a final diagnostic checkpoint at 5.240899% mass error. It then
returned the original input payload byte-for-byte, as required on timeout.
The campaign's `success` status denotes orchestration and evidence validation;
`training_mass_passed` and `deadline_eligible` remain false. Neither a completed
numerical fit nor an accepted payload is established. LP solves consumed
418.84 seconds across 50 calls, motivating reuse of previously identified
constraint rows while retaining the full original-coordinate feasibility and
primal/dual audits. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-tight-solver-o14-b1536-v7-light-parent-final/campaign-receipt.json`
(SHA-256 `a15422a741330a8ac8c6fe6367f848ee57b54ef249d699f679a29911314625b0`).

The warm-row V9 successor reuses fully audited constraint-row hints and adds
three smaller key steps, with 64 outer steps and the same 420-second key-fitting
deadline. It starts from the original seed and changes the solver trajectory;
it is not a bitwise-equivalent memory-only optimization. Owner 14/block 1536
completed 50 accepted key steps and the support/value stages. Final stored
training-mass error is 4.999807%, within the unchanged 5% gate. The fit took
307.59 sampled guard seconds, peaked at 913.4375 MiB combined supervisor/child
RSS, used zero new swap and drained fully. Separate guarded result validation
also passed. This clears this cell's completed-fit mass and workspace blockers.
Its worst-consumer training output RRMSE remains 12.0995%, above 10%; no held
evaluation, payload acceptance or physical publication-memory claim follows.
Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-warm-cg-nine-backtracks-o14-b1536-v2-light-parent-final/campaign-receipt.json`
(SHA-256 `05343c07be76913da58a3df1dc4ca481980a2a8266f83e8f87eae1a1b2fab7f4`).

The same frozen V9 recipe completed owner 9/block 4608 with 27 accepted key
steps and 5.210373% stored training-mass error, still above the gate. Its next
smallest key step worsened mass by 9.36e-6; larger proposals also failed to
improve it. The fit took 147.50 sampled guard seconds at 636.859375 MiB combined
RSS, zero new swap and complete teardown. Independent guarded validation
passed, confirming a valid negative result. The output fitting stage was not
run because mass failed. These two focused cells do not establish a complete
36-cell recipe. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-warm-cg-nine-backtracks-o9-b4608-v2-light-parent-final/campaign-receipt.json`
(SHA-256 `0eb92b1989cc2b29062efbb97816d87ce69d237c5b84d926b8b9d82dd7ec06c1`).

The V10 owner-9 successor added three further halvings and skipped duplicate
quantized proposals within each outer iteration. It completed all 64 improving
key steps at 5.115360% stored training-mass error; termination was the outer
step cap, not a line-search stall. The final ten accepted steps all used
0.0001220703125 and changed stored f16 key elements. Resources passed at
156.85 sampled seconds, 680.90625 MiB combined peak RSS, zero new swap and
complete teardown; independent guarded validation passed. Mass remains above
5%, so the value stage was not run. This supports testing a larger outer-step
cap under the same 420-second key deadline, without resuming a checkpoint or
changing any numerical or memory threshold. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-warm-cg-twelve-backtracks-o9-b4608-v2-light-parent/campaign-receipt.json`
(SHA-256 `90d9a9d745fd7902c9e19e694d189b125370fc23fa118e5c4b97f988a99cd6d7`).

The V11 owner-9 successor raised the outer cap to 192 under the unchanged
420-second key deadline. It accepted 147 steps before a genuine quantized
line-search stall, ending at 5.043660% stored training-mass error. All twelve
LP-envelope trials and eight bounded residual fallback trials rejected the
next update. The smallest proposal changed 270 f16 elements but increased
mass error by 1.62e-7; anchor drift was only 0.06593 of the 0.375 bound.
Resources passed at 305.23 sampled seconds, 752.140625 MiB combined peak RSS,
zero new swap and complete teardown; independent validation passed. The
5% gate still fails. This motivates testing retained prequantization optimizer
state, while keeping gradients and acceptance evaluated on actual stored
keys, rather than simply extending this stalled run. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-warm-cg-192-outer-o9-b4608-v1-light-parent/campaign-receipt.json`
(SHA-256 `1082a974c720e2838edaaf586a39df655724de49b89e264e53c27ccf0a629dd4`).

The V15 shadow-state successor clears this owner-9/block-4608 training case.
It retains prequantized keys only after stored-format acceptance, while
gradients, mass checks and final gates still use actual f16 keys and f32 bias.
Starting from the original seed, 29 accepted updates reach 4.989523% mass
error; subsequent support/value fitting returns 4.999805% mass error and
9.258821% worst-consumer output RRMSE. The guarded fit completed in 89.30
seconds at 681.25 MiB combined RSS, zero new swap and complete teardown;
separate guarded result validation passed. This clears the focused training
mass/output/workspace blocker without changing thresholds, but does not
qualify the full recipe or held data. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-shadow-keys-v15-o9-b4608-v5/campaign-receipt.json`
(SHA-256 `692ff1cbc76eb86288a9c8117c32559e6f2e034960cf9ea31ed416f948926f0f`).

The unchanged V15 recipe also completes owner 14/block 1536 from its original
seed. It accepts 31 key updates and returns 4.999803% stored training-mass
error and 11.781611% worst-consumer output RRMSE. Fit resources pass at 215.42
seconds, 956.21875 MiB combined RSS, zero new swap and full teardown; separate
guarded validation passes. This improves the V9 output result but still misses
10%. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-shadow-v15-owner14-b1536-v1/campaign-receipt.json`
(SHA-256 `6662a2ee617603ffd15997b34feae5351e3581767020077908644e5099d19b1f`).

The same V15 recipe on owner 14/block 4096 is resource-rejected. After 26
accepted key steps, its last nonqualifying checkpoint has 5.533649% mass
error, but combined parent-plus-child RSS reaches 1,032.390625 MiB and trips
the unchanged 1,024 MiB guard at 190.40 seconds. Swap growth is zero and the
child group drains completely. There is no completed qualifying payload and
the checkpoint is not a resume or acceptance authority. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-shadow-v15-owner14-b4096-v1/campaign-receipt.json`
(SHA-256 `613ff8559a237d84738453380df6df2f9f02ae4f942442481ed022419cb629e5`).

A separate fixed-key convex-value diagnostic on the earlier owner-14 V9
payload converges to a floating minimax upper RRMSE of about 10.574%, with a
numerical dual value of about 10.573%. The proposed values overflow f16,
so the solver correctly returns the original 12.0995% payload. The fit uses
2.86 seconds and 250.21875 MiB combined RSS, zero new swap and full teardown.
This is evidence about the earlier V9 attention basis, not the new V15 keys
or a certified global bound over possible keys. Result:
`/private/tmp/attention-matching-owner14-convex-value-v1/value-fit.json`
(SHA-256 `eba14c0a8271dbb298d2f9dfbf07f9e31f27b49f244973e4ed40663a93480c22`).

The corresponding convex-value V2 diagnostic on the newer V15 owner-14
payload produces finite stored f16 values and reduces worst-consumer output
RRMSE from 11.781611% to 10.605331%. Aggregate output RRMSE is 9.816293%,
but the unchanged per-consumer 10% gate still fails. Keys and bias remain
byte-identical, and recomputed mass error stays at 4.999803%. The guarded
diagnostic completes in 2.85 seconds at 256.578125 MiB combined RSS, zero new
swap and full teardown. This is a training-only phase result. Result:
`/private/tmp/attention-matching-owner14-convex-value-v2/value-fit.json`
(SHA-256 `515a85368790e8c724af331c6aadb0c408c3b8f8142db395d5f5d067414acdf4`).

The corrected tangent-projection V3 phase on that exact terminal V15
owner-14/block-1536 payload passes both training targets. It recomputes the
10.605331% convex-value baseline, then accepts one projected key step with
closest-feasible bias repair, returning 9.722081% worst-consumer output
RRMSE and 4.999805% stored mass error across 3,000 training queries and 40
consumers. The linearized witness violation is 2.07e-8, below the unchanged
1e-7 limit; actual nonlinear stored checks also pass. The fit completes in
31.56 seconds at 993.03125 MiB combined RSS, zero new swap and full teardown;
separate validation passes at 279.71875 MiB. This is a successful training
phase with narrow workspace margin, not a fresh end-to-end recipe or held
qualification. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-tangent-owner14-b1536-v1/campaign-receipt.json`
(SHA-256 `df200da229f71d4bcb5b844b056f9b7bfa6df5fa1e6d2d26152ce6ba48538920`).

The first executable tangent-projection multiouter pilot V3 completes and
independently validates, but returns the same three ordinary accepted steps
and 14.159493% worst-consumer output RRMSE for owner 4/block 5120. All six
tangent proposals fail the existing linearized feasibility witness: violations
of approximately 2.77e-7 to 6.30e-7 exceed the 1e-7 limit. The projection solver
stops on relative objective reduction before satisfying that witness; no
tangent proposal reaches stored nonlinear acceptance. This identifies an
optimizer stopping issue, without establishing whether a sufficiently solved
tangent direction would improve the payload. Fit resources pass at 24.69
seconds and 694.921875 MiB combined RSS, zero new swap and full teardown;
separate guarded validation passes. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-tangent-multiouter-v3/campaign-receipt.json`
(SHA-256 `5e138c9fd1cbfdf486761a57c840b36c63a0cb8e0f8feddb7d38a7df833f6a00`).

The first output-aware key pilot's child completed within its resource bound
(3.69 sampled seconds, 328.34 MiB combined RSS, zero new swap, clean teardown).
Its largest predeclared key step reduced the floating weighted objective from
0.01618409 to 0.01453579, but the support LP reported that candidate infeasible
under the unchanged 5% mass constraint. Zero steps were accepted and the
returned payload matched the input. Smaller predeclared key steps were not
polished by this pilot, so it does not rule out a smaller feasible step.
The parent then failed because its independent validator had not configured
the pinned source loader. The campaign remains failed; a separate guarded
validation sidecar must check the saved child bytes without rerunning or
relabeling the original campaign. Failed campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-key-pilot-v3/campaign-receipt.json`
(SHA-256 `3e10a2def51b7b6967ec7e17208bf497d0e4b436027d0d68d722563211628926`).

The separate posthoc validator V2 subsequently passed in 0.95 sampled seconds
at 159.56 MiB combined RSS, zero new swap and full teardown. It configured the
exact source loader, recomputed all stored output/mass/drift metrics, and
confirmed zero accepted steps and byte-identical return of the input payload.
No fit was rerun, and the original failed campaign remains unchanged. Sidecar:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-key-pilot-v3/posthoc-validation-v2.json`
(SHA-256 `571ea9d310bd7fda22cb5f4db1546975953b5843e532e55f493f298c681f46a0`).

The ranked-backtracking successor V5 completed and independently validated all
six predeclared proposals in 49.39 sampled campaign seconds. Peak combined RSS
was 459.56 MiB, swap growth was zero, and teardown completed. Five smaller
proposals satisfied the unchanged stored mass limit, but their maximum
consumer output errors exceeded the incoming 15.5275%. The largest proposal
was mass-infeasible. No step was accepted and the emitted payload was
byte-identical to the input. This narrows the next experiment to minimax value
reoptimization and continuity of the bias fit after key changes; lower weighted
average error alone does not satisfy the worst-consumer requirement. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-key-pilot-ranked-v5/campaign-receipt.json`
(SHA-256 `7f28501c660362f164dbe1836741acaa8a98a49330bbe28a0751d63b8621b071`).

The corrected convex consumer-minimax pilot V4 completed and independently
validated one accepted key step for owner 4/block 5120. Reoptimizing values at
unchanged keys first reduced the stored worst-consumer RRMSE from 15.5275% to
14.3556%. The accepted 0.0078125 key step then reduced it to 14.2469%, with
4.999806% stored training-mass error. The larger 0.03125 proposal was
mass-infeasible and the 0.015625 proposal failed its minimax convergence check;
both were rejected. Fit resources passed at 9.39 sampled seconds,
446.84375 MiB combined RSS, zero new swap and complete teardown. Separate
guarded validation recomputed the actual stored output and numerical witness.
This establishes a feasible improving key direction, not the required 10%
output result or a complete recipe. Receipt:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-minimax-key-pilot-v4/campaign-receipt.json`
(SHA-256 `73c43110fa5d92bff5cbb6a88c30cb40308e5512d9573d68c58644a8eaa80390`).

The first multiouter extension then emitted three improving diagnostic
checkpoints, the last reporting 14.159493% stored worst-consumer output error.
The child failed on an unsuccessful closest-bias LP result (`HiGHS Status 0:
Not Set`) before returning a final payload. The campaign is failed, with no
independent final validation; checkpoint metrics cannot be promoted to a
qualified result. Its guard observed 23.99 seconds, 570.015625 MiB combined
peak RSS, zero new swap and complete teardown. A successor rejects unusable
solver vectors or witnesses as candidate-local failures and continues the
bounded proposal list, while preserving strict input validation and all
stored numerical gates. The original failed receipt remains unchanged:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-output-aware-minimax-key-multiouter-v1/campaign-receipt.json`
(SHA-256 `5026924e6b4ace84e5dbd9be6e8abde46d1be7c44539deffb8b4c3c1e4c24c1c`).

The corrected multiouter V2 child completed at the same three-step result:
14.159493% stored worst-consumer output error and 4.999808% training-mass
error. The next ranked proposal list exhausted: larger steps were infeasible
or solver-rejected, two smaller feasible bias repairs produced rank-deficient
value problems, and the smallest failed stored-value materialization. The fit
used 31.87 sampled seconds and 639.671875 MiB combined peak RSS with zero new
swap and complete teardown. Its parent failed during independent checkpoint
report comparison and remains failed (campaign SHA-256
`0df8b165fbf02235dd3b830aefb238f3bf21417536b6b8613ac5d151423e3431`).

A separate guarded no-fit diagnostic traced that comparison failure to
floating-point evaluation order: the independently factored attention and
stable-softmax attention differed by at most 6.67e-16; reported consumer metrics
differed by at most 2.78e-17 and matched the stable-softmax reporting formula
exactly. A formula-aware sidecar retained the independent factored numerical
gates, compared reports against their original formula, and passed full stored
payload/checkpoint validation. The V2 sidecar's deep resource checks passed
at 1.25 seconds, 222.0625 MiB, zero new swap and complete teardown. It neither
reran fitting nor converted the original campaign into a success. The 10%
output target remains unmet, motivating joint key/bias candidate updates.
Sidecar: `/private/tmp/attention-matching-multiouter-v10-validation-sidecar-v2/validation-sidecar.json`
(SHA-256 `a6ecab1c0d37435bb256859dee0a9252ef7b34cb62423e2bc83d9f0d7a55a11c`).

A training-only target-rank diagnostic computes optimistic worst-consumer
RRMSE floors of 0.7897% at r288 and 0.3209% at r352 under uniform normalized
consumer weights. The returned multiouter dual gives still smaller floors.
These floating SVD bounds do not rule out the 10% target; they also do not
establish attainability with softmax attention, stored f16 payloads, mass
constraints or the chosen anchor radius. The diagnostic used 0.92 seconds,
148.39 MiB combined RSS, zero new swap and complete teardown. Result:
`/private/tmp/attention-matching-output-rank-feasibility-v2/rank-feasibility.json`
(SHA-256 `1b3409b0530478ab89bc79eaba02f9a811c82ac344badcf56cd3baf335e269d8`).

The owner-9/block-4608 support-stall diagnostic V4 completed its numerical
child in 7.58 sampled guard seconds at 404.27 MiB combined RSS, zero new swap
and full teardown. The primary minimax solution had 72 weights at the lower
bound. An alternate support solution removed those floor weights, but its
gradient cosine with the stalled direction was 0.999999999984 and its pairing
with the primary dual failed interior stationarity. It therefore declared no
distinct usable direction and ran zero descent trials. This particular
near-optimal support redistribution did not establish a way out of the stall.
The parent incorrectly required the cross witness to pass even for this valid
negative diagnostic and emitted no campaign receipt. A separate no-fit audit
confirmed the negative-path semantics; the failed parent execution and original
child evidence remain unchanged. Child result:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-owner9-support-stall-v4-light-parent-final/diagnostic.json`
(SHA-256 `37ad8c88a07923ea67b0bc2ec9ce7d83ac967d96e94b8b99f5db61fae6ba5716`).

### Numerical and memory milestone clarification (2026-10-19)

A fresh read-only allocation and profile audit distinguishes the old 32/36
training prerequisite from a deployable profile. The controlled E2B 8K
post-publication baseline is 58,228,736 bytes. Hypothetical 32-block r288
compaction with four exact blocks would save 14,630,912 bytes (25.1266%), but
current profile identity and coordinator retention are fixed per owner/head;
per-block exact fallback is not supported. Uniform r288 therefore requires
all 36 blocks to satisfy the unchanged numerical gates. At 36/36 its projected
saving is 28.2780%, before the required actual post-publication measurement.

The current ABI permits different fixed retentions by owner. For example,
assigning 288 rows to one owner and 320 to each other owner projects 25.5768%
savings, including page-rounded K/V, bias, metadata, exact tail and reserved
continuation. These are alternatives for a separately frozen training recipe,
not accepted numerical results. The r288 experiment's unchanged >=32 training
mass prerequisite permits further held study only; it does not establish a
serving-compatible profile. The audit is
`/private/tmp/context-e2b8k-r288-memory-audit-v1.md`
(SHA-256 `90125e2f60c87b7bd016a8b88fce226d701c6b5e9dfbe33cce2d5a5abd28d5bc`).

The alternative tuple (352,288,288), ordered by owners (4,9,14), projects
14,876,672 bytes saved (25.5487%) with 319,488 bytes of margin above the
25% requirement. All twelve owner-4 r352 seeds were reconstructed from the
original exact source and training-query metric and frozen before any r352
fit. The serial seed campaign completed in 14.04 seconds, with maximum
combined RSS 206.75 MiB, zero new swap and full child teardown. No held tensors
were loaded and no fitting occurred; projected savings still require actual
native publication measurement. Seed campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-owner4-r352-seeds-v4/campaign-receipt.json`
(SHA-256 `16a5c19aef98f4339c573cd0f095b69055f6c52291fe83fe4568df582b360afb`).

The first r352 fit on owner 4/block 5120 passes both training numerical
targets. Starting from the original r352 seed, five accepted shadow-key
updates reach 4.882774% mass error; support preservation and convex-value
fitting return stored mass error 4.999804% and worst-consumer output RRMSE
7.543941%. The final value solve is full column rank with condition about
3,499, and support preservation leaves no weights at the floor. The guarded
fit takes 32.65 seconds, peaks at 644.828125 MiB combined RSS, records zero
new swap, and drains fully; separate stored-result validation passes. This
clears the focused owner-4 output blocker within the projected heterogeneous
memory budget. It does not establish the remaining eleven owner-4 cases,
other owners, fresh held evaluation, or measured native publication. Campaign:
`/Volumes/Extreme/antfly/qualification/attention-matching-full58-r352-owner4-b5120-fit-v9/campaign-receipt.json`
(SHA-256 `302d193cbaf297e858dc4af8df2f70c5c708097042484d91b327ae507d502e6f`).

### Fresh full58 installed-model coverage

The coverage run bound to full58 completed in 136.11 seconds with one accepted
case: 8160 rendered prompt tokens and a 32-token reserve, exactly 8192 context
tokens. The loaded E2B artifact, tokenizer, template, layout and implementation-v3
identity matched the sealed inputs. The prompt and question token hashes also
match the earlier full56 capture. Peak sampled process-group RSS was
3301.72 MiB, minimum reported free memory was 31%, and new swap growth was zero.
Shutdown returned zero and reported zero remaining process-group RSS. This
collector does not report a final live-process count, so that measurement is
not inferred. Its 64 MiB child file limit with SIGXFSZ ignored remains scoped
to coverage; replay uses its ordinary file policy under the existing resource
guards.

The session is
`/Volumes/Extreme/antfly/qualification/context-replay58-coverage-v16/session.json`
(SHA-256 `949a1bae90cae8d4c335d0a4a50c86b436329f09ac4b4431fe6364f97b87f217`).
It records `serving_qualified=false`. Coverage establishes loaded identity and
token boundaries; numerical compaction, quality, replay performance and serving
qualification remain separate gates.

The following matched replay attempt stopped during startup of its first serial
control arm: 179.8125 MiB of new system swap exceeded the unchanged zero-swap
limit. It ran 20.46 seconds at a sampled peak of 3455.44 MiB group RSS and 25%
minimum reported free memory; termination returned -15 and cleanup reported
zero remaining group RSS and zero live descendants. No arm was accepted and no
replay performance comparison is available from this attempt. Its receipt is
`context-replay58-coverage-v16/matched-replay-cost-v16/campaign-receipt.json`
under the external qualification root (SHA-256
`b703f8613581491036cbba934d4a118bfcaf2a993f16fd9144d882520917a572`).

The separately guarded full58 exact-quality materializer reached the corrected
resolved model directory, then failed its first request with
`ResourceTemporarilyUnavailable`. Runtime admission required 4,186,895,320 bytes;
available memory minus the unchanged 6 GiB reserve was 3,865,470,566 bytes,
306.53 MiB short. This is an admission failure, not a completed model-load or
quality result. The outer run took 11.83 seconds at 626.89 MiB peak group RSS,
59% minimum reported free memory and zero new swap. It exited with code 1,
without an outer resource violation, and drained to zero group RSS/live
processes. Zero corpus reports were accepted. Resource receipt:
`/Volumes/Extreme/antfly/qualification/context-quality-e2b-8k-exact-58-v1-resources.json`
(SHA-256 `552f9b74ab7e0d074401994d43246803a10934c7fb57377a1f6f5885829a8455`).
The raw driver error remains in its packing journal; the parent receipt's
generic unsuccessful-guard message does not identify the underlying admission
failure. Both failed attempts are preserved; retry evidence must use fresh
paths and unchanged runtime/resource policies.

Reviewed replay retry authority is
`/private/tmp/context-replay58-retry-bootstrap-v18.json`
(SHA-256 `f498f2b2c08a90113ac20d41cebf02c287d7900b093f2eafdb9f4f495b96fb42`);
it changes only the matched-output directory to `matched-replay-cost-v17`.
The independently reviewed fresh quality retry handoff is
`/private/tmp/context-quality-e2b-8k-full58-retry-handoff-v6.json`
(SHA-256 `6ee88235e05d9a388e05e09981f7ca072fe786335655bd7707e8fbc941570b3c`),
with versioned `exact-58-v2` outputs. At that checkpoint neither retry had run;
the subsequent replay outcome is recorded below. A quiet-window
preflight requires at least 65% reported free memory for 60 seconds with no
swapout-counter increase; the runtime guards remain unchanged. The observed
one-minute window after the failed attempts stayed at 61–62% free with no new
swap, so model retries remain pending. No numerical or performance acceptance
is inferred from readiness or launch-package validation.

The next36 full58 V7 preparation is a non-executable template, independently
reviewed in that scope. Its handoff is
`/private/tmp/attention-matching-next36-full58-template-handoff-v7.json`
(SHA-256 `42debfe967fe28e62ac128eeebfc49ed683fe37a3b35c8eb9c1732f4ccfb4b46`).
It fixes the 36 owner/block cells and selects the serial control as training
source only after all three schema-7 replay variants pass canonical parity.
Replay, review, authorization, new manifest and parity references remain null.
Historical V6 payloads are initializers only: exact base K/V, training-query,
response-token, position and geometry parity is required, and all metrics must
be recomputed. Four synthetic tests pass; no fit or held-data access occurred.
Final executable resealing must derive every missing edge from the canonical
replay validator and actual full58 adapter; the template itself grants no run
or release authority.

After the user freed RAM, the replay V18 readiness window passed with at least
74% reported free memory and zero new swap for 60 seconds. The retry in
`context-replay58-coverage-v16/matched-replay-cost-v17` accepted its serial
control: 456.24 seconds of sampled elapsed time, 4312.94 MiB peak group RSS,
zero new swap, return code zero and zero remaining group RSS/live processes.
Independent review validated its schema-7 identity, all 36 base blocks,
training-source graph, serial-route counters and unchanged live-context
fingerprints. Summary SHA-256:
`60c3c107d5a13a1cbe38a23ad5e01271fb4a3606c6f8a5cec97a70630dd08abc`.

This control overlapped host sleep: first sleep at 17:30:13 PDT and full wake
at 17:41:59, a 706-second wall interval containing 633 recorded sleep seconds.
Its data and route checks remain usable, but its timings cannot support a
performance claim. Bounded power evidence receipt:
`/private/tmp/context-replay58-v17-power-log-receipt-v3.json`
(SHA-256 `26a315214bf56593406d520f802483c3dffa18eb2940b0bcee4200b88c1c8256`).

The subsequent prepared-decode arm started with 60% reported free memory and
stopped at 155.00 seconds when new swap reached 16.8125 MiB. It peaked at
3686.89 MiB group RSS, reached 20% minimum reported free memory, and terminated
with -15 before export; cleanup reported zero remaining RSS/live processes.
The split16 arm did not run. The parent campaign is failed with one accepted
control, not a completed three-arm comparison (receipt SHA-256
`bac1aaa9c0a541f3c358518c58c86db9bd9dc99d31ba9d23c0c7d3959a7edeab`).
Optimized-route parity and all performance gates remain open. The next replay
launcher must check readiness before every arm and preserve this partial run.

A separately versioned serial-only training diagnostic is being prepared from
the independently accepted control. It grants no optimized-route, profile,
publication, quality or release authority. It must use a training-only input
path: the existing full assembler opens validation tensors and is unsuitable
for this boundary. Only base K/V, question and fit sources may be numerically
loaded; gate/evaluation references remain opaque metadata. All 36 cells, exact
historical-initializer parity, fresh metrics and the existing 1 GiB/zero-swap
guards remain required. This separates numerical investigation from the still
unmet optimized-route qualification prerequisite without changing release gates.

The reviewed replay V19 handoff is
`/private/tmp/context-replay58-per-arm-retry-handoff-v19.json`
(SHA-256 `ce4deaa5f295f7d7355124add757efc7588cd97f24733e0c30b754ae4ba841f5`).
It copies exactly the 26 sealed coverage files to a fresh root, excludes all
historical matched captures, and enforces readiness and complete authority
revalidation before every arm. Four focused tests pass; it has not run.

A separate, unlanded continuous-timing patch adds strict clock evidence to
performance, soak and serving-route collectors. It rejects wall/monotonic clock
divergence over two seconds without extending existing deadlines. Default
build/correctness receipt behavior stays unchanged. Five focused tests and
independent review pass, including cumulative drift, actual live-child drain,
strict producer/auditor acceptance and malformed-evidence rejection. Handoff:
`/private/tmp/context-strict-continuous-timing-v2-handoff.json`
(SHA-256 `9370fa99b4b4635815a6968f7f08261dcc43890aa5d7171ccd7a36a92e3f56d5`).
It remains isolated from the frozen full58 source and is not installed policy.

The first serial-training V9/V9.2 package is explicitly non-authoritative after
review found gate-file reads through its provenance helper, an old plan hash
in child commands, an undeclared old-plan dependency and missing training-source
integrity checks. No source assembly or numerical cell was run from it. A
successor must pass an end-to-end preparation/runner test with validation files
absent before actual source preparation is authorized.

### Focused validation after build 48

Build 48 stopped before tests on its swap-growth guard; resource receipt:
`/private/tmp/antfly-context-full-release-build-48/build-resource.json`
(SHA-256 `2c5be87801df299b150e08ce80d354d6f110c22fc247e820eb6faf973c8b3c4d`).
It produced no eligible full-release receipt.

Build 49 included the schema-6 replay-observation integration and split32
diagnostic selector. It also stopped before tests on the unchanged system swap
guard: 605.5625 MiB new swap, 448.75 seconds elapsed, 3837.69 MiB peak group RSS,
and 34% minimum reported free memory. Resource receipt:
`/private/tmp/antfly-context-full-release-build-49/build-resource.json`
(SHA-256 `d1d6a0c40ea8da7eacb49312152ad586b1c1c9526e7c47a66ab432665c64c55d`).
This is an environment-limited run, not a successful compilation or a source
failure verdict. Its source archive and logs are preserved; no eligible
full-release receipt was issued and no dependent installed-model campaign ran.

After the user freed memory, build 50 started at 68% reported free memory.
It still stopped before tests on the unchanged swap-growth guard after
555.52 seconds: 524.8125 MiB new swap, 3509.53 MiB peak process-group RSS,
and 31% minimum free memory. The full source archive and logs remain intact;
no eligible build receipt was produced. Resource receipt:
`/private/tmp/antfly-context-full-release-build-50/build-resource.json`
(SHA-256 `d2c1ab167448788eb24aa67b7877df99f9ab917909aaa69115a6b40fd9628fc2`).
The matched loaded-model replay campaign therefore remains unrun. An existing
stripped-build option is being assessed as a separate diagnostic build profile;
it is not a relaxed memory guard or existing release eligibility.

The separate stripped diagnostic 51 completed the full selected context suite:
421 selected, 404 passed, 17 opt-in skips, zero failures and zero leaks. It took
527.12 seconds, peaked at 3159.22 MiB process-group RSS, maintained at least
40% reported free memory, and generated zero new swap under the original
8 GiB / 512 MiB swap-growth limits. The archived binary is 32,537,456 bytes,
SHA-256 `1036729434356431633c0c2351248b29f5cca10d7759a453ddf451ff1cae8c6e`.
The complete 5,759-file source archive, logs, resource receipt and executable
hashes were rechecked after completion. Diagnostic receipt:
`/private/tmp/antfly-context-strip-build-diagnostic-51/strip-build-diagnostic.json`
(SHA-256 `21066e940acfb1e3f4632f7b2e18d042ffc77ace8d2c37781fd1ee7a3f953ab3`).
This removes the observed compilation/resource obstacle for that diagnostic
profile. It cannot be promoted into a release receipt. The implementation-v2 runtime identity used by diagnostic 51 did not encode
strip/error-tracing state, so its successful build alone cannot establish
release eligibility.

The subsequent reviewed change binds the compiler's actual `strip_debug_info`
and `have_error_return_tracing` values into implementation identity v3. A new
strict full-release receipt v2 supports only the canonical stripped command,
requires one matching compiler-emitted profile observation, and preserves the
existing v1 schema and command. Diagnostic receipts remain rejected. Forty-two
related Python tests passed, including malformed metadata and backward
compatibility cases. A focused Zig run passed 4/4 in 11.05 seconds, peaked at
374.84 MiB RSS and generated no new swap; it observed exactly
`strip_debug_info=true` and `have_error_return_tracing=false`. Receipt:
`/private/tmp/context-compiler-profile-focused52-receipt.json`
(SHA-256 `5ec30b92fa7663909980190bf445677653a315756144691cc58bfd23401b8594`).
Fresh strict full-release build 52 completed on this changed source: 422 selected,
405 passed, 17 opt-in skipped, zero failures and zero leaks. It took 638.33 seconds,
peaked at 4,012.03 MiB process-group RSS, maintained at least 39% reported free
memory and generated zero new swap under the unchanged build guards. The full
build adapter independently revalidated the source archive, binary, logs, resource
receipt and actual compiler-profile observation. Eligible build receipt:
`/private/tmp/antfly-context-full-release-build-52/full-release-build.json`
(SHA-256 `746a954126d39d255060a5068a88aa1eb67939e8876122379991be9012e0034b`).
Executable SHA-256:
`c42b485b7c601cd9fe8fa2e753bb7b4b6696f6147c9dbf2ac906141f4b2ad5d5`.
Source archive SHA-256:
`c39faa4708bc0527ad03037f1c950fb60bb01a703b6dee0a8f9a66e9ed12826f`.
Resource receipt SHA-256:
`3109b5e397713b82b0a36a9c213e9bc862c2d29c1db402fa749aa6357e90d12a`.
This clears the full-build prerequisite for this exact stripped profile. The
17 opt-in skips include installed-model qualification; no loaded-model,
clean-memory performance, serving or compaction qualification is inferred.

The first installed-tokenizer preparation attempt using build 52 rejected its
v2 receipt before accepting any report: the narrow Zig runtime receipt parser
still recognized only v1. This was an integration failure, with no resource
violation, 12.56 seconds elapsed, 295.5 MiB observed launcher-group peak RSS
and zero new swap. The driver was in a separate process group, so this RSS
measurement excludes the driver and is not a total-memory qualification. The
failed campaign and terminal parent receipt remain intact:
`/private/tmp/antfly-context-quality-e2b-8k-exact-52-parent.json`
(SHA-256 `f9f9b83312842ac5f69921e78be52155b10802d262034bfb044d3e3e3e8e38a5`).
The runtime parser now accepts v2 only when its exact two boolean compiler
fields are canonical and match the actual executable's builtins. V1 retains
its existing handling of unknown fields. Independent review found no blocker;
the focused stripped build passed 6/6 selected tests in 9.87 seconds, at
366.17 MiB peak RSS and zero new swap. Receipt:
`/private/tmp/context-runtime-build-binding-focused53-receipt.json`
(SHA-256 `447d3010cd5600bea8aa40ae423248c4da27a807bf7f067af4e55e363adcc42e`).
Fresh full build 53 passed on this corrected source: 425 selected, 408 passed,
17 opt-in skipped, zero failures and zero leaks. It took 411.78 seconds, peaked
at 4827.44 MiB process-group RSS, maintained at least 55% reported free memory
and generated zero new swap. The strict build adapter independently revalidated
its evidence. Full-release receipt:
`/private/tmp/antfly-context-full-release-build-53/full-release-build.json`
(SHA-256 `506319a75ca5fa7b6525acbf874d0f3ace7c4a9ded1e9897c1e483a3b19a4579`).
Executable SHA-256:
`85624d93247e6c373daf7a6a0c95cd73497ee70862b89fe77a784ada5b862e49`.
The failed preparation attempt establishes no tokenizer packing, loaded-identity
or quality pass; the successful build does not close those installed-model gates.

The first real coverage campaign using build 53 exited with SIGXFSZ (return code
-25) before reporting any trial, after 2.81 seconds at 238.02 MiB observed RSS,
64% minimum reported free memory and zero new swap. The inherited 64 MiB file
size limit remains unchanged. Model startup's temporary mmap copies are a
suspected cause, pending a focused regression and a fresh installed-model run.
The failed session is preserved at
`/private/tmp/antfly-context-v19-structured-8k53-v8/session.json`.
The separate tokenizer preparation then failed its first response's loaded
identity check, accepting zero reports in 9.78 seconds at 297.44 MiB observed
launcher-group peak RSS and zero new swap. Its driver was likewise outside the
measured group; the resource receipt does not establish total driver RSS.
The collector did not retain that rejected raw response;
diagnosis must establish the underlying driver error before claiming a fix.
Its parent receipt remains at
`/private/tmp/antfly-context-quality-e2b-8k-exact-53-parent.json`.

A fresh bounded diagnostic preserved the first raw preparation response:
`{"completed":false,"error":"ModelNotLoaded","request_index":0,"schema_version":2}`.
The driver was fresh, while materialization required an already-loaded model.
It now uses the same coordinated acquisition path as loaded-provenance export,
holding one model handle across identity capture and rendering. The diagnostic
took 7.32 seconds at 291.17 MiB peak RSS with zero new swap; it accepted no
materialization report and is not a quality pass. Raw response:
`/private/tmp/antfly-context-quality-e2b-8k-exact-53-first-report-diagnostic-v1/diagnostic-first-driver-report.json`
(SHA-256 `27966f2aafb6b6346e22bd8769226559e9f7b1adad452464ddc71934cb2faab8`).
The repository packing collector now validates and journals the structured
driver error before raising it, preserving the real failure instead of a
generic missing-identity error. Its failure, tokenizer materialization and
ordered tool-history checks pass 24/24; this does not replace an actual model
packing run.

The temporary mapping helper now checks its inherited file-size limit before
creating or truncating a file. Over-limit copies return a typed error, allowing
the existing owned-memory fallback without raising the file limit. Separate
child-process controls used a 4096-byte limit: the original helper terminated
with SIGXFSZ at 4097 bytes, while the corrected helper byte-compared an exact-limit
mapping and returned `FileSizeLimitExceeded` at 4097 bytes. The controls passed
in 12.98 seconds at 340.36 MiB peak RSS and zero new swap. Receipt:
`/private/tmp/c-file-rlimit-regression-v2.json`
(SHA-256 `ae76ec8ac881af64c1c26977aadfea11ac07dd3cf49feaebd14693ef26f24c1d`).
The actual-limit test is isolated from the shared test process; repository tests
check pure limit boundaries. This proves the helper defect and fix, while a
fresh installed-model run must still establish the original startup failure's
callsite and successful fallback under the unchanged RSS and swap guards.

Fresh full build 54 passed after these startup and failure-reporting changes:
425 selected, 408 passed, 17 opt-in skipped, zero failures and zero leaks.
Elapsed time was 455.38 seconds, peak process-group RSS 3492.52 MiB, minimum
reported free memory 41%, and new swap growth zero. The strict adapter revalidated
the complete build, and all four changed source/test files match its archive.
Receipt: `/private/tmp/antfly-context-full-release-build-54/full-release-build.json`
(SHA-256 `b5effc0a341cfacae52456bba2c73fcdc899fcd6367fdb7abb225882e48c0c74`).
Executable SHA-256:
`8860f2996f0a7405f6d1e9bdd414f727b74be1ee396ad13f8d01fff6a3066641`.
Source archive SHA-256:
`21bd52844b0e0035d0b8c37065f2cfe416788257de79b9ac6082f41ff2e1fbee`.
Actual installed-model coverage, tokenizer preparation and serving remain open.
The full54 coverage attempt still terminated with SIGXFSZ before its first
report, after 3.08 seconds with 238.00 MiB observed peak RSS, 64% minimum free
memory and zero new swap. Session:
`/private/tmp/antfly-context-v19-structured-8k54-v10/session.json`
(SHA-256 `04a587095e636e66c4cb0fd08541311ecdff5ee812c0d326b5c9e76d58c9f505`).
The temporary-mapping regression therefore proves a separate helper defect
and safe fallback, not the cause or resolution of this real startup failure.
The 64 MiB file limit remains unchanged while the failing call is traced.
The bounded LLDB diagnostic could not launch this executable: LLDB itself
terminated with SIGBUS while `ObjectFileMachO::ParseSymtab` read the stripped
target during `SBDebugger::CreateTarget`. Disabling symbol preloading reproduced
that debugger failure before target execution. This is not a model result.
The preserved diagnostic resource receipt reports 4.40 seconds, 286.16 MiB
peak group RSS, 65% minimum free memory and zero new swap:
`/private/tmp/antfly-context-sigxfsz54-lldb-v4-resource.json`.
Crash-report identities and the second reproduction are recorded in
`/private/tmp/context-sigxfsz54-lldb-failure-review-v1.md`
(SHA-256 `9a08b47ec976f044e5ceeb5470d7a6891189d9801f8505e41ac99a8c28eaa002`).
A subsequent diagnostic with a hash-pinned syscall interposer identified the
actual failing operation: Metal's compiler filesystem cache called `ftruncate`
for 83,476,480 bytes on `com.apple.metal/32024/libraries.data`, exceeding the
unchanged 67,108,864-byte limit. The stack passes through `libCoreFSCache`
(`heap_alloc`, `cvmsPoolAlloc`, `fscache_insert_and_retain`) and
`MTLCompilerFSCache::addElement`. The driver retained the original SIGXFSZ exit.
Raw evidence: `/private/tmp/antfly-context-sigxfsz54-interposer-v1/driver.stderr`
(SHA-256 `42937dedd394fa1d439d4d97bb49a6ac70435b8281ac79a37ab2351edbaacfdc`).
The diagnostic wrapper initially rejected this real trace because Zig's test
progress text shares its first line; that failed wrapper receipt is preserved.
This identifies a framework-cache operation, not a compaction output file.
Injected execution provides diagnostic evidence only and cannot qualify speed,
serving or numerical behavior.

The qualification collector now preserves the exact 64 MiB soft and hard
file-size limit while setting only the child's `SIGXFSZ` disposition to ignored.
Over-limit writes and truncates return `EFBIG`; the production entrypoints are
unchanged. Actual syscall and `/bin/dd` controls prove the limit remains enforced,
an under-limit report survives, and the default-disposition control still dies
with `SIGXFSZ`. Current schema-2 raw evidence requires the exact recorded policy;
missing or altered policy is rejected. Independent review approved the frozen
v2 patch, and root verification passed 76 collector, provenance, failure,
serving and command-I/O tests. Landing evidence:
`/private/tmp/context-child-file-limit-landing55-v2/landing.json`.
This proves bounded failure delivery, not that Metal's private cache recovers.
A fresh full-build-bound coverage run must still demonstrate real model startup
and completion under the unchanged memory, swap and deadline guards.

The quality packer's nested `start_new_session=True` also placed its persistent
driver outside the outer resource guard's process-group RSS sampling and final
cleanup. No new quality run is authorized by those incomplete RSS observations.
The correction is now in the worktree. The outer guard issues a one-use,
parent-bound pipe witness; the packer consumes it before spawning its driver,
then rejects a driver outside the owned group before sending a request. The
outer guard samples surviving workers after leader exit, drains the group,
verifies zero non-zombie members and RSS, and samples system resources again
after cleanup. Witness rejection is terminally journaled with handles closed.
Root verification passed 12 resource/ownership regressions and 24 preparation
and structured-error regressions. Landing evidence:
`/private/tmp/context-guard-ownership-landing55/landing.json`.
The fresh quality launcher must require the new teardown evidence and bind
the changed source. Full build 54 predates this harness correction; it is not
a verification of the current Python changes. This establishes no tokenizer,
quality, performance or serving pass by itself.

Fresh full build 55 binds both harness corrections and passes the complete
425-test selection: 408 passed, 17 opt-in skipped, zero failures and zero leaks.
It reused the compiled executable from build 54; guarded build/test execution
took 26.24 seconds at 404.64 MiB peak group RSS, with at least 79% reported free
memory and zero new swap. Final process-group RSS and live-worker count were
both zero. The strict adapter revalidated the executable, resource evidence,
logs and 5,762-file source archive, including the changed Python sources.
Receipt: `/private/tmp/antfly-context-full-release-build-55/full-release-build.json`
(SHA-256 `95af717b16a79d848103c5fa79571dc2566b3bc80ea8fc4f6a5fd8c9d864cf49`).
Source archive SHA-256:
`67969b33f77d0fe77902d1f1b6a578843cfa293fc9612aa76c41b2f3d301a10d`.
This is current build evidence, not installed-model startup or serving proof.

The next campaign was initially blocked: internal free space subsequently fell to
3985 MiB, below the unchanged 4096 MiB floor. New large artifacts are planned
for `/Volumes/Extreme/antfly/qualification`, which has ample free space. The
resource guard now freezes the artifact, working and temporary directories and
their device identities, samples their minimum free space, and rejects missing
or replaced filesystems. All three direct collector samplers use the same
guard. An external destination therefore cannot hide an exhausted internal
volume. Sixteen resource/ownership tests and 77 collector/lifecycle tests pass;
the later strict full56 build also passed. Landing evidence:
`/private/tmp/context-multivolume-guard-landing56/landing.json`.

Four failed-build bundles (47 through 50, 663,998,775 bytes) have verified copies
under `/Volumes/Extreme/antfly/qualification/archive-pre56`. Every copied file
matches its original hash; the originals remain untouched. Manifest:
`/Volumes/Extreme/antfly/qualification/archive-pre56/archive-manifest.json`
(SHA-256 `f35f54e26f6fa5f343b818f9fc769666a3db15ee3052641697b198cba0cf264c`).
These storage preparations establish no numerical or serving qualification.
The later frozen preflight measured 3945.3125 MiB on the internal work/temp
volume and rejected launch before any build or model process. Receipt:
`/private/tmp/context-qualification-disk-preflight56.json`
(SHA-256 `42e5e782a28e23a815b225b9b8fc3a1135985e0faa4110a7a1f370af5b9a0e18`).
That checkpoint blocked further qualification on restoring disk headroom.
The user subsequently freed space; a fresh preflight measured 16953.125 MiB
across the guarded artifact, work and temp volumes, and full56 completed.
No evidence relocation is needed or authorized: the four original bundles and
their verified external copies remain intact. All resource limits are unchanged.

Full56 selected 425 tests: 408 passed, 17 opt-in tests skipped, with zero failures
or leaks. Its guarded build/test phase took 500.26 seconds, peaked at 4501.36 MiB
process-group RSS, retained at least 44% free memory and 16702.32 MiB free disk,
and incurred zero new swap. Final process-group RSS and live-process count were
both zero. The strict adapter validated the complete source archive and build
evidence. Receipt:
`/Volumes/Extreme/antfly/qualification/context-full-release-build-56/full-release-build.json`
(SHA-256 `bbcc963020615fa8749f6b21fa92ee3cbb88d49ba2d3e6ed0adbc70381318c37`).
Source archive SHA-256:
`0dfbabbeecb6ad93c077c9a5b349534e6b31479a684ef49e6e24004168aebcf3`.
Executable SHA-256:
`8860f2996f0a7405f6d1e9bdd414f727b74be1ee396ad13f8d01fff6a3066641`.
Installed-model coverage, replay timing and numerical qualification remain
separate requirements; the opt-in skips do not establish those results.

The subsequent full56 E2B coverage run passed with the unchanged 64 MiB child
file-size limit and `SIGXFSZ` ignored so oversize writes return `EFBIG`. It
validated one real loaded-model report, the expected tokenizer/template and
implementation identity, and exactly 8160 prompt tokens plus 32 reserved tokens.
It took 116.73 seconds, peaked at 3890.05 MiB process-group RSS, retained at least
48% free memory and 16709.79 MiB free disk, incurred zero new swap and exited
successfully with zero remaining process-group RSS. This direct collector does
not record a remaining live-process count. The earlier startup signal did not
recur; this run does not independently trace whether a cache write returned
`EFBIG`. Receipt:
`/Volumes/Extreme/antfly/qualification/context-replay56-coverage-v12/session.json`
(SHA-256 `d203581e548892a675851926162e2a8fdf17ffadb61e9b00ebff1e1146ccb447`).
This is prepared-payload coverage, not a passing numerical fit, quality result,
production timing or serving qualification.

The first full56 three-arm replay attempt stopped in its baseline before
producing a campaign capture. The Python launcher set
`ANTFLY_CONTEXT_CAMPAIGN_STUDY_PAGED_DECODE=0`, while the native qualification
parser deliberately accepts only `1` or an absent variable. The resulting
`InvalidReplayCaptureInput` is a launcher contract error, not a numerical or
resource failure. The attempt took 115.48 seconds under the resource guard,
peaked at 4216.94 MiB RSS, incurred zero new swap, and left zero group RSS/live
processes. The immutable failed receipt is
`/Volumes/Extreme/antfly/qualification/context-replay56-coverage-v12/matched-replay-cost-v13/campaign-receipt.json`
(SHA-256 `3264bdfdb377284a38229a4deeecf6aaa4088df3c4f5bf73ba8bcb1d29b5510e`).
The correction belongs in the launcher: omit the false flag and preserve the
strict native parser. Successful coverage and full56 build evidence remain valid.

The corrected v14 replay campaign accepted its serial baseline: question,
fit-study and gate-study took 1.904117, 152.824297 and 152.105521 seconds,
respectively (306.833935 seconds combined). The baseline process took 417.50
seconds, peaked at 4865.78 MiB RSS and incurred zero new swap. The singleton
paged arm also completed its model run in 427.08 seconds with zero new swap,
but the campaign rejected its route evidence before starting split16. Its
requested 257 singleton forwards per study branch did not execute the optimized
context tail: `context_precomputed_tail_blocks` remained zero. The generic
forward path declines context sessions in `runGatedDecoderBlockOp`, whereas the
implemented fast context tail belongs to the prepared decoder path. Both arms
recorded 117132 compute encoders per study branch and similar generated-loop
times. There is no measured speedup from this attempted route change.

There is also a distinct validator defect: the ordinary
`paged_attention_1x_calls` counter does not count context-hook attention.
The `.paged_decode` mode and owning frame did propagate. A corrected qualification
must measure actual contextual singleton dispatch and successful optimized-tail
execution; accepting a requested-mode flag or simply allowing zero ordinary
paged calls would not prove the optimization. Failed campaign receipt:
`/Volumes/Extreme/antfly/qualification/context-replay56-coverage-v12/matched-replay-cost-v14/campaign-receipt.json`
(SHA-256 `7068563ccadd2b4f50b650c25229909505b8abd4641604832fca3355d625abf4`).

The independently reviewed full56 quality-materialization launch then returned
`IncompatibleModel` on its first real ordered request. No cases or reports were
accepted. It stopped after 10.15 seconds, peaked at 300.38 MiB RSS, incurred zero
new swap and drained both child-group RSS and live-process count to zero. This
is a separate materializer/model-acquisition issue; the successful coverage
path does not establish quality materialization. Raw journal:
`/Volumes/Extreme/antfly/qualification/context-quality-e2b-8k-exact-56-v1/packing-journal.jsonl`
(SHA-256 `a91c9612c96b69e0759bed3a88d32bd56c94508d3451c30cdb8cb02da9d82efd`).

The following corrections are included in full58 but still require fresh
installed-model replay. Quality materialization and loaded-provenance
capture now resolve installed generator identifiers through the same Node
resolver used by serving before acquiring a pinned model handle. The singleton
study route now calls the prepared whole-model decoder and treats any refusal
as terminal. Capture schema 7 measures successful framed context dispatches
directly and requires the exact expected optimized-tail count, with zero tail
failures. Historical schema 1–6 evidence keeps its original interpretation.
Qualification greedy selection uses separate precise Metal argmax pipelines
that apply the loaded model's final softcap before checking finiteness and
preserving the lowest token index on ties, matching the serial study's order.
This matters for the installed E2B softcap of 30: distinct large logits can
become equal, and raw infinities become finite after the transform. The policy
is scoped to the synchronous decoder call under the model-wide execution lock.
Replay and hidden/logits callbacks also acquire that lock, which is distinct
from the compaction workspace lease. Ordinary
serving argmax and suppression retain their previous behavior. Source review
and the full58 GPU regression suite passed; installed-model replay parity,
speedup and production qualification remain separate requirements.

Full57 compiled the prepared route and ran 434 tests: 416 passed, 17 opt-in
tests skipped, one failed, and none leaked. The failure was the new GPU softcap
regression, which returned the non-finite sentinel instead of the CPU token.
The guarded run took 485.92 seconds, peaked at 3990.36 MiB, incurred zero new
swap, and drained both process-group RSS and live-process count to zero. There
was no resource violation; this was a numerical test failure. No successful
full-release receipt was sealed. Resource evidence:
`/Volumes/Extreme/antfly/qualification/context-full-release-build-57/build-resource.json`
(SHA-256 `b14eaadeedfea26d47bcb71b4e89534055b4cf3e1594dd9d55d37b4a1b34c052`).
An isolated Apple M4 probe with `MTLMathModeSafe` reproduced NaN from
`tanh(+Inf / 30) * 30` and `tanh(1e20 / 30) * 30`; the negative counterparts
returned -30. Explicit saturation above scaled magnitude 10 matched the host
reference in the probe and follows Zig's f32 tanh branch. That correction is
implemented in both strict argmax kernels and must pass the full suite next.
The host policy ABI now carries f32 bit patterns as integers and validates
their exponent/sign before conversion, avoiding `-Ofast` finite-input
assumptions at the C boundary. Full57 is retained as failed diagnostic evidence.

Full58 passed the corrected source: 434 selected, 417 passed, 17 opt-in skips,
zero failures and zero leaks, including both strict-device-argmax tests. It
completed in 551.64 seconds with 3103.28 MiB peak group RSS, 41% minimum free
memory, 16935.34 MiB minimum free disk, zero new swap and zero remaining group
RSS/live processes. The strict full-release adapter independently validated the
archive, executable, resource evidence, test summary and compiler profile.
Receipt:
`/Volumes/Extreme/antfly/qualification/context-full-release-build-58/full-release-build.json`
(SHA-256 `1ddb0dbec90722df76c82a05abd62f4624322db839784c4a62133cfb77ce6eb3`).
Binary SHA-256:
`e0107c7724bdffa0c512fbbbe1d291e103cfc5e75aa977085e1f16d8473aa2ad`.
Source archive SHA-256:
`721275ca52ff5a8adc5d174d85ad7ec37f37de495041bd03e7b2e61d24c35d4a`.
The build receipt explicitly leaves `serving_qualified=false`. Its 17 opt-in
skips do not establish installed-model numerical, quality, serving or soak
results; those runs must use this new binary and its matching identity.

Full59 includes the installed-model caller and current native changes: 449
selected, 430 passed, 19 opt-in skips, zero failures and zero leaks. The strict
release adapter passes, and all 5,766 archived file hashes match the current
worktree. It completes in 483.70 seconds with 3773.640625 MiB peak group RSS and
490.1875 MiB swap growth, within the unchanged 512 MiB build-only guard. This
is build/source evidence, not clean-swap performance qualification. Fitting
and native diagnostic runs retain their zero-swap requirement. Receipt:
`/Volumes/Extreme/antfly/qualification/context-full-release-build-59/full-release-build.json`
(SHA-256 `4355e2d03ce911e7c5795111cea05dc31e03180cb11dfcfcb7b9db7f1f61df51`).
Binary SHA-256:
`1d179a31361b0d90ebff171301620a7946f7332070335f0fb19858b4d52f2e58`.
Source archive SHA-256:
`04efef10c0f4c479fbf4c02d498ec8b35e3bb9595ddf98404e476c27cd38e843`.
The receipt leaves `serving_qualified=false`. Full58 training captures remain
historical diagnostic evidence; final qualification requires fresh capture
from this binary and the matching runtime identity.

Full60 includes the compensated validation reduction and deterministic regression:
450 selected, 431 passed, 19 opt-in skips, no failures/leaks, 487.1031 seconds,
3052.03125 MiB peak group RSS and zero swap growth. The strict release adapter
validates its build graph. Receipt:
`/Volumes/Extreme/antfly/qualification/context-full-release-build-60/full-release-build.json`
(SHA-256 `fdf8a10d47cf8f4e39c3230ef6d3904894de53485ee67d37929a24b7280667e5`).
Binary SHA-256:
`e5cb922ccc3a9d0b9759c86632a8065bbcd901fbcb7aedf24d6e974de2736474`.
Source archive SHA-256:
`19e275c921c50e66678650b3213f935d20be29619bbc8a0e6605ee278bb18a02`.
Only the Metal validation source and its Zig regression differ from full59.
Same-payload training parity passes on this binary; its build receipt retains
`serving_qualified=false` and all final model/serving gates remain explicit.

The first full60 fresh-source attempt stops before model loading during the
coverage stage. Live-memory admission requires 4,186,895,320 bytes plus the
unchanged 6 GiB reservation, while available memory is 10,479,720,202 bytes:
a 142.69 MiB shortfall. The outer guard records 4.87 seconds, 600.109375 MiB
combined RSS, zero swap growth and complete child drain. This is an admission
failure, not a numerical result. The failed operational receipt and coverage
evidence remain at
`/Volumes/Extreme/antfly/qualification/attention-matching-final60-source-operational-v1.json`
and `context-final60-source-coverage-v1`. A successor must use fresh paths and
preserve the reservation and resource limits.

The subsequent light-parent attempt drains the strict source audit in a
separate process: 1.47 seconds, 309.4375 MiB combined RSS and zero swap. Its
coverage parent peaks at 43.640625 MiB, compared with 254.03125 MiB previously.
Coverage then rejects the command before native startup because a temporary
copy of `release_attention_matching.py` shadows the repository module. The
failed receipt is
`/Volumes/Extreme/antfly/qualification/attention-matching-final60-source-operational-v3-light-parent.json`.
This attempt supplies no model or numerical evidence. The successor pins and
verifies the repository module's absolute origin and digest; its real CLI check
passes with the stale temporary module still present.

That corrected launcher reaches native admission, which again stops before
model loading: 4,186,895,320 requested bytes plus the unchanged 6 GiB reserve
exceed 9,620,726,743 available bytes by 961.8945 MiB. The coverage parent peaks
at 39.171875 MiB, confirming the lower harness footprint. The outer guard
records zero swap growth and complete child drain. The failed receipt remains
at
`/Volumes/Extreme/antfly/qualification/attention-matching-final60-source-operational-v4-origin-fixed.json`.
This is an environment admission failure, not a numerical result. The next
attempt requires sufficient host headroom and fresh output paths; the reviewed
capture implementation, native reservation and resource limits remain fixed.

After the user frees additional RAM, the V9 attempt passes strict preflight but
rejects four predecessor evidence links against the light consumer's exact
three-link contract. Its receipt remains
`/Volumes/Extreme/antfly/qualification/attention-matching-final60-source-operational-v5-quiet-window.json`.
The V10 data-only successor preserves this failure through three links and
passes both adapter and light-consumer checks. Installed-model coverage then
passes in 156.2118 seconds (157.5966 seconds including the outer harness), with
8160 prompt tokens, a 32-token reserve, zero swap growth and complete drain.
Its immutable successful coverage session is
`/Volumes/Extreme/antfly/qualification/context-final60-source-coverage-v6-three-edge-retry/session.json`.
The subsequent capture wrapper rejects a provenance-schema mismatch before
tensor capture: the native manifest contains exactly binary/build hashes,
whereas the collector session includes device/OS hashes as well. The binary,
source build, request and loaded-model identities match. A successor must
validate each producer's exact schema and the capture-child/sealer boundary;
the enclosing operational failure remains preserved and is not a numerical
result.

The V11 successor validates the collector's four provenance fields and the
native manifest's two-field projection separately, and pins the actual
capture-child schema in the sealer. It completes fresh full60 coverage,
capture and sealing for all 36 owner/block coordinates. Capture takes
500.583989 seconds at 3908.296875 MiB combined peak RSS; sealing takes
17.474860 seconds at 683.96875 MiB. Both report zero swap growth and complete
child drain. The terminal training source is
`/Volumes/Extreme/antfly/qualification/attention-matching-final60-source-v7-provenance-fixed/final-source-receipt.json`
(SHA-256 `9ca511c292349be48adc2effa0daca31e8d61cb922cf161f2689f376bd38cca1`).
This is current-build source evidence, not fitted-payload or held-out
qualification. The first seed binding stops before producing outputs because
owner 14 has 960 validation rows against the authority's expected 1280.
Owners 4 and 9 each have 912 training and 256 validation rows; owner 14 has
3000 training rows. All 36 captures retain the expected 512 source rows,
one KV head and dimension 512. The discrepancy requires an exact sampling
contract check before a corrected authority can authorize fitting.

That check confirms the native protocol's 1024-row global validation cap:
40 owner-14 consumer strata times two sources times `floor(1024 / 80)` gives
960 rows. Historical full58 owner-14 metadata also reports 960; the 1280
assertion was incorrect. Seed driver V7 and authority V4 change that assertion
and dependent identities while preserving the exact seed profile and all
training geometry, retention, gates and resource limits. Independent review
passes, as do six authority checks and six lifecycle checks. The review is
`/private/tmp/attention-matching-final-build-fresh-source-seed-driver-independent-review-v7.json`
(SHA-256 `534679e2c9cd308c62e122d930c11b6a81c38f82ffd8fb52f32fa95c50f0e8e9`).
Sampled current and historical source K/V hashes match, but query/position
hashes differ; historical hard-case metrics therefore do not replace the
required fresh fitting campaign.

The ownership audit also confirms that current production Metal context routes
hold the outer model mutex through session teardown and external-runtime drain.
Those routes must remain shared-serial unless that ownership contract is
extended; the compaction pool mutex alone does not protect the model runtime.

The next training-only LP-envelope recipe also has an explicit native
implementation gap. The current Zig fitter exposes RMS and matching-pursuit
selection, and the Metal fitter copies selected source keys, fits positive mass
with coordinate descent and values with aggregate ridge regression. The
experimental Python recipe instead moves stored-f16 keys around centroid
anchors, re-solves a positive minimax mass problem and uses a consumer-normalized
value objective. A Python pass cannot qualify those different native kernels.
Promotion requires an implementation identity for the exact new solver,
deterministic request-specific initialization (including the cost of producing
its continuation seeds), bounded workspace and cancellation, and stored-format
CPU/Metal parity before independent held-out evaluation. Captured seed payloads
cannot be reused as a model-wide production profile.
The independent source review records the exact native/Python differences and
remaining implementation requirements in
`/private/tmp/attention-matching-native-production-gap-analysis-v1.md`
(SHA-256 `80a81bd7bbe845711bfb94e3477a580517f3df059a70c4a988d76ab47783bc87`).

The schema-6 parser passed 28 Python tests after integration. Its narrow Zig
provider-counter, admission and route-identity regression run passed 11/11
selected tests in 14.09 seconds, at 539.14 MiB peak RSS and zero new swap.
Receipt: `/private/tmp/context-observation-focused49b-receipt.json`
(SHA-256 `c20ade15cba7ddbaf7923871411156b66c092c6a059f0abd7630cd1db9018593`).
The writer's loaded-model path still requires the full build and a real capture.
The producer now also rejects missing question timing/provider counters and
invalid counter relationships before writing artifacts, matching the schema-6
parser. Its focused Zig regression passed 3/3 selected tests in 13.91 seconds,
with 514.55 MiB peak RSS and zero new swap. Receipt:
`/private/tmp/context-question-provider-focused50-receipt.json`
(SHA-256 `df68815d91064032a3f7af3acaf424940e28518261850554b19929263cd1e39b`).
An earlier wrapper incorrectly supplied runtime filters and triggered full
compilation; its 2 GiB guard stopped that attempt. The corrected wrapper uses
compile-time filters and preserves the earlier failure evidence.

The focused rerun verifies the renderer allocation-failure propagation,
branch-local query admission, artifact-bound profile rejection, shared replay
ownership and tool-history provenance fixes. It also exposed an oracle fixture
that compared unused output capacity with uninitialized expected data. The
oracle now compares the requested rows and separately checks that unused rows
remain unchanged. Explicit driver/message imports make these tests discoverable
under narrow compile-time filters. The final run passed 13/13 in 26.83 seconds,
peaked at 865.875 MiB process-group RSS and generated no new swap. Receipt:
`/private/tmp/context-focused-fixes48-v2-resources.json`.

The opt-in 8K replay fixture also needed its existing eight continuation rows
included in its retained-capacity reservation. With that fixture corrected,
all 18 split/serial cells passed, including the FP64 checks at one query row
for each head dimension. Actual dispatch counts were five per route per cell.
Physical scratch was 1,064,960 bytes at D256 and 2,113,536 bytes at D512.
Receipt: `/private/tmp/context-split-focused48-v2-diagnostic.json`
(SHA-256 `d01ae78863e7af747035aca55f20d9eec275f310c315571d4ea283b03a8343a8`).
This is a focused synthetic diagnostic with Metal validation enabled, one
warmup and four timed samples per route. Old system swap remained present.
It does not replace the full build, matched installed-model performance or
clean-memory production qualification.

The subsequent explicit split32 diagnostic passed all 24 cells and the strict
split-count selector test. Maximum absolute error and relative RMSE remained
2.28e-8 and 1.51e-5. It completed in 20.55 seconds at 593.16 MiB peak RSS,
62% minimum free memory and zero new swap. Receipt:
`/private/tmp/context-split32-focused49b-resources.diagnostic.json`
(SHA-256 `c7421c117ff7e789ee0543a940330ea0d90b729cf56dd95a99b9fba9a0eb50ff`).
Its exact focused executable is archived and hash-bound; it is not a full
release artifact. Split32 provided no consistent improvement across geometries:
at D512 one row it measured 4.18 ms versus split16's 4.10 ms, and at eight rows
20.24 ms versus 21.95 ms. It doubles the D512 fixture's physical shared scratch
to 4,210,688 bytes. The default remains split16, and the frozen whole-model
comparison continues to use split16. These short synthetic samples do not
justify a production route change.

Before this run, an attempted focused build was rejected by Zig's scheduler
because its 1 GiB configured reservation was below the context target's declared
8 GiB reservation. The corrected wrapper admits that declared reservation while
independently enforcing the same actual 1 GiB process-group RSS limit and zero
new swap. The rejected attempt executed no tests and is retained separately.

### Training continuation after build 48

The frozen r288 continuation completed all 36 cells in 1,226.16 seconds,
within the original total key-row drift limit of 0.25. All per-cell resource
guards passed: peak process-group RSS was 872.53 MiB, minimum free memory was
48%, and no new swap was observed. Thirty cells improved; median training
maximum relative mass error fell from 11.21% to 6.86%. The unchanged 5%
training mass limit is satisfied by 13 cells, compared with five before the
continuation:

| Shared KV owner | Training mass passes | Median maximum relative mass error |
| --- | ---: | ---: |
| 4 | 2/12 | 31.04% |
| 9 | 11/12 | 4.49% |
| 14 | 0/12 | 7.67% |

Receipt: `/private/tmp/antfly-context-v19-five-source45f-direct-key-continuation-v6/campaign-receipt.json`
(SHA-256 `134ffb399eab442b572f15aaa536a4f258c493008a1c9f11bf66a27b2c027eb8`).
No held-out queries were evaluated and no candidate gained runtime authority.
The receipt's `training_full_gate_passed` field describes a stronger diagnostic
criterion. Training output errors and individual source slices are diagnostics;
the existing numerical contract requires training mass error at most 5%, then
independent held-out mass error at most 5% and output relative RMSE at most 10%
overall and for each consumer after storage conversion. A training diagnostic
must not silently become an additional release gate.

The next numerical investigation separates an inadequate bounded search from
an inadequate retained-key representation before another expensive campaign.
The 0.25 key-motion radius is a recipe choice, not a release threshold. Any new
recipe still needs frozen selection rules and independent evaluation under the
unchanged numerical and total-memory requirements.

Keeping the other 23 blocks exact cannot rescue this candidate's memory gate.
Using the existing 58,228,736-byte packed-exact baseline, page-rounded storage
for those 13 hypothetical compact blocks is 52,297,728 bytes: only 10.19%
savings. Even an impossible zero-overhead ceiling is 10.24%. At r288, at least
32 of 36 blocks must compact to reach 25% total savings; an evenly distributed
integer pattern needs 11 per owner (33 total). This is a storage projection,
not measured publication or evidence that any block passes held-out output
accuracy. Receipt:
`/private/tmp/antfly-context-v19-v6-adaptive-memory-upper-bound.json`
(SHA-256 `1cc50fffaba019e818eff83c90c898250ca84788c90f227535df2f788f2784c4`).

A subsequent necessary-feasibility diagnostic examined the original 36 r288
anchors under the same 0.25 key-motion radius, including its existing numerical
tolerance and stored-bias weight endpoints. Thirty-five relaxed problems were
feasible; owner 9/block 4608 reached its 60-second solver deadline. No numerical
infeasibility indication was obtained. Feasibility of the relaxed problem does
not establish feasibility of the actual stored-f16 fit, and the screen is not
a formal real-arithmetic certificate. It therefore supplies no numerical or
release pass. The run took 144.93 seconds, peaked at 508.16 MiB RSS and produced
no new swap; no held tensors were loaded. Root revalidated all plan and evidence
hashes after completion. Receipt:
`/private/tmp/antfly-context-v19-five-source45f-trust-feasibility-v4/campaign-receipt.json`
(SHA-256 `9971d47ed1809822e16c2463e25481b33e5a16d07eb6a3315df9348638bb7e95`).

A four-case pilot then evaluated the corrected LP-envelope key gradient and
bounded group-weighted value refit from the frozen continuation payloads.
The original r288 retention and total 0.25 key-motion radius were preserved.
All four cells completed within their deadlines and resource limits in 311.18
seconds total, with 779.78 MiB peak RSS and zero new swap:

| Owner / block | Initial training mass error | Returned training mass error | Training consumer output RRMSE | Algorithm time |
| --- | ---: | ---: | ---: | ---: |
| 4 / 3072 | 68.25% | 14.29% | 17.61% | 10.85 s |
| 4 / 4096 | 4.82% | 4.82% | 13.57% | 0.10 s |
| 14 / 4096 | 17.63% | 9.77% | 18.92% | 182.91 s |
| 14 / 5632 | 7.16% | 4.64% | 11.58% | 115.12 s |

One additional cell passes the unchanged training mass threshold; the already
passing control remains within it. The two other cells exhausted eight key
steps. Training output remains diagnostic. These float64 diagnostics reevaluate
stored f16 K/V and f32 bias; they do not establish Metal arithmetic accuracy,
held-out acceptance, a certified minimax solution, or production fitting cost.
No held tensors were loaded, and all release/profile authority remains false.
Receipt: `/private/tmp/antfly-context-v19-five-source45f-lp-envelope-pilot-v8/campaign-receipt.json`
(SHA-256 `f7ea899f46f6c8ef94f0ddbbae847a3e0e8fec4926e4fb3d7e88f621dd4954a6`).

A fixed-attention projection audit then examined the same four immutable pilot
payloads at three predeclared singular-value thresholds. All four cells completed
in 6.43 seconds under an aggregate 1 GiB guard, at 295.84 MiB peak RSS and zero
new swap. The shared attention matrices were highly ill-conditioned (reported
condition numbers about 1.48e12–8.05e12). Their numerical ranks at the 1e-8
threshold were 160, 204, 206 and 251 out of 288 retained columns. Seven of the
12 unconstrained shared least-squares solutions overflowed when converted to
f16; those stored diagnostics were explicitly recorded as unavailable.
Independent per-consumer projections had near-zero residuals, but use separate
values and underdetermined systems, so they do not establish a feasible shared
payload. The aggregate least-squares objective also did not improve the pilot's
worst-consumer training error. These results motivate investigating conditioning
and joint key/weight/value representation; they do not prove impossibility or
constitute a training-output release gate. No held tensors were loaded and no
payload was changed. Receipt:
`/private/tmp/antfly-context-v19-five-source45f-fixed-p-projection-v3/campaign-receipt.json`
(SHA-256 `00ebe83c82810df0447d4f6829ee0dd4b0cd0740407df60e29005dac61c82602`).

A three-cell training-only diagnostic then kept the pilot keys fixed and
maximized the smallest weight relative to its original centroid cluster size,
subject to either a near-optimal mass cap or the unchanged 5% training mass cap.
V2 removed the near-zero weights and restored rank 288 in every feasible arm,
but f32 log-bias rounding put the stored mass just above its cap; value fitting
was correctly skipped. It completed in 46.04 seconds at 652.09 MiB peak RSS,
with zero new swap. Receipt:
`/private/tmp/antfly-context-v19-five-source45f-weight-degeneracy-v2/campaign-receipt.json`
(SHA-256 `ca8d4e09d42025f15efa1f1fa37dd6b11a1d4620b4a6bfc7192f85a1e01d8f0c`).

The independently reviewed V3 reserves conservative f32 log-bias rounding
headroom inside the raw LP constraint; stored acceptance remains exactly at the
original cap. All three cells completed in 32.82 seconds at 638.78 MiB peak RSS,
with zero new swap. Its hard-5% arm produced:

| Owner / block | Stored training mass error | Stored f16 worst-consumer training output RRMSE |
| --- | ---: | ---: |
| 4 / 3072 | LP reported infeasible at these fixed keys | Not evaluated |
| 4 / 4096 | 0.04999807 | 0.12580489 |
| 14 / 5632 | 0.04999801 | 0.10856686 |

The two output diagnostics improve on the incoming pilot values, but do not
establish held-out accuracy. The eight-step consumer-weighted value solver is
still a bounded heuristic, not a certified minimax solution. No held tensors
were loaded, no payload acquired release authority, and this small diagnostic
does not replace the full 36-cell recipe's 13 training-mass passes. Receipt:
`/private/tmp/antfly-context-v19-five-source45f-weight-degeneracy-v3/campaign-receipt.json`
(SHA-256 `1cc7f2d48905fc779a4541a40af144d8fcb89b4e47621159936daf0cf3ab3839`).

## Attention-matching research review (2026-10-17)

Attention matching remains the preferred compaction direction. The
[paper, revision 2](https://arxiv.org/html/2602.16284v2) targets local attention
outputs and partition mass, with bias terms and unchanged logical positions.
Its reported high compression ratios are task/model results, not lossless
compression guarantees. It evaluates Qwen3, Llama3.1 and Gemma3, not Gemma4.
The online reasoning experiment is a proof of concept. The main document
experiments compact after ingestion, then answer multiple questions. These
results motivate a separate post-ingestion qualification path for our runtime.

Source review pinned the authors' repository to
`2020f6d37940fe013e7e7f1ebf409e5d107c5670`. No upstream code was added to the
serving runtime. The current bounded fitter is an attention-matching variant;
it is not a numerical reproduction of their implementation.

| Component | Authors' implementation | Current Zig/Metal implementation |
| --- | --- | --- |
| Mass features | Max-shifted exponentials with their row sums as targets | Softmax probabilities with target one; this changes the least-squares weighting |
| Highest-attention selection | RMS ranking in the experiment configuration | RMS ranking, with a matching-pursuit retry |
| Mass solve | Clamped least-squares initialization and two projected steps for RMS; weights bounded by exp(-3) and exp(3) | 128 coordinate sweeps; bounds 1e-6 to 1e6 |
| Value solve | Direct unregularized least squares by default | Ridge-regularized correction to selected original values |
| Query source | Repeat-prefill and optional self-study; optional sequential layer replay | Bounded queries captured from the real request, balanced across every GQA/shared-KV consumer |
| Retained allocation | Per-head allocations supported | Fixed 512-to-256 online session; internal ABI 2 packed generations support independent counts and exact heads |

The numerical rows are supported by the pinned
[key-selection source](https://github.com/adamzweiger/compaction/blob/2020f6d37940fe013e7e7f1ebf409e5d107c5670/compaction/algorithms/highest_attention_keys.py),
[linear algebra source](https://github.com/adamzweiger/compaction/blob/2020f6d37940fe013e7e7f1ebf409e5d107c5670/compaction/algorithms/base.py), and
[experiment configuration](https://github.com/adamzweiger/compaction/blob/2020f6d37940fe013e7e7f1ebf409e5d107c5670/evaluation/configs/algorithms/default.py).
Query collection and per-head budgets are described in the paper and the
[reference repository](https://github.com/adamzweiger/compaction/tree/2020f6d37940fe013e7e7f1ebf409e5d107c5670).

An independent FP64 diagnostic applied the RMS/max-shift/clamped-LS recipe to
the first global owner's captured README queries, keeping training and
validation disjoint and rounding fitted values to f16. It is not a reproduction
of the authors' CUDA precision or downstream benchmarks. All three examined
heads failed our existing gates:

| Artifact / owner / head | Held-out maximum relative mass error | Held-out relative output RMSE |
| --- | ---: | ---: |
| E2B / 4 / 0 | 79.84% | 13.73% |
| E4B / 5 / 0 | 120.91% | 20.56% |
| E4B / 5 / 1 | 389.46% | 19.41% |

A control retained the same keys, solver and bounds but used relative-mass
weighting; it also failed all three heads. Thus changing normalization alone
is not an evidenced fix. These probes do not rule out attention matching with
better query coverage or different retained budgets. Local diagnostic:
`/private/tmp/context_author_recipe_v9.py`; results:
`/private/tmp/context-author-recipe-v9.jsonl`.
Our maximum per-query mass gate is also a different criterion from downstream
answer accuracy; these failures must not be presented as contradicting the
paper's task-level results.

The next implementation sequence is:

1. Add a reproducible, offline calibration harness with pinned model, tokenizer,
   corpus and reference-source identities. Compare the published and current
   objectives on identical tensors, including QR-based value fitting, before
   choosing a Metal solver. Record rank, conditioning and f16 conversion error.
2. Add an internal post-ingestion replay path for repeat-prefill queries. Its
   temporary sequence must have separately charged KV/activation/query budgets,
   cancellation and position limits; synthetic study tokens must never enter
   user-visible history or usage. Identify the document/tool-payload token range
   explicitly and preserve instructions and role delimiters; the current fixed
   prefix count alone is not a semantic protection guarantee. Validate on separate questions and generated
   continuations, not only nearby prefill activations.
3. Measure sensitivity per global KV owner/head on independent calibration
   documents. Shared Gemma4 owners must satisfy every consumer. Introduce
   different retained counts only with an explicit arena/descriptor/admission
   design; preserve exact heads when their measured error requires it. A total
   memory target must not force every head to discard exactly half its rows.
   Replaying the full model after fitting must recheck downstream shared-KV
   consumers; layer-local calibration alone does not establish their behavior.
4. Port only configurations that pass independent CPU quality checks, then
   establish GPU parity and model-level quality. Keep SWA exact, original RoPE
   positions, mass biases, bounded scratch and atomic publication. Existing
   approximate-mode capability flags remain disabled during this work.
5. Qualify E2B and E4B separately on document QA, exact-detail retrieval, code,
   structured tool output and multi-turn continuation. Report answer quality,
   reference-generation loss and instruction retention alongside attention
   errors. Establish any revised acceptance contract on independent data;
   do not relax thresholds merely to accept the current failing examples.
6. Exercise factory/HTTP/CLI execution, cancellation, mixed requests, repeated
   compaction and reload, then measure actual device/process memory and latency
   on clean-memory destination hardware. Only those results can support release
   enablement or a compression-ratio claim.

This work can reuse the bounded arena, staged descriptors, device-resident
query capture and transactional publication already implemented. The fitting
and qualification work remains a release blocker.

## Accepted post-ingestion implementation contract

The first release compacts explicitly selected payloads after ingestion, then
generates from that context. It uses adaptive retention and one isolated
repeat-prefill pass. A separately admitted temporary memory peak is allowed;
the committed decode allocation must actually decrease. Repeated compaction
during generation is a later qualification target.

1. **Numerical qualification.** Compare relative-mass coordinate descent with
   RMS and matching-pursuit selection against max-shifted exponential mass,
   clamped QR initialization plus two projected steps, and direct QR value
   fitting. Evaluate 128/256/384/448 retained rows per original 512-row block;
   512 is exact retention. Keep the 5% maximum mass and 10% held-out output
   relative-RMSE gates after f16 conversion. Every shared-layer/GQA consumer
   must pass. Separate calibration and evaluation documents, freeze the
   smallest passing budgets, and use measured fit cost only to break ties.
2. **Payload provenance.** Add `context_policy.compaction_targets` with
   zero-based `{message_index, part_index}` selectors and repeatable CLI
   `--compact-part MESSAGE:PART`. String content is part zero. Only selected
   user/tool text is eligible. System/developer/assistant content, delimiters,
   tool-call metadata, special tokens, boundary-crossing tokens, and protected
   prefix/tail tokens remain exact. Preserve rendered bytes and token IDs;
   propagate provenance through rendering/tokenization without substring
   matching or inserted markers. Reject unsupported mappings explicitly.
3. **Reference replay.** Prefill the real prompt exactly. A private branch
   shares immutable original global KV and owns its writable tail and SWA.
   Replay at most 8192 additional tokens including the fixed instruction,
   bounded by the remaining position range. Copy the whole selected payload
   when it fits, otherwise choose deterministic evenly distributed excerpts.
   Collect up to 4096 queries per KV head, balanced across all consumers, with
   disjoint training/validation positions. Free the replay tail before fitting.
   Synthetic tokens must not affect the live sequence, sampler, grammar,
   output, or user usage. Inadequate coverage retains exact KV or fails with
   the applicable quality/budget error.
4. **Adaptive storage.** Version the Zig/Metal descriptor ABI for per-head
   span counts. Only eligible full 512-token blocks are candidates. Retry
   larger calibrated budgets and preserve exact heads/blocks when necessary.
   `max_kv_tokens` remains a retained-row cap per global head; charge original
   ingestion and temporary preparation separately. A bounded first pass
   determines counts, then allocate right-sized destination arenas once,
   refit/pack one block at a time, and revalidate. Publish all owners atomically
   after reader drain and cancellation checks, then free old arenas and
   calibration buffers. Require a decrease in page-rounded committed context
   bytes including continuation reservation. Free slots inside an unchanged
   arena do not establish memory savings.
5. **Serving and release.** Use the same lifecycle for native, CLI, HTTP,
   streaming and serial mixed batches. Keep scheduler batching, speculative
   decoding, media, and repeated compaction separately gated. Qualify the
   optimized Metal decode route against the correctness kernel. Bind capability
   receipts to artifact, tokenizer/template, profile, source build, device/OS,
   dtype, route, and tested context range; mismatches remain disabled.

Release evidence must include real published compaction and generation at
4K/8K on E2B and E4B, measured allocation reduction, five evaluation buckets
(document QA, exact retrieval, code, structured tool output, multi-turn),
aggregate accuracy regression at most 2 percentage points, per-bucket regression
at most 5 points, and relative perplexity regression at most 5%. Extend context
coverage to 2K/8K/32K/64K and position boundaries only when those ranges pass.
Exercise uneven head budgets, donor consumers, append behavior, Unicode and
normalization, duplicate payloads, semantic protection, sampling/grammar and
streaming. Cancellation, OOM, nonfinite values and GPU failures must preserve
atomic rollback and request isolation. Clean-memory destination measurements,
backend-wide KV transfer accounting, at most 5% exact-path performance
regression, and a four-hour serial serving soak are still required.

## Offline calibration tooling

`zig/pkg/inference/scripts/gemma4/attention_matching_calibrate.py` implements
`seal`, `sweep`, `freeze`, and `evaluate`. It uses NumPy/SciPy FP64 and pivoted
QR (`gelsy`, relative rank cutoff `1e-10`); this is an independent numerical
oracle, not bitwise Metal parity or a production capability receipt. It records
all failed gates rather than stopping at the first training rejection.

`capture_attention_matching.py` runs the opt-in installed-model replay probe
serially under process-RSS, free-memory, disk and zero-swap-growth limits. Its
build receipt must contain `binary_sha256` and `source` (`git_head`, `files`);
every changed file in the Zig workspace must be represented. The driver checks
that receipt, the supplied model digest, and the expected global owner list
before sealing captures. It snapshots the document bytes before execution,
checks the selected GGUF is unambiguous, and rejects external tokenizer or
template overrides without a separate identity contract. It verifies replay
payload token IDs against their original prompt positions. Use separate output directories for each artifact,
document and corpus split. The probe owns a fresh private numerical session;
immutable-prefix sharing with a live serving session is still unimplemented.

```sh
python3 zig/pkg/inference/scripts/gemma4/capture_attention_matching.py \
  --binary /path/to/context-tests --build-receipt build-receipt.json \
  --artifact /path/to/model.gguf --model-id gemma4-e2b \
  --model-sha256 MODEL_SHA256 --global-owner 4 --global-owner 9 --global-owner 14 \
  --document README.md --document-id repo-readme --bucket code \
  --purpose calibration --output-dir /tmp/am-e2b-calibration
```

Replace the artifact paths/digest with the verified build/model inputs. E4B's
global owners are 5, 11, 17 and 23 for the installed artifact described below.
The probe deliberately retains all original KV, collects only queries within
the separately bounded replay interval, verifies the original global KV digest
is unchanged, and reports `published=false`. A successful capture is evidence
about inputs to fitting, not a successful compaction transaction.

The reusable `runtime/kv/reference_replay.zig` planner copies selected original
token intervals without changing them. It counts the fixed instruction against
the 8192-token and remaining-position budgets, records original positions, and
uses deterministic excerpts when needed. The caller must establish eligibility
through byte/token provenance; the planner does not guess message ownership.

A capture spec supplies `schema_version: 1`, a `purpose` of `diagnostic`,
`calibration`, or `evaluation`, and these objects:

| Field | Required provenance |
| --- | --- |
| `model` | `id`, SHA256 values for artifact/tokenizer/template, and `global_owner_shapes` (`layer`, `heads`, `dim`) |
| `document` | `id`, `sha256`, and bucket (`document_qa`, `exact_retrieval`, `code`, `structured_tool`, `multiturn`) |
| `source` | Capture-time `git_head` and a path-to-SHA256 `files` map, including uncommitted runtime sources |
| `capture` | Rows/heads/dimension, training/validation counts, block start, owner layer, scale, query origin/sampling version, replay start/token count, consumers and strata |
| `tensors` | Paths to little-endian raw `keys`/`values` (f16, rows × heads × dim), `queries` (f32, heads × combined queries × dim), and `positions` (u32, heads × combined queries) |

The opt-in model probe emits `*-capture.json` with actual consumer/slot
provenance. Each consumer has `layer` and `query_heads`; each stratum has
`consumer_layer`, `query_head`, `kv_head`, and training/validation `_start` and
`_count` fields. Validation starts are relative to the validation partition.
The campaign controller supplies the capture-time model/document/source
identities; sealing cannot retroactively establish those identities.
Qualification captures require `query_sampling: uniform_partition_v1`. A stable
hash assigns one held-out position per quartet; each consumer samples uniformly
across its complete replay partition, including both endpoints when it has at
least two samples. Single samples use the partition midpoint. The independent
sealer verifies every selected position. Earlier rolling-tail captures are
diagnostic evidence and cannot qualify this sampling contract.

```sh
python3 zig/pkg/inference/scripts/gemma4/attention_matching_calibrate.py seal \
  --spec capture-spec.json --output-dir sealed-capture
python3 zig/pkg/inference/scripts/gemma4/attention_matching_calibrate.py sweep \
  --capture sealed-capture/manifest.json --output calibration.json
python3 zig/pkg/inference/scripts/gemma4/attention_matching_calibrate.py freeze \
  --receipt calibration.json --adaptive --output frozen-profile.json
python3 zig/pkg/inference/scripts/gemma4/attention_matching_calibrate.py evaluate \
  --profile frozen-profile.json --capture unseen-document/manifest.json \
  --output evaluation.json
python3 -m unittest discover -s zig/pkg/inference/scripts/gemma4 \
  -p 'test_*attention_matching*.py'
```

Repeat `--capture` for every global owner and document. Sealing checks finite
inputs, shape/memory bounds, causal positions, disjoint query partitions and
complete consumer coverage, then records tensor hashes and independent exact
block attention outputs/log partitions. Reports include rank/conditioning,
storage conversion error, per-consumer gates, retained KV/bias bytes and fit
duration. These byte counts exclude arena/page/descriptor/continuation costs
and are not device-memory savings. Timing is diagnostic, not a hardware claim.

Freezing requires complete owner coverage for every calibration document and
allows sensitive heads to remain exact, but rejects an all-exact model or a
profile that does not reduce its raw KV/bias bytes. Ordinary-prefill captures
are diagnostic only. `--adaptive` freezes a strictly increasing retention
schedule for the chosen solver, ending in exact retention. Evaluation tries
only that schedule, stopping at the first passing candidate. Rejected attempts
remain in the receipt, while `selected_attempt` identifies the resulting head.
Without the flag, evaluation uses the single frozen retention count. Neither
mode changes thresholds or selects a different solver using evaluation data.
Every model/document pair must reduce raw block bytes; savings elsewhere cannot
hide an exact-only document. Even a passing numerical receipt does not establish
a decrease in page-rounded committed device allocations.

Evaluation rejects calibration document IDs/hashes and
reused calibration query tensors,
missing owners, and changed model/tokenizer/template/source/geometry contracts.
Neither command changes serving capabilities.

The CPU reference now supports explicit retained counts and a bit-preserving
exact outcome. CPU and Metal validation gate each shared/GQA consumer's held-out
output error in addition to aggregate output and per-query mass. The Metal
fitter and published arena still support only half-block compaction and reject
other requested retention counts before allocation. The offline adaptive
profile is not accepted as a serving capability or a GPU-supported profile.

The next runtime changes remain payload provenance through chat rendering and
tokenization, a private replay branch sharing immutable original KV, per-head
descriptor/arena allocation, and a qualified fitter/decode implementation.
The test-only replay capture owns an entirely separate session; it proves none
of those serving lifecycle or committed-memory properties.

### Replay qualification checkpoint (2026-10-17)

The first repeat-prefill campaign captured both installed artifacts on a
calibration document and a separate evaluation document, but inherited the
rolling-tail query reservoir. Frozen fixed profiles failed that evaluation:
E2B owner 9 reached 41.41% held-out mass error; all four compressed E4B heads
exceeded the 5% gate. Those results remain diagnostic. The source snapshot is
`/private/tmp/antfly-context-v10-capture-source.zip` (SHA256
`39e93303c6ae699d8c0ad319ae3d58a6c00de700f746217339f925a76da5bced`).

The corrected sampler covers the complete private replay and is checked
independently by the sealer. A new E2B run ingested 2817 source tokens and 2829
replay tokens, preserved the original global KV digest, and published nothing.
It completed under the resource guard with zero new swap-outs. Across all three
global owners, all 48 lossy fits (four recipes at 128/256/384/448 rows) failed
the unchanged gates; all three exact references passed. The harness refused to
freeze an all-exact compression profile. This evidence concerns the captured
512-token block, not every possible block, document or attention-matching method.

At 448 retained rows, the lowest maximum held-out mass error among the four
recipes was 98.755% for E2B owner 4, 22.694% for owner 9, and 47.249% for owner
14. Full-replay coverage exposes errors that tail-only calibration missed.
Because calibration failed, the planned new evaluation document was not used
to tune or qualify a profile.

The corrected E4B capture was terminated by the resource guard after observing
12 MiB of system swap-out growth. It produced no completed sealed capture or
numerical verdict. This is an environment-limited run, not a fitter failure or
qualification pass. The host already had swap in use; no run here establishes
clean-memory performance.

The corrected campaign's build receipt, sealed E2B captures, numerical results
and E4B resource failure are under
`/private/tmp/antfly-context-v11-replay-uniform/`; the controller log is
`/private/tmp/antfly-context-v11-uniform.log`. Approximate serving remains
disabled. A nonidentity profile that passes representative queries and separate
evaluation data remains a prerequisite to porting its solver into the adaptive
Metal storage path. At that checkpoint, payload provenance, shared replay ownership, per-head arenas,
published generations and release qualification were outstanding. The later
packed-storage work below adds the arena/publication building block; request
integration and release qualification remain outstanding.

Final local correctness checks for this checkpoint passed:

| Check | Selected | Passed | Skipped | Failed |
| --- | ---: | ---: | ---: | ---: |
| Full context suite, Metal with API validation | 200 | 194 | 6 | 0 |
| Full context suite, CPU-only | 200 | 173 | 27 | 0 |
| Python numerical/provenance/capture tests | 25 | 25 | 0 | 0 |

The full suites skip opt-in installed-model probes; the separate capture
outcomes above remain the model evidence. The Metal request-lifetime fixture
returned to 19,005,440 allocated bytes after each of eight measured lifetimes,
with a 33,128,448-byte observed peak. This establishes fixture cleanup, not
production compaction savings or a clean-memory performance result.
The local source snapshot is `/private/tmp/antfly-context-v11-source.zip`;
hashes, binary/source bindings, and validation log paths are recorded in
`/private/tmp/antfly-context-v11-validation.json`. No commits, pushes or CI
dispatches were performed.

## Request contract

```json
{
  "model": "google/gemma-4-E2B-it-qat-q4_0-gguf",
  "messages": [{"role": "user", "content": "Hello"}],
  "max_tokens": 256,
  "context_policy": {
    "mode": "exact",
    "max_context_tokens": 8192,
    "max_kv_tokens": 8192
  }
}
```

The equivalent CLI controls are `--context-mode exact`, `--context-window 8192`
and `--max-kv-tokens 8192`. The optional positional cap cannot extend either the
model's artifact limit or an active draft model's limit. Admission reserves
output, speculative bonus positions, and media expansion. Exact mode rejects a
request whose retained KV limit cannot hold its requested continuation. It
never truncates global attention to satisfy that limit. Token usage continues
to count the original prompt and generated tokens.

`rolling` and `attention_matching` require an explicit KV budget. Their reserved
contract protects 256 prefix tokens and 2048 recent tokens by default. These
are token counts, not message-boundary or system-message guarantees. Setting
`keep_prefix_tokens` to zero is represented explicitly in generated SDKs.
Approximate execution is currently rejected with `UNSUPPORTED_CONTEXT_POLICY`;
there is no environment override that declares it qualified. Unsupported ONNX
policy routes also reject the request rather than ignoring its limits.

The internal attention-matching route reserves capacity up to the lesser of
the requested logical continuation and the explicit KV cap. It still rejects
requests whose minimum compacted peak cannot fit that cap. If a candidate fails
quality validation, the route can retain the remainder exactly only when every
global owner can hold the entire remaining request in its existing reservation.
This stops further compaction attempts, preserves the live generation, and
increments `exact_tail_fallbacks`. It does not count as successful compaction.
Insufficient headroom, nonfinite input, cancellation and device errors remain
request failures. Fallback never allocates additional context storage.

A request combining `context_policy` with `cache_compaction_ratio` is rejected.
Legacy ratio `1` is normalized to exact identity by CLI, HTTP, batch, and
native generation. Approximate ratios remain rejected, including direct
pipeline callers, before they can reach the old host prototype. The `0.5`
post-prefill migration still requires execution and quality qualification.

## Implemented components

| Component | Behavior | Current use |
| --- | --- | --- |
| `runtime/context_policy.zig` | Checked positional arithmetic, full-continuation admission, bounded 2:1 row accounting, feature gates, owner-layer layout validation | Native runtime and API/CLI admission |
| `models/gpt.zig` | Retains explicit HF and GGUF Gemma attention schedules, including irregular global layers; preserves global history | Model parsing and attention selection |
| `architectures/gemma4_runtime.zig` | Includes the schedule in the prepared-runtime fingerprint | Runtime cache identity |
| `runtime/kv/attention_matching.zig` | Per-head CPU reference with supplied real queries, nonnegative mass fitting, log bias, regularized value fitting, and held-out rejection | Reference and tests only |
| `runtime/kv/context_view.zig` | ABI 1 interleaved spans, ABI 2 independent per-head spans/arenas, checked ranges, and publication leases | Metal context session and internal packed-generation builder |
| `backends/context_attention.metal` | Segmented f16 KV attention with FP32 online softmax, mass bias, absolute causal/SWA masks, GQA, and fixed workspace per query/head | Model attention through the request-owned session; optimized paths remain gated |
| `backends/context_compaction.zig` and `.metal` | Device-resident mass selection/fitting, regularized value correction, f16 output, held-out validation, bounded staging including page slack, and 32-byte status readback | Online session maintenance and correctness tests |
| `backends/context_kernels.zig` | Checked ABI 1/2 Metal dispatch, per-head history/range/epoch validation, bounded descriptor upload, synchronous completion | Session attention and kernel tests |
| `backends/packed_context_storage.zig` | Per-head sizing, exact continuation reservation, device-only exact/candidate packing, bounded refits, all-owner publication and actual old-buffer retirement | Internal post-ingestion building block; serving integration pending |
| `backends/metal_context_session.zig` and `context_storage.metal` | Bounded owner storage, real-query reservoirs, exact SWA and rolling rings, device-only copies, repeated age-block compaction and all-owner publication | Native request factory and qualification tests; HTTP/CLI activation remains gated |
| `runtime/tier/memory.zig` | Uses the same context allocation estimate for per-request and process-wide admission, including heterogeneous batch policies | CLI/HTTP admission; unchanged estimates for exact mode |
| `runtime/kv/context_session.zig` | Typed synchronous device-reference contract, session identity, boundary-aware prefill and metrics | Native decode state and Metal attention |
| `runtime/kv/compaction.zig` and `linalg.zig` | Bounds the legacy host prototype before whole-context allocation and rejects nonfinite solver inputs/results | Existing internal reference paths |

The CPU and Metal fitters accept at most 512 original rows, at most 4,096
training plus validation queries, and head dimensions through 512. The Metal
fitter now accepts explicit lossy retention counts from one through source
rows minus one; exact retention bypasses fitting and copies original f16 KV.
The existing online session continues to request half blocks. This expands
internal storage/solver shapes without qualifying a new model profile.
The session reserves up to 3,072 training and 1,024 held-out rows per KV head,
distributed across its donor consumers and GQA heads. The recent-token interval
can reduce these counts. This replaces the initial 512-row total, which supplied
too few positions per consumer to constrain a 256-key fit. The complete larger
query allocation and fitter workspace are included in admission.
They rank keys by RMS attention importance so minority query classes are not
lost behind redundant high-average-mass keys. This follows the highest-attention
selection method in [Attention Matching, section 3.3](https://arxiv.org/html/2602.16284v2#S3.SS3).
If that candidate fails quality validation, the session retries once with bounded
matching pursuit: residual correlation selects up to eight keys per round,
interleaved with eight nonnegative coordinate-descent sweeps. Both selections
use the same final fit and validation tolerances. The retry reuses the physical
workspace budget after the failed candidate has completed and been freed.
Negligible-norm columns retain a positive mass floor so their stored logarithms
remain finite. Nonfinite input, divergence and cancellation do not trigger retries.
They reject nonfinite inputs, invalid shapes, divergent fits, out-of-range f16
values, and held-out mass/output errors. The value solve fits a correction to
selected original values to avoid losing the ridge anchor in rank-deficient
blocks. Each block is fitted once. The session captures actual post-RoPE,
model-scaled queries at the generic
attention boundary. Every global KV owner has disjoint training/held-out
strata for every donor consumer and GQA query head. No key proxy is used.
The training/validation split chooses one held-out position per quartet with
a deterministic varying phase, avoiding fixed-stride aliasing with periodic
prompts. Pool sizes include boundary slack so all captured queries still lie
after the age block being fitted.
The fitter currently models scaled
attention without logit softcapping, matching the initial Gemma4 target.

The Metal fitter operates on one KV head of an interleaved source block and
returns immutable per-head candidate buffers. It checks an explicit physical
staging budget before allocation, submits all stages as one frame, and waits
before reading its fixed 32-byte report. Cancellation and failed fits drain
GPU work before freeing staging. No source KV, scores, or fitted KV are copied
to the host by the fitter. A GPU packing operation interleaves completed heads
and their mass bias into reserved staging, with alias and range checks.
The session packs candidates into an unused 256-row arena page per owner and
publishes only after every global owner succeeds.

The Metal kernel consumes already rotated keys. It never rebases positions and
rejects using a compacted block for a query inside its original age interval.
Its softmax adds the fitted log mass after attention scaling and optional
softcapping. Exact spans carry zero mass bias. The context runtime has its own
command queues. The model-facing hook drains
the producer frame before consuming its tensors and returns only after all
context GPU operations finish. Gated-block and attention-residual fusion
decline context-session requests so they cannot bypass query capture. This
synchronous route is not the optimized split-decode/flash implementation.
Attention reads a four-byte completion status to reject nonfinite inputs or
outputs; it does not read queries or KV back. Fitter status remains a separate
32-byte report, including training and validation fit diagnostics. Evaluation
bits distinguish measured zero errors from stages that did not run. A finite
held-out mass failure no longer stops other validation rows: the fitter computes
the complete held-out mass maximum and output RMSE before rejecting quality.
This removes thread-scheduling dependence from the reported maximum.

The session serializes mutation with its request lock and rejects concurrent
use or stale identities. Each global owner preallocates one KV arena containing
the protected prefix, 256-row pages for retained data, and one additional
unpublished candidate page. The budget includes arena/page slack, query
buffers/index uploads, two host descriptor generations, a shared 512-row source
gather buffer pair and the largest simultaneous fitter workspace/candidate.
The explicit retained-row limit excludes both page slack and the candidate
reservation. Extra physical space cannot authorize a request that exceeds that
limit, including after quality fallback.
Exact local attention uses a window plus in-flight-row ring. Rolling global
attention adds a protected prefix and masks the tail separately for every query.
After all layer consumers finish a chunk, extra in-flight rows leave the retained
view while their physical capacity stays reserved for the next chunk.

A maintenance transaction fits only original 512-token age blocks. Previously
compacted blocks, the prefix and exact tail retain their bytes and physical
locations. An original block occupying nonadjacent pages is gathered entirely
on device into the shared scratch pair; contiguous sources need no copy.
Every owner must validate before an allocation-free publication swaps all
descriptor generations. The two old source pages become reusable only after
publication and reader completion. Failed fits leave published bytes,
descriptors and the epoch unchanged; the unpublished candidate page is reusable.
A quality-only rejection can take the
bounded exact-remainder path described above after transaction cancellation;
other failed model steps poison the request so it cannot retry against partially
appended KV. Generic prefill also declines
chunk-resize retries for a context session after an allocation failure. Cancellation drains synchronous
GPU work before buffers are freed. Kernel/driver and model activation allocations
remain the responsibility of backend admission; the context budget covers its
own data allocations. Candidate staging is bounded independently of context
length instead of duplicating the whole retained KV allocation. Halving retained
rows still does not imply halving physical memory: calibration, page slack and
fitting workspace are charged as well. A production memory claim additionally
requires measured process/device high-water marks with model activations and
driver allocations included.

The native decode-state hook preserves logical token counts and chooses prefill
chunks that stop at maintenance boundaries. Generic decode recognizes retained
context without a legacy `KvView`, so sampling and grammar forward only the new
token. Global attention rejects gaps in represented history. The route rejects shared scheduler batching,
compiled graphs, prompt caching, speculation and media for this route. A caller
must keep the session alive until every request call has returned. The factory
passes request cancellation/deadline checks through maintenance and logs context
statistics before destruction. Metrics include
logical/retained rows, live/staged/query/peak bytes, compaction duration, rejected
transactions, epoch and fixed-size fitter status readbacks. The reported peak
is the admitted upper bound, not a measured device high-water mark. The session does no
KV readback; global backend transfer counters still need end-to-end auditing.
`fit_fallbacks` records additional fits, `exact_tail_fallbacks` records retained
exact remainders, and status bytes include rejected attempts as well as successful fits.
`source_gather_bytes` counts device-only copies of fragmented original blocks;
it excludes query capture and candidate packing and must not be interpreted as
a backend-wide transfer audit.

## Remaining implementation

1. Resolve real-query fitting and generalization failures without weakening the
   quality gates. RMS selection followed by a bounded matching-pursuit retry
   still fails on the installed E2B and E4B probes below. More NNLS iterations
   alone did not fix the E2B maximum error. Review query coverage across donor
   consumers and time, key selection, and the mismatch between the least-squares
   objective and the maximum-error acceptance criterion. Passing training fits
   is insufficient; the independent held-out set must also pass.
2. Qualify the new admission-to-factory path end to end. CLI, HTTP and serial batch
   admission use the same context estimate; the factory rejects a missing or
   insufficient physical reservation. Existing serving gates remain disabled;
   merely accepting the schema is not support.
3. Qualify the generic model route at long context, then integrate captured-query storage and
   mass bias into optimized fused decode, split reduction and flash prefill.
   The current session deliberately declines fusion. Shared scheduler batches
   and graph replay need explicit per-item ownership and completion contracts.
   The serial fallback does not establish shared scheduler qualification.
4. Extend the short generation checks to long contexts, complete tool
   outputs, streaming cancellation, request isolation, model reload and GPU
   failure. Expand exact SWA rings in the existing serving route only after all
   relevant fallback paths stop gathering evicted KV.
5. Extend the per-request context log to server telemetry/results and audit
   backend-wide KV transfers and measured physical high-water marks. Charge backend
   driver/pipeline allocations together with context and activation budgets.
   Measure the physical memory benefit of block staging over exact storage;
   admission estimates and the owned-buffer allocation audit alone are insufficient
   for a complete process/device high-water claim.
6. Implement the legacy `0.5` migration only after the corresponding execution
   route is qualified (`1` identity is implemented).
7. Replace constant-disabled capabilities with artifact/device/route-specific
   qualification receipts after the independent E2B/E4B campaigns below.

## Local artifact metadata

Both local Q4_0 decoder files were inspected and SHA-256 hashed on 2026-10-16;
rehashing on 2026-10-17 produced the same identities.
These hashes identify the local inputs; they do not establish release provenance
or model qualification.

- E2B: 3,349,516,256 bytes, SHA-256 `fa401b55b07ee70a54c6dae3903c783a6e65064312529ea57175cb5f8dec6634`.
- E4B: 5,154,941,280 bytes, SHA-256 `676c35070db6dbe52f93e9c864ee0fba4eddea94b9c875d9cb10daff453fbaee`.

| Artifact | Positional limit | Layers | Query / KV heads | Native SWA | Shared KV tail |
| --- | ---: | ---: | --- | ---: | ---: |
| `gemma-4-E2B_q4_0-it.gguf` | 131072 | 35 | 8 / 1 | 512 | 20 layers |
| `gemma-4-E4B_q4_0-it.gguf` | 131072 | 42 | 8 / 2 | 512 | 18 layers |

Both advertise 256-dimensional SWA heads and 512-dimensional global heads.
E2B's schedule has four sliding layers per global layer; E4B has five. Their
complete GGUF schedules are preserved rather than reconstructed from this
pattern.

## Verification and release criteria

Run the focused suite from `zig/` with the bounded builder:

```sh
ANTFLY_ZIG_MAX_RSS=8589934592 python3 tools/run_bounded_zig_build.py build \
  inference-test-context -j1 -Dmetal=true -Doptimize=ReleaseFast -Dstrip=true
```

Use `-Dmetal=false` for host tests. The test root explicitly references the CLI,
single-request HTTP, and batch HTTP entrypoints so Zig's lazy analysis also
checks executable policy wiring. This suite is correctness evidence, not a
production qualification campaign.
Request cleanup excludes the Metal session destructor at compile time when
Metal is disabled, so the new ownership path does not introduce Metal linker
dependencies into CPU builds.

Before enabling either approximate mode, pin the source revision, artifact
hashes, tokenizer/chat-template identity, model configuration, Metal compiler,
OS/device, KV dtype, and execution route. Qualify E2B and E4B independently with
matched exact baselines. Cover 2K, 8K, 32K, 64K and the artifact limit, with
output capacity reserved, plus page/window/chunk/compaction boundaries.

Required coverage includes exact tensor/logit parity, appended-token attention
mass, donor/GQA geometry, repeated compaction, grammar and tool outputs,
sampling, streaming cancellation, batch isolation, allocation failure, GPU
failure, model reload, and multi-hour serving. Media and speculation remain
separately gated. Approximate evaluation must satisfy the planned limits of
2 percentage points aggregate task regression, 5 points in any evaluated
bucket, and 5 percent relative perplexity regression. Exact performance must
remain within a matched 5 percent regression envelope.

Memory qualification requires the admitted physical envelope to cover live
KV, unpublished candidate pages, query reservoirs and workspace, with zero full-KV
host readbacks. Swap-affected or unmatched timing is not release evidence.
Unit fixtures passing on Metal do not satisfy these model-level gates.

An opt-in installed-artifact probe compares complete logits and greedy IDs for
an eight-token prefill against two four-token chunks through the real model:

```sh
ANTFLY_CONTEXT_MODEL_DIR=/absolute/path/to/model-directory \
ANTFLY_ZIG_MAX_RSS=8589934592 python3 tools/run_bounded_zig_build.py build \
  inference-test-context -j1 -Dmetal=true -Doptimize=ReleaseFast -Dstrip=true \
  '-Dtest-filter=context Metal installed Gemma'
```

This test does not enable approximate serving and is a short correctness probe,
not long-context, compaction-quality or performance qualification.

Installed-artifact checks on 2026-10-16 passed independently for E2B and E4B:

| Check | E2B | E4B |
| --- | ---: | ---: |
| Maximum logit error, eight tokens versus two four-token chunks | 0 | 0 |
| Maximum logit error versus existing paged Metal route | 0.0065663457 | 0.015484333 |
| Context allocation envelope for the eight-token probe | 7,127,040 bytes | 22,249,472 bytes |
| Greedy, sampled and grammar generation | Three tokens each | Three tokens each |
| Streaming stop callback | Stops after two tokens | Stops after two tokens |

The generation probes verify logical positions and the absence of a legacy KV
sequence. The parity reference uses the existing paged store, including Gemma's
shared donor layers. A null decode context is not a valid reference for that path.
These results establish short model-path correctness, not long-context quality.

Setting `ANTFLY_CONTEXT_COMPACTION_PROBE=1` also runs a 1,089-token
prompt with actual model queries, one attempted 512-to-256-token age-block
transaction, and a matched exact baseline. The default input is a repeating
control-token pattern. Set `ANTFLY_CONTEXT_PROBE_TEXT_FILE` to a UTF-8 file to
exercise natural text through the artifact's tokenizer; the file must provide
at least 1,088 tokens and is capped at 1 MiB. The probe records its input kind.
The current natural-text input is the repository's `README.md`.

A rejected candidate fails this opt-in qualification test. If the runtime safely
finishes using the exact-remainder fallback, the probe checks its full final
logits, greedy token and capacity, then still fails with `CompactionNotPublished`.
Successful fallback must not be mistaken for successful compaction.

With the enlarged calibration envelope, the strict probes still reject:

| Input | Artifact | Training mass error | Held-out mass/output | Candidate |
| --- | --- | ---: | --- | --- |
| Periodic control tokens | E2B | 12.483416% | Not evaluated after training failure | Rejected |
| Periodic control tokens | E4B | 6.701164% | Not evaluated after training failure | Rejected |
| README natural text | E2B | 31.715593% | Not evaluated after training failure | Rejected |
| README natural text | E4B | 28.616652% | Not evaluated after training failure | Rejected |

The maximum permitted mass error remains 5%. The broader query history exposes
errors missed by the original smaller training sample. Before that expansion,
E2B's training error was 5.0320618%, while E4B passed training but failed the
held-out mass gate at 12.949556%. Replaying the captured E4B block with complete
validation measured 2.56% output RMSE; the mass gate still rejected it.

Test-only numerical captures are available with
`ANTFLY_CONTEXT_PROBE_DUMP_PREFIX=/absolute/path/prefix`. After a failed first
transaction, the test can write the original 512-row block and captured queries
for each fully initialized global owner. This diagnostic deliberately reads
shared GPU storage on the host. It is absent from serving, does not change
capabilities or quality thresholds, and cannot turn a failed probe into a pass.

Local correctness tests cover the enlarged CPU/GPU envelope, last-row nonfinite
validation, complete held-out error reporting independent of row order, and
all-owner rollback followed by exact continuation only when the reservation can
hold the whole remainder. Rollback is tested both before and after an earlier
successful compaction. They also cover exhaustion without headroom, which
must remain a request failure, and distinguish both outcomes in metrics.

Before the block-staging refactor, the CPU suite selected 185 tests: 162 passed and 23 hardware or optional
tests skipped. That pre-refactor Metal suite selected 185: 180 passed and five optional
tests skipped. The three critical fitter/fallback regressions also passed with
Apple's Metal API validation enabled. Formatting and whitespace checks passed.

All four installed-model probes completed 1,089 logical tokens by retaining
exact KV after the failed fit. They retained 1,089 rows, preserved epoch 1,
recorded one exact-tail fallback, matched the exact baseline's greedy token,
and reported no testing-allocator leaks. The strict compaction tests still failed with
`CompactionNotPublished`; no candidate was published.

| Input | Artifact | Maximum final-logit difference from exact baseline |
| --- | --- | ---: |
| Periodic control tokens | E2B | 0.003460884 |
| Periodic control tokens | E4B | 0.0057411194 |
| README natural text | E2B | 0.007540226 |
| README natural text | E4B | 0.0111198425 |

The pre-refactor admitted context allocation for these probes was:

| Artifact | Exact allocation | Attention-matching allocation |
| --- | ---: | ---: |
| E2B | 14,991,360 bytes | 71,188,480 bytes |
| E4B | 44,662,784 bytes | 129,155,072 bytes |

These are charged context allocation envelopes, including query buffers and
staging, not measured process/device high-water marks. Model weights, model
activations and driver/pipeline allocations are separate. These historical
figures include the complete shadow generation, which block staging removes.

These are local correctness checks on a host with swap already in use. They do
not establish clean-memory performance, long-context task quality, measured
production memory high-water marks or serving soak. Approximate serving must
stay disabled until fitting and the independent release campaigns pass.

## Block-staging validation (2026-10-17)

The full Metal suite after the arena refactor selected 186 tests: 181 passed,
five optional cases skipped. It includes ten compactions with fragmented
source pages, varying values, shared consumers and an independent analytical
attention result, plus byte-level preservation of published state on rejection.
The final CPU suite selected 187 tests: 163 passed and 24 Metal/optional cases
skipped, with no failures.
A subsequent focused run with `MTL_DEBUG_LAYER=1` passed all six selected tests,
including the newly added request-lifecycle test. After driver warmup, eight
measured request lifetimes each returned `currentAllocatedSize` to 10,551,296
bytes; the observed live-request peak was 24,674,304 bytes. This verifies those
resources were released; it is not a process-RSS or serving-soak claim.
The short installed E2B and E4B exact-context tests also passed with Metal API
validation enabled: two tests per artifact cover chunked/full/legacy logit
parity, sampling, grammar and stream stopping through borrowed session hooks.
They do not establish the admission/factory/HTTP path or long-context quality.

All eight installed-model probes below failed with `CompactionNotPublished`
after validating bounded exact fallback. Each reported one exact-tail fallback,
publication epoch one, all logical rows retained and zero testing-allocator
leaks. Training mass validation failed before the GPU held-out/output checks,
so those later errors are unavailable, not zero. No candidate was published.

| Artifact | Input | Prefix / recent | Tokens | Maximum training mass error | Maximum final-logit difference after exact fallback |
| --- | --- | --- | ---: | ---: | ---: |
| E2B | README | 256 / 2048 | 2817 | 27.106% | 0 |
| E2B | Periodic control | 256 / 2048 | 2817 | 11.959% | 0 |
| E4B | README | 256 / 2048 | 2817 | 39.785% | 0 |
| E4B | Periodic control | 256 / 2048 | 2817 | 10.207% | 0 |
| E2B | README | 64 / 512 | 1089 | 31.716% | 0.007540226 |
| E2B | Periodic control | 64 / 512 | 1089 | 12.483% | 0.003460884 |
| E4B | README | 64 / 512 | 1089 | 28.617% | 0.0111198425 |
| E4B | Periodic control | 64 / 512 | 1089 | 6.701% | 0.0057411194 |

Allocation estimates using the loaded layouts and default protected regions:

| Artifact | Logical limit | Retained cap | Exact context bytes | Matching context bytes | Candidate staging bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| E2B | 32768 | 17664 | 209,534,976 | 169,230,336 | 1,572,864 |
| E2B | 65536 | 34048 | 410,861,568 | 270,090,240 | 1,572,864 |
| E4B | 32768 | 17664 | 563,576,832 | 414,859,264 | 4,194,304 |
| E4B | 65536 | 34048 | 1,100,447,744 | 683,819,008 | 4,194,304 |

These are admitted context envelopes, including bounded query/scratch storage,
not successful long-context runs or measured total device/process peaks. The
64K estimates are about 34% and 38% below exact respectively; that benefit is
conditional on successful compaction. With current fitting failures, those
tight-cap requests would reject at the first failed candidate. The 2817-token
matching envelopes are instead larger than exact (79,380,480 vs 25,608,192
bytes for E2B; 175,325,184 vs 72,974,336 for E4B), because fixed fitting/query
workspace dominates small requests. The refactor removes full-cache shadow
storage; it does not establish an unconditional memory saving.


## Per-head packed storage implementation (2026-10-17)

The new internal `backends/packed_context_storage.zig` implements the allocation
and publication portion of accepted step 4. It does not select payloads, produce
a qualified retention policy, or replace the serving session yet.

- ABI 2 has explicit per-head KV, bias and span ranges. Validation rejects
  aliases, orphan spans, unsupported versions, overflowing indexes and missing
  history in any head. The legacy interleaved ABI remains supported. The
  retained-row result is the maximum across heads, never their average.
- A counts-only pass validates supplied per-head retention decisions and
  reserves the entire exact continuation. Plans bound head/span counts and
  require a final exact segment. Only complete 512-token blocks can become
  lossy. The packed builder uses even head dimensions for Metal buffer-offset
  alignment; Gemma's 256/512 dimensions satisfy this requirement.
- Destination K/V and bias arenas are allocated once at their final sizes.
  Exact source rows are gathered entirely on the GPU, preserving f16 bits.
  `fitAndPack` caps each refit at the admitted workspace peak and releases its
  candidate before returning. The original relative-mass solver and strict
  quality gates are unchanged; this is not a port of the authors' QR solver.
- A batch admits coexistence of old and new arenas, conservatively page-charged
  host metadata, staged generation records, and one fitter workspace peak.
  Model weights, activations, query pools, backend pipelines and attention's
  temporary descriptor uploads need separate request-level charges when this
  builder is integrated. It rejects a transaction unless both total owned
  physical bytes and actual page-rounded device-buffer bytes decrease.
- All owner candidates must be complete before publication. Cancellation or
  failed fitting leaves live descriptors and buffers unchanged. Active read
  leases delay the swap. Synchronous GPU completion precedes freeing old
  backing allocations. The request's sequence lock must serialize these APIs;
  the builder does not introduce another scheduler or lifecycle owner.
- Appends reject active publication/readers and stale epochs. They write only
  reserved tail capacity and update all head descriptors after the copies
  complete. Cancellation keeps the published logical length unchanged.

The new tests cover 128/384/exact-512 head layouts, GQA, both Gemma head widths,
causal continuation, and softcap before mass bias against an independent FP64
expanded-history oracle. Fitter fixtures cover both key selectors at 1, 128,
384, 448 and 511 rows in addition to existing 256-row tests. These shape tests
are not calibrated profile recommendations. Transaction tests exercise two
owners, allocation failures, peak-budget rejection, nonfinite fits, cancellation,
reader drain, exact-head bit preservation, append exhaustion, and device-buffer
retirement. The final Metal suite selected 206 tests: 200 passed and six optional
cases skipped, with Apple Metal API validation enabled. The two-owner fixture
reduced page-rounded device buffers from 3,211,264 to 2,195,456 bytes (31.6%).
At commit, the measured Metal allocation dropped by exactly 3,211,264 bytes,
confirming that all old buffers were released. The net committed buffer saving
was 1,015,808 bytes. This is fixture storage evidence, not installed-model
compression, total-process memory, clean-memory performance or serving proof.
Log: `/private/tmp/antfly-context-v12-metal-final.log`; source/binary binding:
`/private/tmp/antfly-context-v12-metal-build-receipt.json`.
The final CPU-only suite selected 206 tests: 176 passed and 30 hardware/optional
cases skipped. Both binaries are bound to the same 38 changed Zig workspace
files and unchanged HEAD. Formatting and whitespace checks passed. The complete
local validation record is `/private/tmp/antfly-context-v12-validation.json`;
`/private/tmp/antfly-context-v12-incremental.patch` isolates this checkpoint from
the prior dirty worktree, and `/private/tmp/antfly-context-v12-source.zip` preserves
the full uncommitted source and evidence. No commits, pushes, CI dispatches or
new installed-model qualification runs were performed.

Remaining work from the accepted plan is unchanged in scope: provenance through
Jinja and the actual HF BPE tokenizer, immutable shared-KV replay ownership,
calibrated first-pass budget selection/retries, integration with the request
session and admission, optimized decode, and artifact-specific release/soak
campaigns. Existing E2B/E4B numerical failures remain blockers. No approximate
capability flag is enabled by this storage implementation.

## Payload provenance and replay ownership hardening (2026-10-17)

The next local increment adds explicit selectors to the OpenAPI contract and
CLI (`--compact-part MESSAGE:PART`). The policy owns a bounded 64-entry selector
array, avoiding borrowed JSON/CLI lifetimes. Duplicate selectors, invalid indices,
empty/nontext parts and roles other than user/tool are rejected. Exact/rolling
policies cannot silently accept attention-matching selectors. SDKs are regenerated
from the source schema; HTTP reports provenance errors as client errors.

Jinja's opt-in provenance renderer copies selected strings into distinct request
arena allocations and propagates byte labels through slices, trimming, string
concatenation, joins, macros and captures. Template literals and metadata remain
protected. The implementation uses allocation identity, not substring matching;
identical protected/selected strings stay distinct. Unsupported case transforms
on selected text fail closed. Ordinary rendering retains its existing path.

Gemma's recognized Replace-space/Split BPE profile now exposes original UTF-8
byte spans from the actual merge/direct-piece emission path. Replacement bytes
map back to their original space; byte-fallback tokens retain their original byte
spans. Added tokens and BOS/padding have empty protected spans. Unsupported
normalizers/tokenizers fail explicitly. The optional path bypasses the ID-only
cache; ordinary generation does not allocate provenance. The shared
`prepareCompactionPayload` boundary checks byte/token identity, protects special,
prefix, tail and boundary-crossing tokens, and emits eligible per-part token
ranges and full 512-token candidate blocks. This preparation boundary is not yet
called by the public approximate serving lifecycle, whose capability remains off.

Packed generations now offer explicit immutable replay leases. A lease retains
both the Metal alias and its host backing pages, plus descriptor metadata. An
original generation can be retired without invalidating a surviving lease.
Append and all-owner publication reject outstanding replay readers; the latter
must actually release the old allocation to claim a reduction. Lease acquisition
is serialized by the sequence lock; release follows GPU completion. Allocator
lifetime must include all leases, and the control allocation is separately
budgeted. The live `metal_context_session` still needs migration to these packed
owners and a base-plus-private-tail attention route. The existing installed-model
capture continues to use its separate full private session; it does not establish
shared live-branch integration.

The offline fitter now records query-coverage diagnostics without using held-out
queries for selection. On the previously sealed E2B README capture, the 448-row
RMS selection omits 6/4/7 keys in owners 4/9/14 respectively whose maximum training
probability is at most 5% but whose held-out maximum exceeds 5%. Before mass
refitting, the maximum omitted held-out mass is 99.05%/24.85%/56.62% respectively.
These are diagnostics, not new passing fits or quality qualification. The existing
5% mass and 10% output gates remain unchanged. The earlier complete numerical
campaign rejects every lossy E2B candidate. A fresh E4B campaign now also has a
sealed verdict, reported below; the earlier resource-stopped run is not used as
numerical evidence.

`release_attention_matching.py` adds a serial local-driver soak and a separate
release-evidence auditor. The driver must emit an identity-bound report of actual
publication, reduced committed allocation, zero backend KV readback, clean memory,
request release and isolation. Failed/timed-out trials retain terminal receipts
and terminate the driver's process group. Short runs are diagnostics. The auditor
requires separate E2B/E4B numerical/quality/failure/performance evidence, disjoint
calibration/evaluation documents, all five task buckets, 4K/8K generation, native,
CLI, HTTP, streaming and mixed serial batch routes, and a four-hour non-idle soak.
It rejects stale hashes, exact fallback, nonfinite metrics and weakened quality
thresholds. Even successful evidence auditing does not enable runtime capability.

```sh
python3 zig/pkg/inference/scripts/gemma4/release_attention_matching.py soak \
  --driver serving-driver-argv.json --identity capability-identity.json \
  --output-dir /tmp/gemma4-soak --duration-seconds 14400
python3 zig/pkg/inference/scripts/gemma4/release_attention_matching.py audit \
  release-manifest.json --output release-audit.json
```

The driver JSON is an argv array, executed without a shell. One persistent
process owns the server/model for the entire soak. The harness sends one JSON
line `{"command":"next","request_index":N}` on stdin; the driver exercises a
real serving request, releases its request allocations, then emits one JSON
report line with the same `request_index`. `{"command":"stop"}` requests clean
shutdown. Logs belong on stderr. Native/CLI trials and the resident HTTP server
must remain inside the driver's process group so its RSS is measured. The
harness independently checks process-group RSS, free memory/disk and cumulative
swapout growth, and requires an unchanged live-allocation baseline across
requests. Reports are journaled incrementally; trial count and output sizes are
bounded, and timeout/cancellation kills descendant processes. Unit-test drivers use
synthetic evidence only. No actual four-hour serving soak or release acceptance
is claimed by the harness tests. Required next integration remains: a passing
frozen Gemma4 numerical profile on disjoint documents; live packed-owner/private
replay-tail integration; native/HTTP factory publication; optimized attention
parity; actual paired generation and clean-memory release/soak campaigns.

An additional diagnostic used the unchanged relative-CD/ridge/RMS solver at
480/496/504/508/511 rows, beyond the accepted calibration grid. Owner 4 still
failed at 511 rows (16.13% held-out maximum mass error). Owners 9 and 14 first
passed at 508 and 504 rows, with held-out mass errors 2.51% and 3.35%. These
near-exact observations are not a frozen production policy, separate-document
evaluation, Metal parity result or page-rounded allocation-saving claim. The
published calibration grid and release gates are unchanged.

Local v13 validation uses the same 47 changed Zig workspace file hashes for
Metal and CPU binaries. The Metal context suite passes 207/214 tests with seven
installed-model tests skipped; the CPU suite passes 182/214 with 32 Metal/model
skips. Separate installed E2B and E4B provenance probes both pass with Metal API
validation: string prompts contain 7210 tokens and structured prompts 10808;
each selects exactly 3598 tokens and seven full candidate blocks, preserving
ordinary rendered bytes and token IDs. Peak probe RSS is 305.13/299.44 MiB,
respectively, with zero swapout growth. These probes do not run generation or
publish compaction.

The tokenizer suite passes 89/91 tests (two optional fixture skips), the Jinja
suite passes, and 35 Python calibration/capture/release tests pass. OpenAPI build
path tests pass (10); Python generated-client consistency and regenerated Go SDK
compilation pass. TypeScript types use pinned Node 24.16.0. The existing packed
Metal publication fixture still releases 3,211,264 bytes at commit and reduces
owned device allocation to 2,195,456 bytes; that is a fixture storage result,
not an installed-model compression claim. Approximate capability flags remain
false. Detailed logs and source/binary receipts are under
`/private/tmp/antfly-context-v13-*`.

The fresh E4B repeat-prefill capture completed in 379.85 seconds with Metal API
validation, zero system swapout growth, peak process-group RSS 532.70 MiB and
minimum reported free memory 26%. The source-bound capture is a correctness
measurement; pre-existing swap and the diagnostic route prevent a clean-memory
serving performance claim. Four global owners and both KV heads were captured,
including all shared-layer/GQA consumer strata.

All 128 lossy candidates (four recipes at 128/256/384/448 rows, across eight
owner/head pairs) failed the unchanged numerical gates. All eight exact-512
controls passed. The 448-row relative-CD/ridge/RMS results were:

| Owner | KV head | Held-out maximum mass error | Held-out output relative RMSE |
| --- | --- | ---: | ---: |
| 5 | 0 | 99.14% | 16.20% |
| 5 | 1 | 99.78% | 20.09% |
| 11 | 0 | 61.11% | 4.28% |
| 11 | 1 | 41.01% | 4.36% |
| 17 | 0 | 21.39% | 2.67% |
| 17 | 1 | 13.89% | 2.14% |
| 23 | 0 | 94.77% | 18.26% |
| 23 | 1 | 13.01% | 5.07% |

Adaptive profile freezing correctly refused an all-exact result with no memory
reduction. Therefore no frozen E4B profile, disjoint-document evaluation or
release qualification was produced. The capture receipt, numerical receipt and
terminal failure-to-freeze record are under
`/private/tmp/antfly-context-v13-e4b-numerical/`. This result joins the earlier
48 failed E2B lossy candidates and three passing exact controls. It does not
establish that attention matching cannot work for Gemma4; it establishes that
the current query/selector/solver combinations cannot be released.

The production work now has these concrete boundaries:

| Area | Implemented and checked | Release work still required |
| --- | --- | --- |
| Numerical qualification | Sealed E2B/E4B captures, all-consumer gates, solver comparison, query-coverage diagnostics, rejection of all-exact profiles | Improve training-query coverage or key selection, freeze a passing profile, evaluate disjoint documents and generated continuations, establish Metal parity |
| Payload provenance | Owned API/CLI selectors, Jinja byte labels, actual Gemma BPE spans, installed-artifact token-identity probes | Call the common preparation boundary from the post-ingestion request lifecycle |
| Shared replay ownership | Refcounted original host pages/Metal buffers, immutable leases, publication/append exclusion, GPU lifetime test | Integrate packed global owners and a private replay tail/SWA into the live model branch |
| Serving integration | Generated API/SDK contract, native CLI serialization, request validation/error mapping, fail-closed legacy-session guard | Native/HTTP factory publication and admission for the packed lifecycle; optimized decode parity across serving routes |
| Release and soak | Identity-bound evidence auditor, persistent serial-driver protocol, independent resource guards, failure receipts, synthetic harness tests | Real paired generation/quality/performance campaigns and four-hour E2B/E4B serving soaks on the integrated route |

The next numerical change must use training data only for query/key selection;
held-out diagnostics cannot be fed back into a selector for the same validation
claim. Keep the frozen-profile and independent-document boundary, and require
actual page-rounded savings when choosing adaptive exact/compacted heads.
Implementing the remaining lifecycle can proceed behind the disabled capability,
but enabling serving or spending four hours soaking an exact fallback cannot
substitute for resolving the numerical blocker.

The complete uncommitted checkpoint and evidence index are preserved in
`/private/tmp/antfly-context-v13-source.zip` and
`/private/tmp/antfly-context-v13-validation.json`. The incremental patch is
relative to the v12 source archive. No commits, pushes or CI dispatches were made.

## High-retention numerical path and private replay attention (v14)

Three training-only key selectors were first tested on the sealed calibration
captures: peak attention, greedy minimization of the worst omitted training
mass, and farthest-point key diversity. None passed at 448 rows for any of the
11 E2B/E4B owner/head pairs. These are development diagnostics; no alternative
selector was added to the runtime.

The unchanged relative-CD/ridge/RMS fitter was then evaluated on an exploratory
480/496/504/508/511/512 grid. This extends the development grid without changing
the 5% mass or 10% per-consumer output gates. Separate per-artifact adaptive
profiles were frozen on README calibration data before capturing the evaluation
document (`docs/design/relational-restore-architecture.md`). Each profile fixes
the initial counts, solver and increasing-retention retry schedule. Sensitive
heads remain exact. Both artifacts passed the independent-document numerical
checks, with these final evaluation choices:

| Artifact | Owner | Per-head retained rows out of 512 |
| --- | --- | --- |
| E2B | 4 | 512 |
| E2B | 9 | 511 |
| E2B | 14 | 504 |
| E4B | 5 | 512 / 511 |
| E4B | 11 | 512 / 508 |
| E4B | 17 | 504 / 508 |
| E4B | 23 | 512 / 504 |

The maximum held-out mass error across selected fits is 3.283%; the maximum
held-out output relative RMSE across all consumers is 1.599%. Calibration and
evaluation captures use the v13 binary and its archived source receipt. The
fresh E4B evaluation and E2B calibration/evaluation captures all completed under
the resource guard with zero system swapout growth. This is one calibration
document and one evaluation document per artifact, not the release corpus,
generated continuations or a rendered-chat serving qualification.

`metal_attention_matching_qualify.py` and the opt-in
`context Metal sealed capture fitter qualification` test now execute the actual
Metal fitter on selected sealed inputs. The driver checks the capture and build
identities, recomputes the FP64 fit, transfers the real f16 K/V and f32 queries,
and independently evaluates the GPU-produced f16 K/V and f32 mass biases. It
checks every consumer, exact selected-key identity, device-report agreement
within 0.0002 absolute error, and FP64/Metal fit metrics within 0.002. These
parity tolerances do not relax the quality gates. All seven selected lossy fits
passed with Metal API validation. Four exact heads bypass fitting. Candidate
readback is confined to this qualification test; this does not establish
backend-wide zero-readback serving behavior.

```sh
python3 zig/pkg/inference/scripts/gemma4/metal_attention_matching_qualify.py \
  --binary /path/to/context-tests --build-receipt build-receipt.json \
  --numerical evaluation-numerical.json --output-dir /tmp/metal-fit-parity
```

Numerical acceptance does not imply useful allocation savings. A device-buffer
projection using the actual 16 KiB page size, one evaluated 512-token block per
owner, 2817 original tokens and 512 reserved continuation tokens saves just
32 KiB for each model. E4B owner 5 alone grows by 16 KiB because its bias page
outweighs the removed row. This projection is not a publication measurement and
cannot be extrapolated to unevaluated blocks or context lengths. The packed
builder's existing physical/device reduction checks remain necessary at commit.
Current high-retention profiles are therefore development evidence, not a
production context-window expansion or a substantial compression claim.

`backends/replay_context.zig` now supplies the missing global attention primitive
over an immutable packed base and a separately owned writable tail. It leases
the original pages and Metal objects, uploads only bounded head metadata, casts
appended f32 K/V into the private f16 tail, and computes causal attention over
both buffers without copying the base. Admission bounds the tail to 8192 tokens
and the remaining positional range. It rejects compacted, sparse or wrapped
bases. Cancellation and nonfinite data cannot advance the visible tail length;
a partially failed branch cannot be reused. Temporary bytes and the original
owner's lease-control reservation are accounted separately.

The new Metal test checks both 256- and 512-wide heads, eight-query-head GQA,
causal multirow attention, softcaps, append exhaustion, cancellation and
nonfinite failures against an independent FP64 full-history oracle. A sibling
branch failure leaves original KV and the other branch unchanged. Attention and
append remain valid after the original handle is retired because leases retain
its storage. These tests cover the global replay primitive. A live model branch
must still integrate it with independently owned SWA, query collection and the
request lifecycle; this primitive does not itself provide those components.
The v15 model branch below now implements private SWA and query collection;
the packed live-owner serving lifecycle is still pending.

Next production work is to integrate the packed live owners, private replay/SWA,
payload preparation and all-owner publication into one native/HTTP lifecycle;
qualify the optimized attention route; and run real paired generation,
instruction-retention, failure/reload, allocation, performance and four-hour
serving campaigns. The release corpus still requires all five task buckets at
4K/8K for each artifact. Runtime capability defaults remain false.

Numerical and GPU evidence is under
`/private/tmp/antfly-context-v14-adaptive-numerical/` and
`/private/tmp/antfly-context-v14-metal-parity/`. The separate allocation estimate
is `/private/tmp/antfly-context-v14-allocation-projection.json`.

The final v14 Metal suite passes 209 of 217 selected tests, with eight optional
installed-capture tests skipped; the separate seven-candidate Metal fitter run
passes all selected cases. The CPU-only suite passes 183 of 217, with 34
hardware/optional skips. Both binaries bind the same 52 changed Zig-workspace
files. All 42 Python calibration/capture/release/parity tests, Zig formatting and
whitespace checks pass. The original packed publication fixture still frees
3,211,264 old device-buffer bytes and retains 2,195,456 new bytes; those fixture
numbers are distinct from the small installed-capture allocation projection.

The full uncommitted source and evidence are preserved in
`/private/tmp/antfly-context-v14-source.zip`, with the validation index at
`/private/tmp/antfly-context-v14-validation.json`. Its incremental patch applies
to the preserved v13 source checkpoint. No commits, pushes or CI dispatches were
made.

## Private model replay and query sealing (v15)

`backends/model_replay_context.zig` connects the shared global primitive to the
actual model attention hook. It validates the complete layer/donor layout,
original epochs, exact source coverage, bounded replay horizon and additional
physical allocation before creating GPU state. Every global owner leases its
original packed generation and appends to its own tail. Each sliding owner
copies only its last window into private storage, keeps absolute positions,
and reserves the complete bounded replay continuation. Shared consumers use
their owner's private state. Original KV allocations and backend pipeline memory
remain separately admitted; the branch estimate is an additional peak.

The branch owns position tracking and balanced query pools for every global
owner and query-head/shared-consumer stratum. Calibration and model replay now
use the same deterministic train/held-out partition and uniform sampling code
in `context_query_sampling.zig`. No sampler, grammar, user history or usage
object is borrowed by the branch. Invalid layer order, mismatched positions,
nonfinite attention or a partially failed step make that branch unusable.
Cancellation and stale hook identities are checked before mutation.

A complete replay must be explicitly sealed. Sealing synchronously releases
all global leases and private KV while retaining the captured query pools, so
an all-owner fitting/publication transaction can proceed without active replay
readers. Incomplete or failed branches cannot seal or expose sealed queries.
The remaining query/metadata allocation is reported separately; lease-control
objects remain charged to the original owners until their retirement. A sealed
branch rejects further model execution. Captured pools live until its final
teardown and are only borrowed synchronously by a fitter.

The new Metal fixture exercises wrapped sliding seeds, both sliding and global
shared consumers, chunked causal attention, private query capture, cancellation,
incomplete sealing, stale identities, budget rejection and sibling failure.
It retires the original packed generation **and its creating Metal runtime**
before running the surviving branch. Constant-key attention has an independent
analytical average, including the moving sliding window and GQA head ownership.

The opt-in installed-model test is
`context Metal installed Gemma shared model replay parity`. It ingests 1025
raw-text tokens, then runs the same repeat instruction and selected excerpt
through the private branch and an independently writable exact reference. It
compares every replay position's full logits and greedy token, every captured
query vector, all captured positions, and original KV/metadata/position hashes.
It tests sealing with query pools still alive and proves original leases have
been released. Limits are maximum logit absolute error 0.05, every greedy token
identical and maximum per-vector query relative RMSE 0.001.

This test explicitly converts the legacy oracle's global KV to packed owners
once before forking, reports that migration allocation separately and checks
that the branch itself shares their buffer handles. It does **not** claim that
legacy ingestion is already zero-copy or that the serving factory owns packed
live state. Diagnostic hashes/query comparisons read tensors only in the test;
branch execution performs only bounded status readback. This is exact replay
qualification, not fitted-generation quality or serving qualification.

Run each artifact separately with a source-bound test binary and the existing
resource guard (8 GiB RSS, at least 10% free memory, no system swapout growth,
at least 4 GiB disk headroom):

```sh
MTL_DEBUG_LAYER=1 ANTFLY_CONTEXT_SHARED_REPLAY_PROBE=1 \
  ANTFLY_CONTEXT_MODEL_DIR=/path/to/one/model \
  ANTFLY_CONTEXT_PROBE_TEXT_FILE=README.md \
  /path/to/context-tests \
  --test-filter 'context Metal installed Gemma shared model replay parity'
```

The next serving milestone remains direct packed-owner ingestion and bounded
replay in the native request factory, selected-payload preparation, then atomic
publication and continuation. Optimized attention parity, broad fitted-output
quality, useful measured allocation reduction, clean-memory destination
performance and actual four-hour serving soaks remain release gates. Runtime
approximate capability defaults remain false.

Both installed artifacts pass the v15 exact replay probe with Metal API
validation on Apple M4. Each starts from 1025 original tokens and executes 1037
private replay tokens. Every replay position's full logit vector and greedy
choice matches the independent exact reference, and every captured query vector
matches exactly (maximum absolute and relative errors are zero).

| Artifact | Model layers / global owners | Captured query vectors | Admitted additional branch peak | Sliding seed copied |
| --- | --- | ---: | ---: | ---: |
| E2B Q4_0 | 35 / 3 | 12,232 | 49.3125 MiB | 6 MiB |
| E4B Q4_0 | 42 / 4 | 32,768 | 142.4375 MiB | 20 MiB |

Those branch peaks are the page-rounded admission calculation, not a measured
process or device memory saving. The separately reported legacy-oracle migration
allocations are 6,537,216 and 17,104,896 bytes, respectively. Forking the packed
global bases copies zero KV bytes. Branch status reads total 2,788 and 3,400
bytes; diagnostic hashes, query comparisons and logits are additional test-only
readbacks. All original state hashes remain unchanged, and sealing releases all
global leases while preserving the query pools for fitting.

The resource guard records 107.68 seconds for E2B and 181.44 seconds for E4B,
including loading, prefill, both replay routes and diagnostics. Peak process-group
RSS is 921.47 / 751.89 MiB, minimum system free memory is 41% / 30%, and system
swapout growth is zero for both. Historical swap remains allocated, so these
are bounded correctness runs and cannot support clean-memory serving latency
or capacity claims. Per-artifact receipts, logs, document hashes and command
lines are under `/private/tmp/antfly-context-v15-model-parity/`; model GGUF
hashes are checked before and after each run. Model manifests were rechecked
after E2B and during E4B, and are verified again at checkpoint creation.

The final v15 Metal suite passes 211 of 220 selected tests with nine optional
probes skipped. E2B and E4B each separately pass the opt-in installed-model replay
probe. The CPU-only suite passes 184 of 220, with 36 hardware/optional skips;
both binaries bind the same 54 changed Zig-workspace files. All 42 Python
calibration/capture/release/fitter tests pass. Source, receipts and logs are
preserved in `/private/tmp/antfly-context-v15-source.zip`, with the validation
index at `/private/tmp/antfly-context-v15-validation.json`. The incremental patch
is checked against the preserved v14 checkpoint. No commits, pushes or CI
dispatches were made.

## Direct packed ownership and maintenance lifecycle (v16)

`backends/packed_context_session.zig` adds direct packed global KV ingestion,
starting with an explicitly unpublished empty original. No empty descriptor is
exposed to attention or replay; the first completed append publishes positive
counts. Each global owner reserves its complete exact prompt and continuation.
Sliding owners retain a bounded token-major ring with `window + chunk - 1`
capacity, retire temporary rows only after every shared consumer completes,
and preserve absolute positions across wraparound.

The new owner validates layer order, donor geometry, capacities, epochs,
scale/softcap consistency, buffer aliases and whole-step positions. Partial
model failures poison the request. A prompt-boundary maintenance object freezes
live decoding while a private model branch executes; a failed or abandoned
branch closes without changing the user position. Global leases refer to the
same packed ingestion buffers, so no legacy-cache migration is needed. Closing
maintenance destroys branch state before unfreezing the live request. The
maintenance object must be closed before destroying its request owner.

A sealed maintenance object can fit a frozen per-owner/per-head retention plan
and publish it atomically. It validates every lossy interval against a complete
512-token block and uniform nonzero labels from prepared payload eligibility.
The caller still owns the rendered/tokenized provenance and must use the same
token sequence for ingestion. This internal interface does not establish a
trusted model-profile registry or authorize public approximate serving.

Every changed head uses the existing 5% training/held-out mass and 10%
per-consumer held-out output gates; exact heads copy their original f16 KV.
Softcapped fitting is rejected because the fitter currently implements ordinary
softmax. Budgets include originals, staged generations, sealed queries, one
serial fitter workspace, runtime pages and bounded metadata uploads. Retained
control allocations remain charged even after a branch-construction failure.
Publication requires an actual decrease in both page-rounded device and
physical generation allocations. All owners swap together; a failed second
owner cannot leave the first owner published. Publication epochs remain
monotonic even when an earlier candidate was cancelled.

The native generation pipeline now has an internal `context_storage =
.packed_exact` selection with an explicit physical budget. It creates and
cleans up its own packed owner and restores the prior decode hook on every
exit. CLI/HTTP do not expose this selection. Approximate policy validation
still runs before owner creation; the public capability defaults remain false.
This allows qualification of native sampling, grammar and streaming lifecycle
without changing the default exact execution route.

The installed-artifact direct lifecycle probe uses actual rendered chat with a
selected user payload, a protected system instruction and a protected final
user question. It trims tokenizer padding, ingests exactly those tokens,
replays only prepared eligible ranges, seals queries, checks original state
hashes and continues the user sequence against an independent exact reference.
The reference and the replay branch do not borrow live sampler or grammar
state. Diagnostic state hashes and logits are test-only readbacks.

The fixture additionally exercises two global owners, per-head exact fallback,
sliding/global shared consumers, replay abandonment/cancellation, insufficient
budgets, invalid payload labels, no-saving rejection, a nonfinite second-owner
fit after first-owner staging, atomic publication and post-publication decode.
It uses controlled uniform-attention data; its allocation reduction is not an
installed-model compression result.

Remaining release work: trusted artifact-bound retention profiles and the
post-ingestion coordinator/admission in the native/HTTP approximate factory;
actual installed-model publication with useful memory savings; fitted-output
and instruction-retention quality across the five release buckets at 4K/8K;
optimized attention parity; failure/reload and mixed-request qualification;
clean-memory destination performance; and actual four-hour persistent serving
soaks. The v14 numerical results remain source-bound historical evidence,
including the small 32 KiB projection. They do not establish v16 rendered-chat
publication quality or a useful compression ratio.

Request-owned contexts retain final accounting in
`NativeDecodeState.last_context_metrics` before their owner is destroyed. The
next generation clears it before validation, so an admission failure cannot
report the previous request's counters. Qualification asserts the retained
logical position, physical accounting, epoch and KV readback counter directly,
then submits a zero-budget request and verifies both telemetry clearing and
hook restoration. These checks do not depend on informational logs, which are
suppressed in the ReleaseFast test binary.

The final v16 Metal suite passes **214 of 224 selected tests**, with ten
optional probes skipped. Both installed artifacts separately pass the direct
lifecycle probe and the expanded native generation probe with Metal API
validation. Each direct probe ingests **1205 actual rendered tokens**, has two
eligible payload blocks, executes **1154 private replay tokens**, then checks
four user-sequence continuation steps. Prefill's final full logit vector and
all four continuation logit vectors agree exactly with the independent exact
reference, including every greedy choice. Original global state hashes remain
unchanged during replay, all leases release, and global migration/fork copy
counts are zero.

| Artifact | Global owners | Captured query vectors | Charged live allocation | Additional branch allocation | Sliding seed copied |
| --- | ---: | ---: | ---: | ---: | ---: |
| E2B Q4_0 | 3 | 12,232 | 14,876,672 B | 54,067,200 B | 6 MiB |
| E4B Q4_0 | 4 | 32,768 | 43,909,120 B | 156,565,504 B | 20 MiB |

These allocations are conservative page-rounded accounting, not process RSS
or compression savings. No lossy installed-model generation is published by
this probe. The controlled two-global-owner fixture does publish after replay
and serial fitting: its charged allocation decreases from 8,880,128 to
7,831,552 bytes (1 MiB), with a 19,693,568-byte admitted peak. A nonfinite
second-owner fit first proves rollback; the successful retry uses epoch 3,
then completes three continuation tokens.

The native probe runs greedy, sampled, grammar-constrained and early-stop
streaming requests through both the legacy borrowed owner and the new owned
exact route. Each new owned route reports its actual final metrics and restores
the decode hook. A following zero-budget request fails admission and clears
prior metrics. There are four verified owned requests and four verified
admission rejections per artifact. Approximate serving remains disabled.

| Artifact / probe | Wall seconds | Peak process-group RSS MiB | Minimum free memory | System swapout growth |
| --- | ---: | ---: | ---: | ---: |
| E2B direct lifecycle | 121.36 | 864.84 | 41% | 0 |
| E2B native routes | 16.68 | 417.27 | 44% | 0 |
| E4B direct lifecycle | 204.51 | 567.86 | 29% | 0 |
| E4B native routes | 23.44 | 462.81 | 30% | 0 |

The source-bound final campaign is
`/private/tmp/antfly-context-v16-model-lifecycle-final/`. The controller verifies
model and manifest identity, the exact binary and changed-source hashes, and
unambiguous local artifact/tokenizer selection. The guard enforces 8 GiB RSS,
10% minimum free memory, zero swapout growth, 4 GiB minimum disk headroom and
a 1200-second per-probe deadline. Historical swap remains allocated, so these
are correctness results, not clean-memory performance qualification.

The first campaign's E2B tests passed but its collector rejected suppressed
informational owner logs. That run is preserved separately under
`antfly-context-v16-model-lifecycle/`. Final native ownership assertions use
retained metrics and explicit test diagnostics; the complete final campaign
was rerun against the updated source-bound binary. No log-parsing failure was
reclassified as a passing qualification run.

The CPU-only suite passes **185 of 224 selected tests**, with 39 hardware or
optional probes skipped, and the Python qualification tooling passes all
**42 tests**. CPU and Metal receipts bind the same 55 changed Zig-workspace
files. Formatting and whitespace checks pass. The v16 checkpoint preserves
all 66 dirty files, including prior work; nothing is staged, committed or
pushed. Source/evidence are packaged in
`/private/tmp/antfly-context-v16-source.zip` with validation and archive receipts.
The incremental patch is checked by applying it to an extracted v15 source
snapshot and comparing every resulting file hash.

## Native coordinator and adaptive admission (v17)

`backends/compaction_coordinator.zig` prepares a request before device ingestion:
owned token/eligibility snapshots, a bounded private replay, every global
owner's frozen per-head segments, and a complete conservative KV peak. The
estimate includes exact originals and continuation, replay tails and sliding
seeds, retained lease controls, sealed query pools, staged generations, one
serial fitter workspace, descriptor uploads, stable records, and page-rounded
request metadata. Model weights, activations, tokenizer/rendering allocations,
and driver pipeline admission remain the surrounding request owner's charges.

The plan checks actual page-rounded physical and device savings against a
profile minimum. Retention is checked per head, including the full requested
continuation. `GptGenerationBudgetComponent` requires this prepared request for
attention-matching estimates; it no longer substitutes the legacy 512-to-256
formula. A failed reservation cannot silently shrink an already prepared
request's chunk geometry. Replanning must create a new matching admission.

After ingestion, the coordinator freezes the original, runs a private model
hook, seals captured queries, fits and atomically publishes all owners, then
closes maintenance before releasing the live request. Cancellation and fit
failure close the branch without publishing partial owners. A consumed request
cannot be replayed twice. The native runner uses `forwardHiddenOnly`, so replay
does not project or read back logits, sample tokens, advance grammar, invoke
user token callbacks, or replace the live prefill result.

`GenerationConfig.context_qualification` exists only in test binaries. It
connects actual provenance-aware rendering/tokenization to native packed
ingestion and post-prefill orchestration. It requires exactly matching rendered
bytes and rejects overrides, caller pre-renders, and grammar transformations.
Exact replay-only qualification never publishes lossy KV or authorizes serving.
The strict coordinator is separately exercised with controlled fitting data.

`runtime/kv/compaction_profile.zig` defines independent artifact, tokenizer,
template, layout and runtime identities, frozen owner/head retention, tested
prompt/output bounds and minimum allocation savings. Identity mismatch is
rejected. The release registry is empty and cannot be populated by an environment
variable, request body or profile file. This is a structural admission contract;
loaded-byte identity capture/sealing and registry-backed native/CLI/HTTP factory
selection are still release work. Fixture identities in coordinator tests are
explicitly synthetic and are not installed-artifact provenance evidence.

Final metrics now refresh retained rows at publication and accumulate branch
status readbacks on maintenance close, including cancellation. The dedicated
replay counter covers append and attention status traffic; it is not a
backend-wide KV-transfer measurement.

Qualification results are recorded below after execution. Public approximate
capabilities remain false. Useful installed-model lossy savings and quality,
optimized attention parity, loaded-byte profile admission, HTTP failure/reload
and mixed-request tests, clean-memory destination performance, and the real
four-hour serving soak remain independent release gates.

The final v17 Metal suite passes **217 of 228 selected tests**, with eleven
optional probes skipped. This includes pure allocation/provenance/profile
checks, allocation-failure injection, stale prepared-geometry admission,
private-replay cancellation, retry, strict all-owner fixture publication, and
publication metrics checked before any generated token. The final arena owns a
stable allocator context; moving the request cannot leave its eligibility or
replay records pointing at a former stack allocator. Eligibility validation
requires ranges to agree with labels and blocks to be exactly the complete
512-token intervals derived from those ranges.

Both installed Q4_0 artifacts independently pass the native coordinator test
with Metal API validation. Each request uses **1205 rendered prompt tokens**.
A baseline owned exact generation and a generation with private shadow replay
produce the same four raw continuation token IDs and the same returned text.
The live epoch remains 1, no lossy block is published, query pools release,
and the decode hook is restored. A following identity-mismatch request fails
before device owner creation and clears the preceding request's telemetry.
The coordinator's runner uses actual model layers with hidden-only execution;
these tests do not claim instruction-retention quality for a compacted model.

| Artifact | Admitted context peak | Replay status readback | Continuation |
| --- | ---: | ---: | --- |
| E2B Q4_0 | 69,189,632 B | 3,116 B | Four token IDs and returned text identical |
| E4B Q4_0 | 200,572,928 B | 3,800 B | Four token IDs and returned text identical |

Admitted context peaks are conservative lifecycle budgets, including prepared
metadata and fitter reservations. They are neither process RSS nor actual
compression savings. Replay status readback includes append and attention
status scalars, not KV tensor readback.

Both artifacts also repeat all eight native generation routes (legacy borrowed
and owned packed greedy, sampled, grammar-constrained and early-stop streaming)
and the four following zero-budget rejections. Every owned route restores its
hook and reports current request metrics; each rejection clears telemetry.

| Artifact / probe | Wall seconds | Peak process-group RSS MiB | Minimum free memory | System swapout growth |
| --- | ---: | ---: | ---: | ---: |
| E2B native coordinator | 59.70 | 672.63 | 41% | 0 |
| E2B native generation routes | 16.50 | 422.55 | 44% | 0 |
| E4B native coordinator | 111.21 | 542.09 | 27% | 0 |
| E4B native generation routes | 24.49 | 461.00 | 28% | 0 |

These are complete qualification-process durations, including loading and
multiple requests. They cannot be interpreted as replay latency, TTFT,
throughput, or a speedup over v16. Existing swap remains allocated. Model,
manifest, test binary and 57 changed Zig-workspace file hashes are checked
before and after the campaign. Receipts are under
`/private/tmp/antfly-context-v17-model-lifecycle-final/`.

The compression-effectiveness gate remains open: the latest v14
installed-artifact numerical profiles retain 504–512 rows per block and save
only a projected 32 KiB per model in their tested allocation case. V17 does not
replace that historical result with a stronger compression claim. Useful
committed savings and generated-answer quality must precede a production
performance claim or enabling the release registry.

The final CPU suite passes **188 of 228 selected tests**, with forty optional
or Metal-only probes skipped. All **42 Python** calibration, capture, fitter and
release-auditor tests pass. Metal and CPU receipts bind the same final set of
57 changed Zig-workspace files; the CPU binary has no Metal framework linkage.
Formatting and whitespace checks pass. The source/evidence checkpoint is
`/private/tmp/antfly-context-v17-source.zip`, indexed by
`/private/tmp/antfly-context-v17-validation.json`. Its incremental patch is
applied to the preserved v16 checkpoint and every resulting file hash checked.
No commits, pushes, CI dispatches, or remote operations were made.

## Performance and qualification hardening (v18)

The accepted qualification order is exact 8K, then exact 32K, on the local
16 GiB M4. E2B and E4B qualify independently. A qualified exact route is a useful
milestone even if attention matching remains unavailable. A context limit means
prompt plus reserved output; an 8K or 32K prompt followed by 512 output tokens
is a different, explicitly labelled workload.

Implementation changes in this checkpoint:

- `PreparedPlan` owns immutable payload, token and replay inputs before chunk
  selection. Its chunk-dependent estimate separates host and backend peaks.
  Adaptive admission can retry smaller chunks; finalization transfers ownership
  once and rejects zero or insufficient budgets in either tier. The combined
  execution ceiling remains separate for runtime allocation checks. The native
  qualification path now uses this two-stage API.
- Loaded provenance hashes the retained GGUF mapping, exact parsed tokenizer
  bytes, effective BOS policy, and successfully parsed template plus its special
  token strings. A load-generation token tracks ownership separately. Unsupported
  composite or converted artifact stores remain ineligible for compaction. The
  recognized Gemma4 audio-projector wrapper exposes its retained primary decoder
  mapping for text-context provenance without reopening the artifact pathname.
  Layout and implementation identities use canonical, versioned encodings.
  The implementation fingerprint binds the relevant sources, Metal compilation
  policy, optimization mode and ABI; release registry contents are excluded to
  avoid circular promotion identities.
  The final manifest hashes the complete Metal runtime translation unit and
  Zig FFI surface. Hashing only pipeline creation left dispatch and frame edits
  outside runtime profile admission. Mutation checks against the actual host
  generator prove that C dispatch and Zig FFI changes now invalidate identity,
  while a registry-only change preserves it. This deliberately invalidates
  profiles for unrelated edits in those shared files as well.
- Offline calibration includes the pinned upstream single-key OMP selection
  and least-squares refitting recipe, distinct from the existing batched pursuit.
  Reports preserve rank, conditioning, objective residuals and storage conversion
  error. Fit count, estimated working set and time are bounded. This recipe has
  not been ported to Metal or authorized for serving.
- Native timing separates post-ingestion maintenance from decode and records
  the first nonempty streaming callback when one exists. Nonstreaming runs retain
  a null callback timestamp. Installed-artifact probes report actual rendered
  token counts, raw-token output hashes, loaded identity and allocation counters;
  unavailable counters remain null. Repeated filler probes measure capacity,
  not retrieval or answer quality.
- Exact and approximate batch requests are partitioned before materialization.
  Exact siblings retain their ordinary batching path; approximate windows are
  singletons. Shared and exclusive execution barriers run after memory admission
  and before the model lock. Atomic shared-group admission avoids deadlock when
  an exclusive waiter is interleaved between batch lease acquisitions. HTTP
  barrier waits check cancellation without retaining the coordinator mutex.
- The exact-context collector writes bounded logs and preserves partial failure
  receipts. It verifies the executable against a build receipt, rebinds every
  raw loaded identity, and rejects duplicate, incomplete or unscheduled pairs.
  Collection alone never promotes an artifact.

Exact-context auditing also binds the collected executable and build-receipt
hashes to the requested provenance. Incomplete collections, reclassified or
reused warmup pairs, partial output reservations, and warmup output divergence
cannot qualify a context length.

The release contract remains: at most 5% exact-path regression; AM p95 TTFT at
most 1.25 times exact and decode throughput at least 0.90 times exact; at least
25% actual committed total context-KV savings including metadata and continuation
reservation. Numerical gates remain 5% maximum mass error and 10% per-consumer
output relative RMSE after f16 conversion. Quality regression limits remain
2 percentage points overall, 5 per bucket and 5% relative perplexity. Performance
requires matched, clean-memory samples, and every released artifact requires a
four-hour serving soak.

The 25% savings threshold is a workload-level release gate, not a per-request
promise. Runtime admission currently enforces the profile's absolute reduction
in global compacted storage and device bytes. A smaller selected payload can
meet that floor while saving less than 25% of total context memory. Neither
the lifecycle peak nor the global-byte delta should be reported as a total
context savings percentage.

The public preflight owner remains a release dependency: it must select a
registry profile from the pinned loaded model, render/tokenize once, preserve
payload provenance, reserve both tiers using the chosen chunk, and pass the
same finalized request into packed ingestion/replay/publication. The internal
qualification hook is not a production bypass. No capability flag may be
enabled merely because the low-level allocator or fixture tests pass.

The first v18 OMP diagnostic reuses the sealed v14 E2B calibration capture for
global owner 9, head 0. It does not replace the capture's historical source
identity or count as an independent model-quality evaluation. Training queries
select and fit the keys; the table reports the worst held-out consumer errors
after storage conversion:

| Retained rows out of 512 | Maximum relative mass error | Worst output relative RMSE | Accepted |
| --- | ---: | ---: | --- |
| 128 | 142.82% | 34.88% | No |
| 256 | 103.99% | 22.35% | No |
| 384 | 36.53% | 7.89% | No |
| 448 | 37.03% | 5.56% | No |

This is evidence against porting this recipe/configuration as the next release
candidate. Larger retained counts improve output error but do not satisfy the
5% mass gate on this capture. No threshold was relaxed. The four fits completed
under a 1 GiB process-group guard with 377.8 MiB peak RSS and no new swap;
these offline fit timings are not serving latency. Receipts are
`/private/tmp/antfly-context-v18-omp-e2b-owner9.json` and
`/private/tmp/antfly-context-v18-omp-e2b-owner9-resources.json`.

Adversarial release-auditor review found that raw performance/identity checks
alone cannot authenticate summarized numerical, fault-injection and soak
reports. Schema 2 therefore remains diagnostic with `evidence_passed: false`
and names its missing raw-collector gates. Incomplete or duplicate performance
pairs, mismatched context cells/settings, unverifiable artifact/build files,
imbalanced quality cells and forged soak summaries are rejected or explicitly
remain unqualified. No runtime authority comes from these JSON reports.

Schema 2 now has a bounded raw numerical audit for the supported Metal recipe.
It rehashes sealed tensors, recomputes the FP64 fit, reads the actual Metal
f16/f32 output and report bytes, and derives consumer errors again. Resource
success is checked from measured guard fields. Capture count, input bytes,
metadata, fit count and fit work have explicit limits; only one capture's arrays
are retained at a time. Supplying summaries alone leaves the raw numerical gate
open. OMP cannot satisfy this gate until it has a corresponding Metal recipe.
Raw failure-injection, schema-2 soak and loaded-profile provenance remain open.

Performance auditing compares AM TTFT and decode directly with ordinary exact.
Packed exact must also preserve at least `1 / 1.05` of ordinary exact's decode
throughput. Paired ratios are bootstrapped directly. Using a slow packed baseline
as the AM latency denominator would incorrectly admit a regression; that case
now has an adversarial test. Committed memory savings still compare the same
packed context immediately before and after compaction.

The resumed numerical review found no scaling or storage-conversion defect in
the OMP diagnostic. At 448 retained rows, the mass objective has only 0.00512
relative L2 residual, yet its worst held-out relative error is 0.3703. Several
keys that dominate held-out attention are rare in the training queries and
remain unselected. The solve is full rank with moderate conditioning, and the
measured bias/f16 conversion errors are much smaller than the failed gate.
This FP64 structural port is an oracle diagnostic, not bitwise upstream Torch
conformance; the upstream source-dtype matmul and FP32 solver are different.
Held-out queries must not be used to select the missing keys for a release fit.

Ordinary exact allocation accounting is still incomplete. Its sequence remains
live after generation, but only packed sessions currently populate the context
metrics snapshot. Completing the baseline requires measured sequence-owned
backend slot capacities, reserved tail blocks, host/control allocations, and
SWA ring capacity. Runtime-global page pools must be attributed once. Admission
estimates or all-tensor Metal allocation counters cannot substitute for this
receipt. Compare fresh processes with identical artifacts, prompts and complete
output reservations, preserving raw output hashes and explicit route identity.

The first E2B 8K pair uses 8,160 rendered prompt tokens, 32 generated output
tokens and 64-token ingestion chunks. Both paths produced the same raw-token
SHA-256 and verified the loaded decoder/tokenizer/template identity. Both had
zero new swap and no backend KV readback was reported by the packed path:

| Native pipeline storage | Prefill | Decode | Total generation | Reported packed context bytes |
| --- | ---: | ---: | ---: | ---: |
| Ordinary exact | 115.766 s | 0.707 s | 116.474 s | Unavailable |
| Packed exact, serial reference | 204.128 s | 24.067 s | 228.218 s | 57,688,064 |

This pair fails the intended performance budget and motivated a qualification-only
split-row decode candidate. Its partial softmax work is parallelized over at most
32 splits, then merged stably in FP32. Scratch is allocated before inference and
charged to the backend tier, live bytes and peak bytes. Multi-query prefill and
all public serving defaults retain their existing routes. The probe reports the
selected kernel and successful split-dispatch count; a flag alone is not route
evidence. The candidate does not solve the remaining prefill bottleneck.

Qualification preparation and live session creation use the same split-decode
configuration. This is necessary because the immutable request's backend
reservation and replay ceiling include the preallocated scratch. A mismatched
request/session kernel configuration is rejected before consuming the request
or starting replay; a regression checks that the runner and publication counters
remain untouched. Split decode remains an explicit test-only pipeline option.

The build-9 same-binary E2B 8K follow-up produced the same complete 32-token
output hash on both packed routes:

| Packed decode route | Prefill | Decode | Total generation | Live/peak context bytes | Split dispatches |
| --- | ---: | ---: | ---: | ---: | ---: |
| Serial reference | 226.420 s | 24.545 s | 250.989 s | 57,688,064 | 0 |
| Split candidate | 234.327 s | 16.232 s | 250.587 s | 58,228,736 | 1,085 |

Decode latency fell 33.9% (1.51 times throughput), with exactly 540,672 bytes
(528 KiB) of charged scratch. Both runs had zero new swap and zero reported KV
readback. Prefill variation erased the total-latency gain; the split path still
does not meet the ordinary-exact regression budget. This single, fixed-order
filler pair with API validation and historical system swap is diagnostic only.
The comparison receipt is
`/private/tmp/antfly-context-v18-e2b-8k-decode-comparison.json`. These model runs
precede the final qualification-plan scratch forwarding/compatibility fix; their
exact probes do not enable qualification, so that omitted combined path was not
exercised. Final-source tests separately cover the corrected guard.

Separate ordinary CLI capacity diagnostics completed on the local M4:

| Artifact | Prompt + output positions | Prefill | Decode tokens/s |
| --- | ---: | ---: | ---: |
| E2B | 8,147 + 32 | 50.561 s | 42.84 |
| E4B | 8,160 + 32 | 71.305 s | 20.45 |
| E2B | 32,736 + 32 | 348.274 s | 23.92 |
| E4B | 32,736 + 32 | 291.211 s | 15.44 |

These use the ordinary whole-model CLI route, not the eager packed comparison
above. They are single repeated-filler capacity observations with Metal API
validation enabled. Historical system swap remained allocated, although each
model run had zero new swap. They do not establish quality, clean-memory p95
performance, HTTP serving qualification, or raw-token parity. The first three
receipts bind build 7 (first CLI row) or build 8 (next two rows), before the
split-decode source changes. The E4B 32K observation binds final build 11, sets
the explicit context window to 32,768 and completes the full output reservation.
Its guard records 19% minimum free memory and zero new swap. The verified
artifact/executable receipt is
`/private/tmp/antfly-context-v18-e4b-cli-32k-provenance.json`; ordinary CLI output
still lacks the complete loaded semantic identity and raw-token parity receipt.
The different artifacts/builds/settings are not a matched speed comparison.

The next prefill optimization should consume packed ABI-2 descriptors directly.
The existing generated hd256/hd512 flash kernels are useful arithmetic
references, but their interleaved KV/block-table ABI cannot represent unequal
head spans, compacted log-mass or compacted logical intervals. Repacking the
growing prefix on every ingestion chunk would add copies and a second storage
reservation. A descriptor-aware flash kernel can instead tile eight query rows,
walk each head's spans, and preserve GQA, softcap-before-log-mass and exact/SWA
causal masks while maintaining FP32 online softmax state. It needs an explicit
threadgroup dispatch interface; the current generic 1-D dispatch is insufficient.

Keep that future route opt-in until independent FP64 tests cover both head
dimensions, query tails, wrapped spans, unequal head lengths, compacted bias,
softcap, GQA 8:1/8:2 and nonfinite inputs. Installed E2B and E4B checks must prove
dispatch, complete output parity, charged allocations and bounded resources.
Kernel speedups are an engineering milestone; the unchanged 5% ordinary-exact
regression limit and matched campaign requirements remain the release gates.

Decode synchronization must be measured before further split-count tuning.
The current hook flushes the main runtime before standalone context attention;
each split stage then submits and waits independently. Count and time the main
flush, context dispatch/wait, descriptor upload and KV append separately, with
GPU frame timings where available. Dividing total model decode time by the
dispatch count does not establish the cost of an individual GPU stage.

A simple `beginBatch` around the two context stages is unsafe in the current
live hook. Flushing the main runtime immediately opens a new empty frame and
sets its thread-local encoder owner. Beginning a frame on the independent
context runtime replaces that owner, while submitting/cancelling the context
frame clears it without restoring the still-active main runtime. The narrowest
follow-up is an owned two-dispatch entrypoint that validates both calls, encodes
two ordered stages in one standalone command buffer, and commits/waits once
without entering the frame/TLS API. It must discard any uncommitted partial pair
on error and drain every committed pair before returning. Broader alternatives
are explicit runtime suspension/restoration or shared frame ownership. Regressions
must cover an invalid second dispatch, failure/cancellation, status reset and an
active main frame whose subsequent compute/blit transitions and counters remain
intact. The v18 candidate does not introduce nested-runtime batching.

Final-source validation uses the complete runtime/FFI manifest:

| Validation | Result | Resource evidence |
| --- | --- | --- |
| Metal ReleaseFast context suite, API validation enabled | 229 passed, 13 optional tests skipped | 474.37 s, 5,119.88 MiB peak group RSS, 35% minimum free memory, zero new swap |
| CPU-only ReleaseFast context suite | 199 passed, 43 skipped | 359.96 s, 4,440.42 MiB peak group RSS, 49% minimum free memory, zero new swap |
| Python calibration/collection/release tests | 84 passed | `/private/tmp/antfly-context-v18-python-final2.log` |
| Runtime identity mutation checks | C dispatch and Zig FFI edits invalidate identity; registry-only edit does not | `/private/tmp/antfly-context-v18-identity-mutation.json` |
| Formatting/whitespace | 52 changed Zig files pass; nothing staged | `/private/tmp/antfly-context-v18-final-static.json` |

The final build receipts are
`/private/tmp/antfly-context-v18-metal-build11-receipt.json` and
`/private/tmp/antfly-context-v18-cpu-build2-receipt.json`. They bind the executable,
source hashes, Git head, compiler options, test log and resource report. The
earlier CPU build 1 was deliberately stopped when the manifest gap was found;
its negative exit is retained with an intentional-stop note, not reported as a
compiler failure. Final checks do not promote the earlier single-pair model
observations into a representative quality or performance campaign.

The uncommitted v18 checkpoint is
`/private/tmp/antfly-context-v18-source.zip`, with validation and archive receipts
alongside it. Its incremental patch is applied to the preserved v17 snapshot
and every resulting current-file hash is verified. The archive includes the
raw diagnostic evidence and distinguishes superseded attempts from final
validation. No commit, push, remote CI dispatch or release enablement occurred.

### V19 qualification work in progress (2026-10-18)

Production authorization remains disabled. The release registry is empty, and
none of the new paths below establishes Gemma4 numerical, performance, serving,
quality or soak qualification by itself. The accepted numerical tolerances,
25% total committed allocation reduction, matched performance requirements and
four-hour per-artifact soak have not been relaxed.

The ABI-2 tiled prefill candidate now consumes packed spans directly at head
widths 256 and 512. It preserves per-head physical gaps, GQA, compacted logical
intervals, mass bias after softcap and causal/SWA masking. A dedicated pipeline
and threadgroup dispatch API validates bounds and retains descriptor ownership.
A standalone two-stage dispatch also submits split attention in one command
buffer without replacing another runtime's active encoder owner.

The actual Zig/Metal flash test in build 9 passed 108 finite cases against an
independent FP64 oracle and eight nonfinite cases under Metal API validation.
It covers both widths, GQA 8:1/8:2, nine query lengths, exact physical gaps,
compacted spans and wrapped SWA spans. The resource receipt records 13.589 s,
563.39 MiB peak group RSS, 69% minimum free memory and zero new swap. Its bound
receipt is `/private/tmp/antfly-context-v19-flash-build9-receipt.json`.
Protected-prefix fixtures have subsequently passed in the source-bound build 14:
144 finite and eight nonfinite flash cases. These kernel fixtures do not attest
installed-model output parity.
The preceding full context build compiled successfully and ran 234 passes,
14 optional skips and one failing flash fixture. The fixture had an invalid
compacted interval and inherited an unrelated prefix default; both were fixed
before the successful focused run. That earlier full run is not a passing
final-source suite.

Shared-frame infrastructure now owns immutable descriptor arenas and one
completion callback for a complete context step. The callback validates status
before host token visibility and rolls back pending metadata/growth on failure.
The generic prefill and active greedy decoder now route context attention and
shared-KV consumer layers through the whole-model frame. Device Q/K/V/output
references remain retained through completion. New tests cover full framed
commit, cancellation, partial-frame rejection and safe release; the installed
exact probe requires positive framed counters and zero standalone attention.
Build 14 passed the full context suite, but installed-model execution exposed
a remaining decode-routing defect: the E2B packed-flash probe recorded 1,085
standalone attention dispatches (35 layers times 31 decode steps), despite
positive framed-prefill dispatches. The strict route assertion rejected the
run. No performance claim is made from the source changes.

The next numerical campaign separates original request-question prefill,
private question replay, fixed-study prompt prefill and generated-response
queries. Fit and validation studies have separate immutable templates and
partitions. Generated responses use fixed-length greedy decoding with EOS
ignored; their generation contract is hashed. Runtime source descriptors bind
positions, token bytes, owner/head geometry, timing and allocation observations.
Schema-2 assembly verifies that prefill and generated-response sources form
complete study pairs. Teacher continuation queries remain excluded from
calibration and profile selection. A build-11 E2B original-question prefill capture passed: 4,445 prefix tokens
plus 59 question tokens, 151.962 s total, 807.41 MiB peak RSS and zero new swap.
That older binary did not emit the subsequently required runtime-loaded identity,
so its data remains diagnostic. The study replay then found a correctness defect:
for a 57-token suffix, conservative quartet sizing produced 448 sampled rows,
while the exact hashed partition produced 456. A calibration-only Bounds fix
and odd-length regression now align the two replay paths; the conservative
production sizing branch remains unchanged. Fresh model captures are required.
All eligible full blocks can now be captured with explicit owner/block coverage;
full-context KV is emitted only once in that mode.

The CPQR diagnostic does not justify a release: on the prior E2B owner-4
capture, retaining 352 of 512 rows yielded about 28.48% training mass error,
99.31% held-out mass error and 23.86% aggregate held-out output error. A 448-row
attempt also failed. Those results motivated representative query capture,
not weaker acceptance thresholds. Near-identity retention schedules from prior
work also do not meet the required total allocation savings.

Serving preparation now pins the loaded generation and profile, preserves the
immutable rendered/tokenized prompt owner, and records separate finalized host
and backend reservations. Test-only manager-scoped candidate profiles remain
unavailable in production builds or through HTTP/environment overrides. The
request records actual before/after allocation and publication telemetry. A
Node observation sink runs after request/admission owners release; streaming
completion requires terminal SSE emission. A persistent driver exercising real
native, CLI, HTTP, SSE and mixed-batch entrypoints is still required.

The Python release checks now reconstruct soak and fault evidence from bounded
raw stdout and parent-clock journals. Fault reports require original-context
fingerprints and matching raw observer/exact token streams. Exact-context
collection additionally records the bound run plan, original executable output,
resource samples, command/exit order and successful descendant cleanup. The
release auditor recomputes those observations instead of trusting receipt pass
bits. Exact qualification requires independent document hashes, balanced pair
order, full reserved output, raw-token digest equality, paired elapsed-time
bounds and a separate decode throughput bound. Repeated filler remains only a
capacity diagnostic. The expanded Python calibration, capture, Metal-oracle, exact, release, soak,
failure, paired-performance and quality suites passed 134 tests in 48.674 s.
The log is `/private/tmp/antfly-context-v19-python-checkpoint3.log`. These include
synthetic collector fixtures and are not model-quality or serving evidence.

The subsequent question-only diagnostic also rejected every 352-row candidate.
For owners 4/9/14, the supported RMS recipe's held-out maximum mass errors were
147.4% / 21.6% / 22.5%; pursuit gave 22.9% / 53.2% / 6.7%. These are isolated
within-question diagnostics, not sealed independent qualification. Retaining
352 rows is itself optimistic for total savings after the protected recent tail
and continuation are counted. They justify broader representative query capture,
not selecting a different recipe from evaluation results.

Release audit hardening now binds raw numerical evaluations to their frozen
profile artifact and rejects recipes not implemented by serving maintenance.
Metal qualification exports only fit/runtime-validation queries to the fitter;
independent teacher queries evaluate the frozen stored candidate afterward with
separate aggregate and per-consumer gates. Persistent collector shutdown must
also prove no live descendant process remains. Memory savings must beat both
ordinary exact and packed exact at the unchanged 25% threshold; an inflated
packed baseline cannot hide a regression against ordinary exact.

Total allocation telemetry remains an open gate. Ordinary exact now observes
owner-leased MTLBuffer allocated sizes and explicit KV readback, but its host
metadata is not yet measured. Packed metrics include conservative page charges
for some host/control allocations. These fields must be separated from exact
measurements and unified under an owner-scoped accounting contract before a
production savings claim. Device-only bytes are not emitted as measured total
context bytes.

Build 12 ran the complete Metal context suite with API validation: 243 passed,
14 optional model tests skipped, 491.442 s, 5,036.80 MiB peak RSS, 34% minimum
free memory, and 303.81 MiB new swap within the build-only 512 MiB cap. One
buffer-retention edit landed after its source snapshot, so this is compiler/test
signal, not an exact-current-source receipt. The discrepancy is explicitly
recorded in `/private/tmp/antfly-context-v19-metal-build12-source-drift.json`.
Build 13 was deliberately terminated after review found that generic GPT
framing also selected legacy synchronous hooks; it is not a compiler failure.
Capability-aware prefill frame selection, synchronous-hook fallback, and active
decode consumer-layer indexing are implemented with a regression. The separate
active-greedy legacy-hook capability guard remains under review.

Build 14 passed 244 tests with 14 optional model skips in 488.552 s under Metal
API validation: 5,050.80 MiB peak RSS, 34% minimum free memory and zero new swap.
All 64 dirty or new compiled-source files matched the build snapshot. Receipt:
`/private/tmp/antfly-context-v19-metal-build14-receipt.json`; binary SHA-256:
`3e74dc5f4f8075acba5d7b176614e4061530da013ad1e40caa1678e7561f74c8`.
Its ordinary E2B 8K capacity diagnostic (8,160 prompt plus 32 output tokens)
passed, with 135.618 s prefill, 0.753 s decode, 77,266,944 actual leased device
KV bytes and zero backend KV readback. The output hash matches the earlier
ordinary baseline. Total guarded process time was 139.933 s, peak RSS
4,370.34 MiB, minimum free memory 42%, and new swap zero. This is one cold,
repeated-token capacity run, not a matched independent-document release result.
The packed-flash run failed its route assertion after 154.562 s with zero new
swap; phase timing and output were not emitted before that assertion, so they
are unavailable for comparison.

Raw numerical audit now reopens the frozen profile artifact and requires its
fixed retention, recipe and capture contract for every head of every capture.
A profile hash alone cannot authorize a substituted retention or extra retry.
It also matches capture layout/runtime identity and reconstructs the canonical
Metal fitter-input hash from sealed fit/validation tensors. Teacher continuation
queries remain outside those fitter bytes. The runtime block-coverage contract
now reconstructs eligible ranges and every 512-row block from a full token-label
vector, binds prompt token bytes, loaded generation and binary/build identity,
and checks the exact document/owner/block Cartesian product at every claimed
context length. Legacy captures remain readable only with an explicit missing
runtime-coverage gate. These checks passed 35 focused Python tests; actual
runtime coverage receipts and protocol equivalence remain required for release.

The source-bound build-14 question capture passed with all 24 owner/block
captures, zero new swap, and a 2.566 s question-prefill phase. Its odd-length
private question replay also passed the previous sampling assertion, before
the following study failed with `InvalidContextView`: sliding origins borrowed
baseline spans that the parity loop had already advanced. Study capture now
runs against the original baseline before that mutation, with global and
sliding seed fingerprints checked after every private branch. This fix awaits
the next successful build and installed-model rerun.

An all-block RMS diagnostic retained 256, 320, 352 or 512 rows on those 24
question-only captures. Every lossy candidate failed the unchanged numerical
gates; all 24 exact controls passed. The bounded run took 15.420 s, peaked at
388.88 MiB RSS and added no swap. Receipt:
`/private/tmp/antfly-context-v19-question-all-block-fit3.json`. This is calibration
diagnosis without study or independent teacher queries. The broader offline
schema-2 query envelope also exceeds the current 4,096-row Metal fitter limit
for some consumers; the exporter now rejects that mismatch explicitly. A
shared, predeclared bounded source-sampling protocol is still needed.

Raw quality collection now seals independent document/prompt/reference bytes,
requires each prompt to contain its declared document, and records exact and
attention-matching variants under one loaded generation. Accuracy is recomputed
by the declared UTF-8 NFC/trim exact scorer; perplexity is recomputed from
per-token teacher-forced log probabilities. Parent journals own resource and
wall-clock evidence. Neither child pass flags nor summary NLL values qualify
quality. Actual teacher-forced loaded-model execution remains required; the
contract and its mock driver tests do not supply that evidence.

The expanded Python checkpoint passed 150 tests in 48.936 s
(`/private/tmp/antfly-context-v19-python-checkpoint4.log`). Build 15 exposed a
removed Zig testing helper; build 16 then exposed the new serving driver's
mutex API and enum conversion errors. Those compiler errors were fixed before
build 17. The driver APIs now receive body analysis rather than only parsing.
Build 17 was stopped by the resource guard after 525.280 s: 3,915.28 MiB peak
RSS, 34% minimum free memory and 533.69 MiB new swap exceeded its 512 MiB
build-only cap. It emitted no compiler diagnostic; this is an environment-limit
result, not a successful build or a source failure. Build 18 compiles only the
installed-model probes under the same limits to reduce compilation scope. Full
serving-driver compilation remains required.

The next owner-accounting patch remains outside the compiled checkpoint. Its
single ledger measures host malloc usable bytes and no-copy Metal backing on
one timeline, including realloc overlap. Review caught a deferred access after
buffer teardown and a missing allocation budget; both were corrected in the
draft. The ledger deliberately does not claim a complete total while request
contexts also create unmeasured Metal runtime/queue/pipeline objects. Moving
reusable execution objects under a generation-pinned, bounded model-owned pool
is planned, with shared residency disclosed separately and mutable request
buffers kept isolated. Device-only or partially tracked bytes cannot satisfy
the committed-context-savings gate.

Two release integration gaps also remain explicit: production currently cannot
select the optimized implementation used by test candidates, and the runtime
candidate JSON differs from the calibrator's frozen profile artifact. The
registry must select the actual qualified implementation, and one exact frozen
artifact must bind both offline choices and the runtime owner/head retention
table. An unrelated conversion file or a test-only route cannot qualify the
production implementation.

Build 18 completed the focused installed-model test build: 10 passed and 11
optional installed-model tests skipped in 416.807 s, with 5,007.52 MiB peak
RSS, 43% minimum free memory and zero new swap. All 66 modified or new compiled
sources matched the snapshot. Binary SHA-256:
`8772df1572be270a56cd512b07909992e6383f8bb827e19efe9e40d200c08120`.
Its source archive is independently hashed and verified before and after model
probes, permitting subsequent edits without attributing those edits to the
older binary. This focused build does not compile the complete serving driver.

The build-18 packed-flash 8K diagnostic reached 5,565 framed attention
dispatches and zero standalone dispatches, including 1,085 split-decode and
896 flash-prefill dispatches. It nevertheless failed allocator leak checking:
62 allocations of 600 bytes and 62 of 16 bytes remained (38,192 bytes total).
Packed output also diverged from same-build ordinary exact at the second token.
The ordinary run passed with 141.573 s prefill and 0.736 s decode; packed
measured 105.823 s prefill and 2.441 s decode. Both added zero swap. These are
single repeated-token diagnostics, with correctness and ownership blockers;
they do not establish a speedup or pass the exact-context performance gate.

The next source checkpoint adds a shared bounded five-source query protocol,
two physical study branches, an internal optimized production selector, and a
single frozen artifact containing both offline choices and runtime retention.
These edits are newer than build 18. The release auditor now independently
compares that runtime projection against the frozen owner/head choices,
loaded identities and owner-specific query protocols, while permitting shorter
evaluation requests within the unchanged frozen limits. The focused raw
evidence and block-coverage suites passed 29 tests in 13.625 s. Full request
allocation accounting, serving protocol coverage, numerical acceptance and
destination release campaigns remain open.

The sealed build-18 broader study capture now passes after moving it before
baseline mutation: all 60 replay steps match exactly, maximum logit and query
errors are zero, and the original context and branch leases remain intact.
The run took 485.919 s, peaked at 3,919.91 MiB RSS and added zero swap
(`/private/tmp/antfly-context-v19-e2b-study3.json`). These are four-branch,
manually framed diagnostic captures; they do not prove the newer bounded
five-source protocol or serving-rendered payload provenance.

A 128-token prompt plus four output tokens reproduces the packed mismatch and
six leaked hidden-state wrappers. Disabling both the active decode frame and
direct greedy decode restores ordinary output and eliminates those leaks;
that control intentionally fails the packed route assertion because it uses
105 standalone decode dispatches. This isolates the defect to framed decoding
without establishing the precise leaked owner. The next build preserves
planned barriers in the generic packed decode path and includes opt-in
reserved-hidden-state ownership tracing. Both fixes still require actual
model verification. The latest full Python checkpoint passes 156 tests in
50.634 s (`/private/tmp/antfly-context-v19-python-checkpoint6.log`).

Build 23 passed the complete default context selection: 263 passed, 14 skipped,
277 selected in 499.815 s, with 5,046.16 MiB peak RSS, 37% minimum free memory,
and zero new swap. The sealed binary is
`f5b697a9173b543a6374a0a8779e3600c99bb76ea98660b192d5573a53cd9c85`;
its 75-source archive is
`f577e842f543f0316bfac942d626054ef399eba3cc4e25837b7d5a3ff7049aad`.
This selection still omitted the two explicitly named persistent serving tests;
the default build filter now includes them, so the next successful build must
analyze the actual driver body as well as the core modules.

The real E2B 128+4 build-23 packed probe releases every reserved hidden-state
wrapper and passes allocator checking. The leak was in the direct greedy
caller's success cleanup for the input and returned final hidden tensor.
Packed output nevertheless remains `[818,3847,1816,563]`, while same-binary
ordinary output is `[818,3847,5577,1816]`. Both added zero swap. Disabling only
active paged decode or additionally disabling the reserved hidden carrier does
not restore parity. The canonical gated reference restores the expected first
three tokens and leaks nothing, but intentionally fails the zero-standalone
dispatch assertion. Mid-frame tensor-dump and GPT-stats diagnostics fail with
`MetalFrameSyncFailed`; their partial data cannot establish per-layer parity.
A dedicated decode-only frame-disable diagnostic is included in the next build.

The assembled build-18 broader-study diagnostic covers all 24 owner/block
captures. The assembler now permits different per-stratum quotas across owners
with different consumer counts while still enforcing each physical branch's
own geometry and run identity. The 96 RMS fits at 256/320/352/512 retained rows
took 42.118 s and 738.41 MiB RSS with zero new swap. Every lossy candidate fails;
all 24 exact controls pass. At 352 rows all aggregate output errors meet 10%,
but every candidate fails the 5% worst-query mass gate.

The predeclared matching-pursuit diagnostic at 320/352/384/448 rows took
134.195 s and 769.45 MiB RSS with zero new swap. It improves training mass fit,
but accepts no candidate at 320, 352, or 384 rows. At 448 rows only 4 of 24
pass; held-out mass-error median/max are 9.13%/36.04%. The useful memory-savings
frontier remains unqualified. These older six-source captures exceed the new
bounded runtime protocol and lack serving-rendered coverage, so even passing
individual diagnostic candidates cannot authorize production retention.

The next checkpoint adds explicit schema-2 query authority: ordered source and
physical-branch rules, sampling/accounting versions and caps, actual owner and
consumer-head strata, and fixed study template/generation contracts. Runtime
selection reconstructs the digest from the loaded layout and deeply owns all
strata. The private study runner now appends the final generated token, hashes
fixed-length greedy output, checks cancellation before forwards, and owns its
logits across failure paths. It remains separate from serving callbacks,
grammar, sampler and usage. Actual three-branch coordinator wiring remains open.

Memory ownership now has one coincident host/shared/private ledger, a binding
to the request owner, actual leased Metal allocation sizes including growth
overlap, and separate initial/committed/final snapshots. Packed committed state
is captured after all queues drain and before teardown. Missing committed
evidence stays unavailable. Full-owner availability remains false until the
complete hardware lifecycle and batch accounting are validated.

The profile-free coverage collector binds parent nonce/request index, sealed
manifest and source build receipt, actual loaded identity, and immutable
receipt/token files. Review found that a plain combined question/document
string cannot provide separate document and question attribution; the driver
must instead accept explicitly sealed structured parts before its coverage
can qualify a numerical capture. Build 24 exposed four compile errors while
analyzing the expanded serving bodies; they were corrected for build 25.
No release profile has been registered, and approximate production serving
remains unavailable.

Build 25 hit the unchanged build resource limit after 521.471 s: peak RSS
4,120.28 MiB, minimum free memory 34%, and 512.0625 MiB new swap exceeded the
512 MiB build-only allowance. No compiler diagnostic was emitted. The attempted narrower selection used runtime `--test-filter` arguments, which
did not narrow compilation. Builds 25 and 26 therefore still compiled the
default selection. Compile selection requires repeated `-Dtest-filter=...`
build options. Neither resource-guard stop is a source/test failure or a
passing release gate.


Build 26 also stopped at the unchanged swap guard: 544.335 s, 4,450.22 MiB
peak RSS, 34% minimum free memory, and 557.4375 MiB new swap. It emitted no
compiler diagnostic and produced no qualified artifact. The next build will
separate the exact installed-model probe from the larger persistent-serving
selection; serving and core coverage remain independently required.

The sealed build-23 FFN-isolation probe (canonical FFN, active paged block
disabled) still produces `[818,3847,1816]` at 128+3, with no allocator leak or
new swap. Source review then identified a concrete query-scaling discrepancy:
the direct gated path multiplied Gemma queries by `sqrt(head_dim)` before
attention even when fused head-norm RoPE had already applied it, or unfused
attention RoPE would apply it. The direct path now uses the canonical
single-scaling rule. A regression covers local/global head dimensions and the
host fallback lifetime. This fix awaits a new compiled/model observation;
source review alone does not close exact parity.

Study preparation now extends the original request through the loaded chat
renderer, retokenizes the complete extended prompt, and rejects any change to
the already-ingested token prefix. Compiled fit/gate instructions and a fixed
greedy generation contract are bound into source identity. Embedding ownership
is retained until transfer to the transformer, including scale/PLE failure
paths; 18 fault cases are added but await execution in the next core selection.


The October 19 build-28 checkpoint uses genuine compile-time `-Dtest-filter`
selection. Compilation succeeded in 350.626 s with 4,447.98 MiB peak RSS and
zero new swap. Of 15 selected tests, 13 passed, one skipped, and one failed:
the new study-preparation fixture used a tokenizer without offset support.
The fixture is corrected in source but awaits re-execution. The Q-scaling
regression and 18 embedding scale/PLE ownership fault cases passed without
allocator leaks. The sealed binary and archived source are explicitly marked
**diagnostic-only with one failed test**, not a passing release artifact.

Installed E2B controls from that same build restore exact generated-token
identity at 128+4 and 8160+32. At 8K all 32 tokens match ordinary inference,
with identical prompt/artifact/tokenizer/template/layout identity (implementation
identity correctly differs by kernel route). Packed split-decode/flash-prefill
measured 103,879 ms prefill and 2,380 ms decode; ordinary measured 141,798 ms
and 731 ms. Both runs completed without allocator leaks or new swap. The
single pair is diagnostic, uses a repeated-token capacity workload, and has
historical swap present; it is not a clean-memory matched performance campaign.
Packed decode remains about 3.3 times slower and fails the release requirement.

The next integration replaces production single-reference replay with one
question/fit/gate campaign. Server preflight prepares all sources through the
loaded renderer, binds the actual full prompt and question span, and separates
private loaded-model position capacity from the public context window.
Production `CampaignRequest` and memory admission are separate from legacy
test-only replay. Frame submission failures now propagate instead of returning
apparently valid hidden states. A typed allocation-scope witness binds the
prepared allocator, generation-pinned execution lease, actual Metal storage
hook, drained committed state, and zero-baseline release. Its scope is
request-exclusive context allocation sizes; the warmed execution-pool admission
is disclosed separately. Mixed-batch lifecycle and genuine shared-coordinator
capture integration remain pending compilation and hardware validation.

The next broad Metal selection (build 30) stopped at the unchanged build-only
swap guard after 543.535 s, with 4,284.08 MiB peak RSS and 564.94 MiB new swap.
It emitted no compiler diagnostic and is neither a source failure nor a passing
artifact. The broad `context ` selector instantiated the complete executable
entrypoints; subsequent builds select individual tests at compile time.

CPU builds 31 and 33 are sealed with archived dirty source and binary hashes.
Build 31 passed 21 selected tests with zero new swap. Build 33 passed all 16
selected tests in 27.756 s, using 799.61 MiB peak RSS and zero new swap. Its
coverage includes study preparation and deterministic allocation-failure
cleanup, metadata allocator ownership, exact-sibling and AM-singleton batch
accounting, coverage provenance, and variant settings. The intervening build
32 exposed an allocator-remap-dependent failure fixture; that fixture now
forces allocation fallback so failure indices are deterministic. Build 32 also
overlapped a source edit and is not a qualified artifact.

The pending Metal integration adds framed direct-device packed decode and
borrows pre-reserved Q-width output scratch instead of allocating one output
per layer. Alias checks precede session mutation; a failure after selecting the
packed hook cannot retry through another attention route. Focused regressions
cover owner-frame requirements, cancellation, scratch capacity, and dependent
consume/reuse ordering. Compiler and installed-model evidence are still required.

The numerical capture path now uses the production hidden-only packed prefill
and bounded question/fit/gate coordinator, with fixed 64-row admitted chunks.
It exports every covered exact K/V block from the same packed generation as
the captured queries, after private branches and command queues drain. Artifact
indices bind tensor, query, position, source-token and descriptor hashes to the
renderer-produced request, model identity, executable and source receipt.
Mapped diagnostic K/V export is counted separately from maintenance readbacks.
Coverage also retains the actual rendered prompt bytes; downstream plans do
not reconstruct role delimiters by hand. Admission verifies the actual chunk
size rather than merely reporting the requested value.

The Python release/coverage/serving contract selection passes 60 of 60 tests.
These are validator and fixture results, not installed-model qualification.
Mixed-batch evidence remains explicitly scope-only until each actual item has
an independently observed successful generation joined to its allocation
group. No release profile is registered. Numerical acceptance, measured memory
savings, matched performance, independent quality/perplexity, actual serving
failure recovery, E4B/32K coverage, and the persistent four-hour soak remain open.

The October 19 build-40 selection compiled successfully but is sealed as
diagnostic-only: 26 tests passed, three skipped, and two scratch tests failed
without allocator leaks. Both failures reproduce individually. The next
selection corrects the QKV fixture to submit its production owner frame before
readback and distinguishes persistent direct-output capacity from aggregate
scratch growth when the KV projection width increases. Those corrections still
require hardware execution; they do not retroactively qualify build 40.

A same-binary build-36 E2B capacity diagnostic at 128+32 preserved all 32 output
tokens. Packed prefill/decode measured 2,013/1,859 ms versus ordinary
1,264/713 ms: packed decode remains about 2.61 times slower. Both model runs
had zero new swap, but used a repeated-token workload, historical swap, and a
diagnostic binary. They are not the clean-memory matched release campaign.

The build-40 structured serving discovery stopped before prefill with
`ContextStagingBudgetExceeded`. Review found that exact packed storage received
no session budget and native serving subsequently recomputed an ordinary paged
estimate. Admission now sizes `packed_context_session.allocation()` for each
candidate chunk and propagates the selected component estimate into execution
on native, HTTP, and batch routes. Regressions check chunk reduction, exact
physical charging, workspace denial, and unsupported backend/dtype/policy
combinations. Installed-model confirmation remains pending.

The offline minimax selector is an explicit diagnostic recipe, excluded from
the runtime default. Its training-only worst-stratum selection and bounded
mass solve retain the existing mass/output thresholds; 40 focused calibration
tests pass. This does not establish held-out numerical acceptance or total
context savings. The quality/failure collector selection passes 23 focused
tests. Quality plans reject unsupported CLI/mixed text-scoring routes before
launch, while the separate five-route serving release requirement is unchanged.

The October 19 build-42 selection passed 28 tests, skipped three installed-model
entrypoints, and failed two scratch regressions without allocator leaks. The new
optimized-tail parity fixture and exact-packed admission regression passed. A
matched 128+32 E2B capacity pair preserved every generated token; ordinary
prefill/decode measured 1,300/714 ms and packed measured 2,122/1,716 ms. Both
model runs had zero new swap. This remains diagnostic evidence: historical swap,
one repeated-token pair, and a failed-suite binary preclude release claims.

Installed route counters showed zero optimized-tail dispatches. Static review
then found that `attentionContext` labeled every context session as prefill,
including singleton decode. Framed singleton hooks now retain decode mode, while
legacy synchronous and multi-row hooks retain their prior route. The corrected
routing must prove installed-model output parity and measured performance.

The longer structured serving probe passed admission but aborted during cleanup.
Its crash report identifies the layer-spec cache being freed with the context
tracker instead of its allocating request allocator. The cache now records its
allocator and replaces storage transactionally; serving separately assigns the
context metadata allocator. These fixes await integrated Metal execution.

The QKV scratch failure was a real cancellation bug: packed-weight cache
metadata survived cancellation of the command buffer that initialized it. Cache
entries now track active/submitted/committed publication. Cancellation invalidates
only its pending entries; successful completion publishes them. Regression
coverage also checks that unrelated cancellation preserves completed cached
weights. The oversized scratch fixture now compares against the actual reserved
allocation, since runtime capacity may exceed the last requested width.

Teacher-forcing and fault-controller standalone modules each pass five tests.
The integrated Python release/quality/failure selection passes 94 tests. Genuine
quality scoring and loaded provenance export are still being integrated; these
validator results do not establish model quality. Publication cancellation is
now injected after all fits are staged and immediately before atomic commit.
The next Metal selection includes these ownership, routing, canonical fingerprint,
and failure-contract regressions. No production profile has been enabled.

### Build 43: corrected packed decode and serving cleanup

The source-sealed Metal selection passed 52 tests with three installed-model
tests skipped. The Q8 packed-weight cancellation/reuse, actual scratch capacity,
mixed Q4/Q8 precomputed tail, and layer-spec allocator ownership regressions all
passed. The build used 4,969 MiB peak process-group RSS and 421 MiB new swap;
it is correctness evidence, not clean-memory performance qualification.

Separate E2B 128-token prompt / 32-token continuation diagnostics produced the
same 32 token IDs for packed and ordinary exact storage. Packed decode now
executed 1,085 precomputed tails with no failures, compared with zero before
the attention-mode correction. Packed decode took 1,235 ms versus 764 ms for
ordinary exact; total request time was 3,377 ms versus 2,074 ms. Both model
probes had zero new swap. This short repeated-token probe still fails the
performance budget and does not substitute for independent 8K/32K matched runs.

The structured serving probe no longer aborted in allocator cleanup. It
returned `UnsupportedCompactionStudyTemplate`, producing no qualified capture.
Review traced a boundary-validation defect to the tokenizer's intentional
`(0, 0)` offsets for added special tokens. Study preparation now checks the
complete rendered-byte and token-ID prefix, accepts that sentinel only for
known special suffix tokens, and rejects ordinary zero spans, crossing spans,
and suffixes with no source-backed text. The added-special regression passes
in the CPU selection; installed template confirmation awaits the next Metal
build.

The integrated teacher-forced path uses distinct contexts with identical
published payload, descriptor, loaded generation, prompt, profile, and
implementation identities. Reference IDs stay opaque to fitting and are
revealed only after sibling validation. The driver additionally checks the
sealed reference text against decoding those IDs with the held loaded
tokenizer. The focused CPU selection passes all 36 tests in 30.0 seconds with
826 MiB peak process-group RSS and no new swap. The first CPU attempt caught
and corrected a Zig API rename and missing test initializer fields. Integrated
Metal/model scoring still awaits validation; no quality/perplexity gate or
attention-matching retention profile has passed as a result of these edits.

### Build 44: genuine 8K coverage and decode profiling

The sealed Metal selection passed 62 tests with three installed-model tests
skipped. Loaded provenance, reload ownership, study-template boundaries, and
teacher-forced integration compile and pass the selected regressions. The
integrated Python release suite passed all 105 tests. These results do not
establish model-level quality, failure recovery, or soak qualification.

The installed E2B structured discovery completed successfully. Its 9,391-token
prompt matched independent tokenization of the actual loaded rendering after
disabling an extra automatic BOS insertion. A predeclared source excerpt then
passed strict 8K coverage: 8,160 prompt tokens plus 32 reserved output tokens,
with byte-for-byte agreement for the complete rendered prompt and every token
ID. The successful coverage run took 116.04 seconds, peaked at 3,581 MiB process
group RSS, kept at least 43% free memory, and incurred zero new swap. An earlier
attempt was rejected for a resource-sampling gap; its incomplete receipt is
retained and contributes no qualification evidence.

A matched, instrumented 128+32 exact-storage pair preserved all 32 output IDs.
Both traces contain 33 complete frame records. Median compute encoders per frame
were 1 for ordinary storage and 121 for packed storage; median GPU frame times
were 23.486 and 27.247 ms. Packed decode incurred 406 median planned barriers
per frame. Request decode times were 776 and 1,515 ms. Trace instrumentation,
short repeated-token input, and historical host swap make this diagnostic
profiling, not a release performance campaign. Encoder reuse and dependency
tracking are concrete optimization targets; removing required synchronization
would invalidate the correctness claim.

The worktree now removes a redundant precomputed-tail validation pass using a
frame-bound admission. Runtime, request, tensor view, and frame identity are
rechecked at use, and final C scratch/alias validation remains mandatory. New
regressions cover stale frames, changed shapes/dtypes, and scalar mutation.
This optimization awaits the next Metal build and installed-model parity.

Capture assembly now indexes query strata independently for each KV head. The
previous global indexing rejected genuine E4B GQA geometry and could mislabel
head-local ranges. A two-KV-head regression checks distinct query bytes,
positions, and local training/validation intervals; all 23 capture tests pass.
E2B/E4B numerical, allocation-savings, quality, matched performance, live-route,
and four-hour soak gates remain open. No production profile is registered.

### Replay capture accounting and the next qualification build

The genuine five-source 8K capture on build 44 completed its private replay
work but failed before exporting artifacts after 541.76 seconds. Its resource
guard passed with zero new swap; it supplies no numerical qualification data.
Source review found that first-use shared replay controls remain resident on
the original generations, while the capture baseline was taken before those
controls existed. The worktree now admits that residency before capture and
charges each control once across branch peaks and publication. Strict
before/after capture equality remains required. Specific export errors now
distinguish prompt, query, allocation, branch, and publication failures.

Private replay now has a dedicated framed hook. Per-consumer immutable query
maps and persistent sliding descriptors survive until the decoder frame drains;
completion checks numerical status and commits or rolls back private state.
Legacy hooks retain their existing execution contract. The paired split
attention path also encodes stage and reduction together while preserving
scratch and downstream-output dependencies. These changes still require the
new Metal and installed-model validation; no latency gain is claimed yet.

Coverage exports now bind retained tokenizer byte offsets to the actual prompt,
capture manifest, and block metadata. Release auditing rejects offset
substitution even when the altered manifest is rehashed. The expanded Python
capture/release selection passes 134 tests. The new CLI route runs a distinct
production CLI child against the warm qualification server, and the mixed
route preserves group-scoped ownership receipts. Neither route has completed
installed-model qualification or the four-hour soak.

### CLI output and build evidence hardening

Remote attention-matching CLI requests now validate request features locally and
leave loaded-profile authorization to the server. Local approximate execution
still requires a qualified profile. Non-exact server requests select eager
execution and reject explicit compiled-mode conflicts. The CLI also forwards
the thinking preference without losing an explicit `false` value.

Remote public response text now uses stdout. Reasoning is excluded from that
stream; transport errors and diagnostics retain their stderr path. The
qualification server hashes the exact
successfully emitted public SSE content, and the parent independently checks
the child stdout artifact against those bytes plus one final LF. This hashing
is restricted to tests with an active qualification completion sink. Server
generation timings and child process elapsed time remain separate; this does
not establish end-to-end CLI time to first visible output.

The standalone CLI build audit requires a completed guarded ReleaseFast Metal
build, bound logs and measurements, an executable digest, and the exact sealed
source archive. A binary-hash-only JSON file is no longer accepted as source
build evidence. Twelve focused Python route/provenance tests pass, including
rehashed output, compiler-mode, resource-failure and archived-source changes.
These are contract tests, not installed-model serving evidence.

Two attempts to compile the full CLI entrypoint inside the Metal test selection
were stopped by the unchanged 512 MiB swap-growth guard before tests ran.
Focused Metal compilation is now separate from the required standalone CLI
build. Neither a stopped build nor a focused selection substitutes for the
full CLI release gate. The source preparation checker and diagnostic corpus
constructor pass 19 tests; loaded-tokenizer materialization and real model
quality measurements remain outstanding. No production profile is registered.

The focused build 45f compiled and reached all 91 selected tests with zero new
swap (5,738 MiB peak process-group RSS, 35% minimum free memory). The paired
Metal dispatch, framed model replay, packed lifecycle and coordinator
admission regressions passed. Two new CLI stream fixtures incorrectly used
`finish_reason: null`, which the generated non-nullable API contract rejects;
the server omits that field until completion. The fixtures now match that wire
contract and the corrected selection was stopped before tests by the unchanged swap-growth
guard (514.56 MiB). The failed selection is not a qualifying release build. The expanded integrated Python selection passes all
162 tests in 52.59 seconds.


The original compiled 45f artifact subsequently passed a narrower numerical
capture and Metal lifecycle selection: 71 selected, 68 passed, and three
installed-model tests skipped without their explicit environment opt-in. Its
exact 85-source archive and executable are sealed with `release_eligible:
false`; it cannot satisfy the full release build gate. The loaded E2B coverage
run completed in 114.50 seconds with zero new swap and 3,962 MiB peak group RSS.
Its 8,160 prompt tokens and rendered bytes match the preceding loaded-model
result, and it now exports the complete bound tokenizer offset array. This is
coverage evidence, not a performance or serving qualification.

The quality path now carries ordered system, user, assistant, and tool messages
through native and HTTP requests and the teacher-forced sibling. It binds both
the complete ordered request and each selected source's raw bytes; changing a
question or adding a distractor cannot conceal source reuse across evaluation
cases or overlap with calibration. Legacy document/question inputs remain
supported. These new Zig paths still require a compiled test pass and genuine
loaded-model quality runs.


The genuine 45f E2B five-source campaign completed in 410.48 seconds with zero
new swap, 4,244 MiB peak process-group RSS and 35% minimum free memory. It
exports all 36 global-owner/block captures (owners 4, 9 and 14; twelve eligible
512-token blocks each) from the actual 8,160-token prompt. The live epoch and
58,277,888 committed live bytes are unchanged, and publication was disabled.
The question branch took 1.864 seconds; the fit and gate branches each include
256 generated tokens and took 150.791 and 151.413 seconds respectively. These
instrumented capture costs identify work to optimize; they are not matched
production TTFT measurements. The 36 stored-f16 exact controls pass. The
predeclared 320/352-row campaign stopped after 1,732.79 seconds when a shared
owner shard reached 1,034.98 MiB process-group RSS against its 1 GiB cap, with
zero new swap. It preserved 122 of 144 candidate cells, including 81 complete
three-solver matrices and 41 early numerical rejections. No recorded candidate
passed acceptance. This is an incomplete diagnostic, not a completed numerical
qualification or evidence that every possible recipe fails.

A bounded split global-replay attention candidate is now available only through
explicit test capture options. The serial route remains the default. It admits
one shared scratch buffer per private replay session (2,113,536 physical bytes
for E2B, eight rows and sixteen splits on this host), includes that buffer in
backend and peak accounting, and binds measured branch dispatch counts in the
schema-2 capture receipt. Legacy schema-1 receipts retain absent counters rather
than inventing zero measurements. All 26 Python capture-contract tests pass;
new Metal compilation, serial/split parity and matched timing remain pending.

The page-rounded allocation audit also excludes fixed 320/352-row retention
from the 25% total committed-context savings gate on this 8K request. Even
allowing the three replay-control pages to disappear on publication, projected
post-publication residency is 44,122,112 bytes for 320 rows (24.29% savings) or
46,530,560 bytes for 352 rows (20.16%). The final denominator is the smaller of
ordinary and packed-exact measurements, so these are generous upper bounds.
Retention at most 313 rows is necessary against the packed baseline without
those temporary controls, but is not sufficient numerical or memory evidence.
A separate exploratory 256/288-row centroid recipe is prepared; it cannot
create a runtime profile and would require a new device implementation and a
fresh independent holdout before promotion.

The full context build recorder now archives the entire Git-visible Zig source
tree before compilation, sanitizes runtime/test overrides, and binds the exact
command, Python/Zig executables, resource measurements, logs, compiled test
counts and resulting executable. Scoped capture receipts, binary-only receipts
and all-skipped runs cannot satisfy this gate. Build 46a exposed a checked
integer conversion in the qualification-only decode boundary; that source
error is corrected. Build 46b stopped before tests when system swapouts grew
517.66 MiB, exceeding the unchanged 512 MiB build allowance (192.59 seconds,
2,907.59 MiB peak process-group RSS). It produced no qualifying build receipt.
After the user freed memory, build 46c completed compilation and ran all 411
selected tests: 390 passed, 17 skipped, four failed and zero leaked. It took
511.27 seconds, with 6,701.92 MiB peak group RSS, 37% minimum reported free
memory and zero new swapouts. The four failures still prevent a qualifying
build receipt. Existing swap remains, so this is not clean-memory performance
proof. Two failures exposed real defects: Jinja loop allocation errors could
return a partial prompt, and campaign sizing incorrectly passed a simultaneous
five-source activation mask to a protocol that permits one branch at a time.
Both are corrected; the Jinja suite passes all 69 tests in 10.17 seconds with
433.03 MiB peak RSS and zero new swapouts. The other failures required fixtures
that distinguish release-bound profiles from diagnostics and scratch-capacity
errors from valid-capacity alias errors. Corrected full build 47 stopped before
tests when swapouts grew 563.38 MiB, above the unchanged 512 MiB build cap. It
ran for 416.67 seconds, with 4,636.91 MiB peak process-group RSS and 34% minimum
reported free memory. It produced no qualifying build receipt; the complete
corrected test run remains pending.
The loaded-tokenizer materializer exports exact byte and eligible-token
labels alongside offsets, without model forward work. Its parent uses bounded
nonblocking I/O, per-request and total deadlines, owned process-group cleanup
and fsynced partial evidence. Forty-four focused capture/materializer tests and 24
release-build contract tests pass; loaded-model corpus packing and quality,
performance, serving, failure-injection and four-hour soak gates remain open.

The sealed centroid v5 diagnostic completed all 108 cells in 178.75 seconds:
36 exact controls passed and none of 72 compressed candidates (256/288 rows)
passed the unchanged numerical gates. Peak process-group RSS was 598.06 MiB,
minimum reported free memory 62%, and no cell caused new swapouts. The receipt
is `/private/tmp/antfly-context-v19-five-source45f-centroid-v5/campaign-receipt.json`
(SHA-256 `5f309b83cf79c43830ec44d0c5c7dcf96a06e4eb560c71c306c42aed4c911ca9`).
The recipe is not eligible for release. The subsequent frozen direct-key v2
diagnostic completed all 72 cells in 83.90 seconds: 36 exact controls passed,
and none of 36 compressed candidates passed. Peak group RSS was 602.31 MiB,
minimum reported free memory 68%, and no cell caused new swapouts. Its receipt
is `/private/tmp/antfly-context-v19-five-source45f-direct-key-v2/campaign-receipt.json`
(SHA-256 `7a5b799d89721c5026c06d3c134d69ecce2ac1f4b9a61b2056e5e3bff07d7020`).
The fixed-step optimizer worsened training mass error in 34 of 36 blocks;
median training maximum error rose from 19.54% to 60.95%. This identifies an
optimizer convergence defect, not evidence that direct-key optimization cannot
work. A safeguarded training-only convergence experiment is required before
another validation sweep. Stored-f16 payloads must remain frozen before
validation, and fresh independent evaluation is required before promotion.

The safeguarded v3 training-only experiment completed all 36 cells in 522.40
seconds, with 789.75 MiB peak process-group RSS, 69% minimum reported free
memory and zero new swapouts. Each trial quantizes keys to f16, refits positive
mass weights and checks the resulting f32 bias bytes before accepting a strict
training maximum-error improvement. All 36 fits improved; median maximum mass
error fell from 19.54% to 11.21%. Five owner-9 blocks reached the 5% training
mass gate. Validation query tensors were hash-checked but never numerically
loaded, and no candidate received acceptance or serving authority. The receipt
is `/private/tmp/antfly-context-v19-five-source45f-direct-key-training-v3/campaign-receipt.json`
(SHA-256 `585e7d73999036bb6bf3da784a06a442f8a2fe76ce874a8188dbf014964e2ec9`).
Its `optimizer_converged` field means a specified improvement threshold, not
stationarity: 35 cells accepted all four allowed steps. A separately frozen
training continuation must distinguish convergence, a stalled search and an
exhausted budget. These payloads belong to the captured request; the production
contract still includes request-specific private replay, fitting, validation
and publication in TTFT. The Python fit cost is diagnostic and cannot establish
native serving performance or be treated as a reusable per-model payload.
