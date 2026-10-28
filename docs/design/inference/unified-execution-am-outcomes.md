# AM: immutable CUDA checkpoint ownership

Baseline: `ea68cceacb`. This follows the AL device regression failure rather
than treating the earlier failed run as qualified.

## Shared ownership contract

A resident checkpoint descriptor can own its GPU allocation while its metadata
belongs to the model. Four CUDA fast-path guards incorrectly required
`owns_buffer=false`; normal uploaded checkpoint entries have `owns_buffer=true`
and `owned_by_tensor=false`. They consequently declined legitimate immutable
weights, including recurrent state borrowing.

One private predicate now requires model-owned metadata, exact resident-cache
identity and absence of strict training ownership. Graph weight pointers, token
bursts, recurrent page batches and recurrent state borrowing share it. Their
existing dtype, shape, profile, capability and execution-policy restrictions
remain in place. Detached handles and external aliases cannot establish
immutability merely by sharing a device pointer.

The normalization constant cache retains its existing cold-only membership
check so cache hits acquire no extra scan. Its ownership comment is corrected.
This change consolidates host eligibility; it does not reduce device kernel
source or alter compiled GPU arithmetic. No timing improvement is claimed.

## Validation

- The driver-free metadata regression passes with the CUDA module compiled.
  It covers allocation-owning resident uploads, model-owned views, detached
  aliases, tensor-owned metadata, strict training and F32-only borrowing.
- Both-backend compilation passes, generated-source freshness checks pass
  for 70 files and 129 native owned regions, and the expanded Linux ARM64
  CUDA regression binary compiles.
- The expanded device suite passes **50/50 tests**, including all twelve
  targeted cases, with **zero skips, failures or leaks** on Glowworm.
  It covers all six original AL cases plus recurrent
  batching, independent-page prefill, graph-head admission, token bursts,
  resident-cache teardown and the ownership regression. The collector verifies
  each named case separately from the 38 imported smoke tests. The original
  AL failure remains a negative control for the corrected result collector.
- Retained binary, remote inputs and result archive hashes all match; the
  measured CUDA compute source matches the committed ownership fix.

Device archive SHA256:
`b6a2a1e9e94a7f179712ba102b419d8aaf281e062292ec5471b2aeaed821da8a`.

Evidence: `.benchmark-assets/1166/am-campaign/`. The original failed evidence
remains under `.benchmark-assets/1166/al-campaign/device-final/`.
