
## 11. Pre-handoff review + fix round (2026-08-26, uncommitted on top of b064b22de)

A high-effort multi-agent review (8 finder angles, 21 adversarially verified candidates) produced 10 findings; all were fixed and re-validated (perf wins intact: E2B 53.9→56.5 with repack, tokens identical, long-ctx split ~1.9x, split-routes oracle green, both -Dmetal variants build, targeted tests pass):

1. `-Dmetal=false` macOS link failure → `build_options.enable_metal` comptime guard on the new extern (empirically re-verified).
2. Double free on the repack OOM path → explicit ownership handoff, no errdefer armed across the transfer. (Same latent pattern pre-exists in the Q8 helpers — untouched, note for a follow-up.)
3-4/7. Auto-draft discovery restructured: runs BEFORE qualified-profile validation, admission, and the prompt estimate (speculation_requested now set at config build → estimate/limit/pre-encode-reuse consistent); reserves speculation slot units (degrading, not rejecting, when capacity is short — same at the native-generate lease); name-qualified like the registry companion rule (gemma-4 + -qat only) with manifest/config pre-validation; ALL draft-setup failures for a discovered drafter degrade to single-model generation (labeled draft_setup block) instead of failing the request. Verified live: flag + sibling present → request succeeds.
5. LM-head repack now gated on a real `lm_head` identity bit plumbed contract→rms-runtime→gated-runtime (tagged prepare) and across the metal_compute bridge (which had silently dropped the field — caught because the repack stopped firing), not on out_dim alone.
6. Repack env parse: `no`/`off`/`false`/`0` disable, unknown values warn+disable, `q4_0` warns loudly (kept for A/B evidence only).
8. Companion pull: never inherits --tasks/--capabilities (manifest inferred from its own plan) and skips when already installed.
9. One shared `pipelinedDecodeFrameEnabled()` policy in metal_runtime (cached env reads) used by both the pipeline and executor.
10. Q6_K/Q4_K variant env parsing case-insensitive with invalid-value warnings; forced nsg4/nsg8 gated on maxTotalThreadsPerThreadgroup; Q4_K v2 AUTO scoped to the TAIL workload via the descriptor's existing workload field (M4 + vocab-sized).

Review also REFUTED with evidence: JSON duplicate-key differential (Value parser errors on duplicates too), early-free of the parsed Value tree (typed request ALIASES it — the defer is required), prompt-cache/speculation pairing (blocked downstream by `!use_speculative`), executor-gate weakness (arm path re-checks the full gemma+PLE contract), suppression-mode barrier race (concurrent mode forces the scan on), split-scratch memory doubling (~2 MiB, trivial).

Known accepted residue for the M4 Pro follow-up: single-threaded repack at load (~seconds; parallelize with the cold-load worker pool), Q8 helpers' pre-existing errdefer pattern, batch endpoint double-parse, per-hazard attribution only visible in concurrent mode.
