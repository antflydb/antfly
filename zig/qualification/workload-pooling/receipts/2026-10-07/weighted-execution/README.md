# Weighted execution capsule contracts

`Execution.startWithSlots` admits a bounded CPU demand on one stable capsule;
existing one-slot callers delegate to it. Demand survives cooperative yields,
resource/output blocking and class changes, independently of retained bytes.
The fixture exercises inference and background demand alongside a protected
control grant, then verifies atomic rejection of zero/oversized demand and zero
charges after retirement. `InvalidRunnableDemand` now has an append-only stable
error detail, preserving its invalid-argument class across archive boundaries.

All 15 scheduler tests and all 26 error-ABI tests pass on native macOS and pinned
Nix/glibc GNU AArch64, with complete logs and source hashes retained here. The
small Linux contract containers have two CPUs and a 1 GiB memory/swap limit.
This is core/ABI evidence. Inference and background callers do not yet use this
weighted capsule API; checked archive execution operations and backend CPU team
enforcement still need integration. The new core bytes also await full API and
production qualification and do not enable automatic defaults.
