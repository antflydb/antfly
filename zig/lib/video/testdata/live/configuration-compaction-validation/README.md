# Live AVC configuration compaction

Rebasing the maintained Zig AVC registry frees unreachable configuration maps and
AVCC payloads. It remaps retained selectors, epoch initialization entries and the
active configuration together. Allocation and index validation precede mutation;
array capacity stays bounded by the configuration limit. Epoch initialization
and active entries remain reachable for reference state and future appends.

The parameter-planning test activates 600 distinct PPS configurations with a
limit of two live entries and checks that eviction returns to one entry and
reduces admitted bytes. Its activation prefixes have no macroblock bodies; this
is not a reconstructible codec corpus. Exhaustive allocation failures check clean
shared admission ownership. Existing geometry/pixel, held-window, malformed
append/retry tests continue to qualify reconstruction and ownership.

ReleaseSafe macOS passes 242 video (two skips), 23 live and 49 CPU/Metal model
checks. Actual Linux ARM64 passes 217 video (27 skips), 23 live and 44 model checks
(five Metal skips). Fresh native CPU, Metal and Linux pretrained runs cover six
rolling and two sparse avc3 intervals. All vectors match complete-clip baselines
and the previous source-bound incremental outputs exactly, with unchanged tokens,
source clocks and reconstruction counters (60 packets / 33,683 bytes per policy).
This phase does not rerun the full-replay mode or regenerate PyTorch oracles.

The previously pinned CPU/MPS oracles in dynamic-coordinator-validation qualify
all eight fresh outputs: exact rankings and full top-k overlap (1, 3, 5, 8), with
maximum coordinate error below 2.08e-6. Conservative prefix cache behavior remains
zero rolling hits/eleven stores and zero sparse hits/four stores. Suite paths use
the prior temporary fixture; its checked-in copy and fixture hashes bind the same
bytes. Compiler, source, fixture, binary and artifact hashes are recorded below.

No runtime dependency is added. The full non-CUDA objective remains active:
broader changing-tool pretrained streams, persistent-owner fuzzing, sustained
full-pipeline benchmarks and remaining protocol qualification are not established
by this registry test. Admitted byte reclamation is not measured RSS/throughput.
