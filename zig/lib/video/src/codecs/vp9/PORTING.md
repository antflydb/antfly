# VP9 source ownership

See [the maintained port contract](../PORTING.md). The initial fixtures qualify
profiles 0–3 at 8/10/12-bit across 4:2:0, 4:2:2 and 4:4:4, odd dimensions,
interframes and superframes. These fixtures establish reconstruction parity for
those streams, not exhaustive profile conformance. Public calls configure one
worker and do not expose the reference implementation's variadic control API.

Corruption hardening enables the reference header's coefficient range invariant
through ordinary owner failure propagation instead of an assertion:
`inv_txfm.h` specifies signed 16/18/20-bit intermediate values at 8/10/12-bit.
The 8-point ADST's originally 32-bit `s0`–`s7` use `tran_high_t` (64-bit), so
malformed coefficients cannot overflow before reaching the range checks.
`webm_pictures` retains a Smith-serialized entropy mutation that exercised the
previous overflow. Qualification and failed campaign evidence are retained in
`zig/lib/video/testdata/recovery-validation` from the repository root.
