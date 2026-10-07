# Ordinary SQL write runnable ownership

HTTP and pgwire ordinary SQL work now acquires the write class before execution
setup, after bounded preparation and coarse operation admission. HTTP settings
commands acquire query/write capacity according to their existing classification.
Protected terminal controls continue using the reserved control class. Describe
uses the query class. Disabled scheduling preserves existing behavior.

An ordinary write retains its grant through worker completion, including accepted
durable work. Its adapter withholds read suspension hooks and disables unscheduled
CPU children, while retaining serial spill I/O. Releasing runnable capacity during
write waits and adding mutation CPU quanta require a separate durable completion
owner; this slice does not yet provide those phases or enable automatic defaults.

The mixed protocol fixture exercises explicitly supported read-committed,
read-only BEGIN and protected ROLLBACK through HTTP and pgwire, requiring two
write-class grants and zero remaining scheduler charges. An unsupported default
serializable BEGIN must preserve its 501/0A000 diagnostic and retire its write
entry. The native row-mutation fixture also runs with scheduling enabled; HTTP
INSERT/RETURNING and authenticated pgwire INSERT/RETURNING and DELETE/RETURNING
require write grants and zero remaining charges. Existing joined UPDATE and
normalization cases exercise the serial backend. Stalled accepted mutation
schedules remain separate requirements.

The initial native API build completed compilation, but its runtime command
exited one: 36 embedded tests and 38 coordinator tests passed, with one fixture
failure and no skips/leaks. That fixture incorrectly expected successful default
serializable BEGIN on a source lacking range guards. Product isolation semantics
were preserved: the success case now specifies read committed and the unsupported
case checks the original diagnostic and grant cleanup. `initial-native-api.log.gz`
and `initial-source-hashes.json` preserve the failed evidence. `source-hashes.json`
pins the corrected implementation. The corrected native API run exits zero:
all 50 build steps, 36 embedded tests and 39 coordinator tests pass with no skips
or leaks. `native-api.log.gz` and `native-api.json` preserve complete output,
command, source hashes and terminal result. Complete production qualification
of this write snapshot remains pending.

Linux API qualification is prepared with the existing, SHA-256-verified pinned
Nix/glibc Zig image. The whole pooling API target does not link the storage
archive: it uses the unchanged 23 GiB compiler allowance and Zig's default memory
budget, with no `--maxrss` override. The Docker VM reports 25,159,036,928 bytes
(23.43 GiB). Storage-linked integration/production graphs retain their distinct
26 GiB admission constraint. The runtime gate remains a separate two-CPU, 2 GiB
run without extra swap or a compiler.

Pinned Nix/glibc GNU AArch64 API qualification now passes all 47 build steps
and the same 36 embedded plus 39 coordinator tests, without skips or leaks,
against commit `bbbe67c0b0`. It uses the default Zig build memory budget and the
unchanged 23 GiB API compiler allowance; no compile memory overrun is reported.
`linux-api.log.gz`, `linux-build.json` and `linux-compiler.json` preserve full
output, command, zero exit, actual VM capacity and exact compiler/image hashes.

Both exact ELF AArch64 artifacts also pass under two CPUs and 2 GiB without extra
swap or a compiler. The verifier checks ELF machine 183, matching SHA-256 hashes,
exact test counts, required mixed-protocol/control/weighted-class fixture names,
zero artifact exits and no skips/leaks. `linux-artifacts.json`,
`linux-runtime.json`, `linux-runtime.log.gz` and `linux-runtime-verifier.py`
preserve that proof. This qualifies the preparation, read-index and ordinary
write API snapshot together. It does not prove serverless scheduling, production
CLI compilation of the write changes, stalled accepted mutation handoff,
inference/background execution owners or automatic defaults.
