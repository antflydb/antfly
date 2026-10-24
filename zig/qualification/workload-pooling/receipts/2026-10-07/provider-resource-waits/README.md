# Remote provider resource waits

A scheduled decision provider parks its read parent immediately before the
remote HTTP transport, retaining its operation, memory, original deadline and
cancellation owner. It reacquires through the SQL adapter's existing read-fence
validation before inspecting a successful response or returning a transport
error. Failed resumption destroys the response without parsing/normalization.
Ordinary writes do not attach this cancellable read-parent interface.

All 47 native build steps and 25 tests pass (18 embedded, seven coordinator),
with a 4 GiB compile scheduling budget and reported 1 GiB test compile peaks.
The real HTTP fixture observes the parent resource-blocked with all 17 retained
bytes, admits another query and protected control work, and verifies callbacks
stay on the parent worker. Five cases cover successful use, authority rejection
with deliberately invalid response JSON, HTTP 429, scheduler shutdown, and
transport disconnect. Resumption runs once and final operation/byte charges are
zero in every case. The earlier four-case passing source/log is preserved.

The GNU focused gate passes all 44 steps and the same 25 tests with pinned
Nix/glibc Zig, two CPUs and 4 GiB without extra swap (reported peak 3 GiB). Its first container attempt could not start because
a read-only source mount lacked the cache mountpoint; that setup failure is
preserved; the corrected passing run uses the same source bytes.

The combined native API gate also passes all 50 steps and 85 tests (39
embedded, 46 coordinator), with no skips/leaks. It compiles the real SQL
adapter callback wiring and includes the transport fixture. The launch session
became unavailable while compiling; its exact maker PID was verified live
and later absent after the complete passing summary, without restarting it. This change parks remote transport
waits only; embedded local inference continues to use its current parent path
until grant transfer and backend CPU team enforcement are implemented. No claim
is made about complete inference/background scheduling or automatic defaults.
