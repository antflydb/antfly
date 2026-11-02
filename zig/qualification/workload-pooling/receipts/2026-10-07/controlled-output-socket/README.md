# Per-write original controls

The corrected direct socket gate passed in ReleaseSafe on native macOS and
pinned Nix/glibc Linux. Native selected 29 tests: 21 passed and 8 Linux-only cases
skipped. GNU passed all 29, including all existing socket contracts and four new
controlled output cases. The final exact source manifests match the recorded
sources. Commands and logs are preserved. Native source hashes were captured
after completion; sources were unchanged during that final run. GNU hashes were
captured at launch. The GNU container's 1 GiB limit is a build limit, not a server
runtime bound.

`WriteControl` borrows original cancellation/deadline callbacks for one output
operation. Repeated writes use the remaining original absolute budget, without
changing the socket's request deadline. Native writes poll original controls at
bounded kernel waits and restore the configured native send timeout afterwards.
Fallback writes retain one structured operation while a joined monitor checks
the original controls; a monitoring tick does not start another write. The
monitor may run on another thread and must never checkpoint or park a borrowed
CPU grant. Default calls retain their original timeout timer behavior.

Real loopback TCP tests fill the transport and continue writing against the same
absolute control. Native deadline/cancellation stopped after 265/79 ms on macOS
and 250/52 ms on GNU. With zero async allowance, GNU fallback deadline/cancellation
stopped after 252/57 ms. The deadline was established 300 ms before a setup delay
of 50 ms. The original ten-second socket request deadline and configured logical
send timeout remain unchanged. The four-concurrent-task fixture limit is not a
server execution bound.

A GNU failure caught application cancellation being treated as executing-task
cancellation: `Io.recancel` panicked without a cancelled task. The correction
checks original application controls before recancel; genuine runtime cancellation
retains the existing behavior. Both final gates passed after this correction.
The earlier failure and its source manifest are preserved. An earlier native
fixture also assumed one write necessarily stalled; it was corrected to continue
against one unchanged fence. A prefill hang and stack sample are preserved;
setup now has its own bounded native timeout. Those attempts are not qualification.

These tests qualify the socket mechanism only. The controls are not yet wired
through HTTP response headers, H1 chunks, H2 lock/flow-control waits, or the linked
inference output adapter. A failure after partial frame transmission must retire
an uncertain connection before sibling writes. Real worker subprocesses, scheduler
output ownership, native models, broader overload behavior, and default enablement
remain outside this gate. Default pooling remains disabled.
