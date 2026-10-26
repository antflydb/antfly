# Native mass constraint-generation controller V5 draft

V5 preserves the V4 solver-neutral controller and its twelve compiled tests. It adds only two ownership campaigns using `std.testing.checkAllAllocationFailures`:

- the accepted two-round path, including one newly selected original row, full-coordinate dual zero-fill, stored conversion, and accepted-result teardown;
- the exact failed-witness retry path, including both callback-owned solve results and the terminal `WitnessRejected` unwind.

The injected solver interface, frozen selection order, 1e-9 then 1e-10 retry policy, original-coordinate witness, cancellation and absolute deadline behavior are unchanged. No optimizer is linked and no numerical parity, runtime integration, or serving claim is made.

The V5 tests have not been compiled or run. Root coordinates the bounded pinned-Zig command after review. The prior V4 command and 12/12 terminal audit remain the only compiled evidence.
