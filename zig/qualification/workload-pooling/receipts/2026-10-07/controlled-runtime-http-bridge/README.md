# Runtime HTTP output control callbacks

All eight selected runtime HTTP bridge/ABI tests pass on native macOS and pinned
Nix/glibc GNU AArch64 in ReleaseSafe. The output fence test exercises both sides
of the layout-only C callbacks: an expired start reaches no delegate, then a
successful stream retains its original control and rejects late writes and close
with Timeout or Canceled. Policy header ownership and cancellation precedence
also pass. The first fixture reused a closed writer for a second terminal case;
its failure is preserved and the corrected cases use independent streams.

Native source hashes were captured after completion and match the GNU launch
manifest. Standalone source is recorded for provenance only; this gate compiles
the runtime bridge, HTTP library and ABI, not standalone frontend wiring or
production workers. It does not establish physical formatter CPU ownership,
whole-node bounds or default-enable qualification.
