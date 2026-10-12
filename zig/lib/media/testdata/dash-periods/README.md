# Multi-period DASH qualification

These receipts cover inferred Period timing, period-local numbering resets,
stable identities, announced gaps, future timelines, learned live endings,
prior-Period removal, pending-media loss, expired empty windows, clock
equivalence, rollback, exact rational presentation mapping and allocation-failure
cleanup across planning, reload and decoder reconfiguration. Period transitions
reset decoder epochs with repeated initialization URIs while held packet leases
remain valid. Period/timeline metadata and cumulative planning work have
independent limits; per-Period temporary storage is reclaimed immediately.

Linux ARM64 executes the portable media suite in a local container with networking
disabled. macOS additionally runs production authenticated loopback HTTP and
object-store integration tests; the new HTTP test exercises multi-period
playback with the same initialization object and repeated local segment numbers.
Source and fixture hashes identify the qualified inputs. These tests do not
establish general DASH/CMAF conformance, pretrained parity or codec conformance.
Broader HLS and the outstanding non-CUDA goal remain active.
