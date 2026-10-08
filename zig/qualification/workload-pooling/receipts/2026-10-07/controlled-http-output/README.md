# Original request controls for HTTP output

Native ReleaseSafe selected 43 tests: 33 passed and 10 Linux-only cases skipped.
Pinned Nix/glibc GNU AArch64 passed all 43 in a 3 GiB container. Build/container
limits describe qualification resources, not server runtime bounds.

Controlled HTTP/1 headers, chunks and close preserve the original request
controls. HTTP/2 header encoding, frame writes, contended write locks and flow
credit waits use those controls. Credit waiting releases the connection lock;
a sibling PING can progress. Uncertain frames, header blocks or shared HPACK
encoder state retire the connection. Retirement is protected from task
cancellation. Rejected writes before any uncertainty preserve the connection.
Real Linux fallback task cancellation returns Canceled and retires uncertain
output; the original failure incorrectly returned SendFailed and is preserved.

The GNU manifest also records runtime bridge and standalone wiring provenance;
this HTTP library gate does not compile those bodies. The native hash capture
was performed after completion and matches the compiled-library GNU launch
hashes. Bridge callback qualification is recorded separately. Full production
frontend wiring, physical worker launch, formatter CPU ownership and whole-node
mixed overload remain required. Pooling remains disabled by default.
