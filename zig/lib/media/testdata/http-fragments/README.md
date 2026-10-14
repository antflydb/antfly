# Incremental HTTP fMP4 delivery

`http_stream.Adapter.streamFmp4` validates status, exact range metadata and
identity encoding in httpx's response-header callback before permitting any
fragment publication. Body pieces feed the existing automatic moof/mdat framer
directly; complete fragments reach a synchronous consumer before HTTP response
completion. Consumer execution supplies backpressure. `FragmentDelivery.take`
moves an independent owned snapshot; untaken deliveries are destroyed on callback
return, including failure. Taken snapshots and packet bytes outlive both transport
and ingest owners. The final fragment is committed only after successful EOF.

Pre-entry URI/control/capacity rejection leaves ingest reusable and does not
enter HTTP. Any entered transport, metadata, framing, callback or cancellation
failure makes ingest terminal and discards its pending input. There is no implicit
retry that could duplicate previously published fragments. Explicit restart and
per-resource replay reconciliation remain caller work. Conservative cap-times-two
transport admission is retained, including httpx's buffered HTTP/2 fallback;
this phase does not claim reduced admission reservations or incremental HTTP/2.
Framer and snapshot allocations retain their own shared admission ownership.

Pending-buffer growth is geometric within its existing byte ceiling. Actual
capacity and simultaneous old/new buffers are charged before allocation. If a
shared pool cannot admit speculative growth, exact required growth is attempted;
backing allocation failure remains observable. One-byte input is qualified with
actual allocator counts and exact coded-byte preservation.

Portable tests cover clean packet parity, early callback delivery, taken snapshot
lifetime, bad status/encoding/range, transport reset after publication, callback
failure, cancellation, pre-entry admission denial, exact range consumption, tight
capacity fallback and every allocation failure. The production authenticated
chunked HTTP server withholds the response tail until the first callback releases
it: whole-response buffering would time out. It authenticates the request and
checks identity encoding. Six fragments are delivered; the retained first snapshot
is read again after ingest destruction. [Receipt](receipt.json) binds current
sources, platform/binary identities and completed logs. All 168 portable media
tests and twelve production authenticated HTTP tests pass on macOS ARM64 and
actual Linux ARM64. Both platforms pass the server-withheld-tail callback gate.

The reported largest pending capacity covers the retained framer buffer, not
whole-process peak memory, transient growth overlap, driver allocation or retained
consumer snapshots. HTTP/1 plaintext is exercised by the real socket gate; TLS
uses httpx's maintained streaming path but is not independently qualified here.
Partial-DASH manifest delivery, resumable resource progress and model coordinator
integration remain unfinished. The complete non-CUDA objective stays active.

Reproduce from the worktree with the repository Zig compiler:

```sh
zig test zig/lib/media/media_test_root.zig -O ReleaseSafe
zig test zig/lib/media/media_test_root.zig -O ReleaseSafe \
  -target aarch64-linux-musl --test-no-exec -femit-bin=/tmp/http-fragment-media
cd zig/lib/media
zig build test-media-http -Doptimize=ReleaseSafe --summary all
```

Execute the static ELF on Linux; the production HTTP tests require loopback
socket access. No runtime codec or transport library dependency is added.
