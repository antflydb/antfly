# Metadata Simulation Determinism Audit

This is the Phase 0 inventory for the first metadata world:
`metadata/sim_harness.zig`, including its `raft/sim_harness.zig` transport.
It records replay risks; it does not claim the metadata adapter is complete.

| Input class | Current use | Phase 0 classification | Adapter requirement |
|---|---|---|---|
| Campaign randomness | `runMetadataVoprCampaign` creates `DefaultPrng` from `cfg.seed`; transport action selection and parameters consume that stream. | Seeded, but opaque. A seed reproduces only while draw order and enabled actions remain identical. | Move every draw behind `ChoiceSource`; record the site, occurrence, canonical enabled IDs, and selected ID. |
| Network delivery | `VirtualHttpNetwork` owns a virtual tick, FIFO/seeded-random release, partitions, drops, duplication, and tick delays. Managed HTTP simulation forces synchronous transport and `sim://` endpoints. | Controlled. The internal random streams are independently seeded but are not yet recorded choices. | Expose message delivery and loss decisions as transitions; retain virtual ticks as world state. |
| Metadata time | `MetadataHttpClusterSimulation` owns a `ManualClock`, starts at 1,000 ms, and advances 100 ms in `stepAll`/`stepAllExcept`. Retry clocks use virtual network ticks. | Controlled in the first world. | Make clock advance an explicit transition and include logical time in observations. |
| Real time fallback | `currentGroupStatusTimestampMs` calls `Clock.real().nowRealtimeMs()`. | Uncontrolled whenever this helper participates in scenario-visible state. | Inject the scenario clock and remove the fallback from replayable paths before the metadata Phase 1 exit condition. |
| Sleep | `DelayingRequestExecutor` and several Raft harness tests call POSIX `nanosleep`. | Uncontrolled and unsuitable for a replayable scenario. The first metadata VOPR uses virtual delay faults, but shares a module containing this path. | Reject real-delay executors in replayable worlds; model delay as queued events and virtual-clock advances. |
| Threads | There are no direct `std.Thread`/spawn calls in either audited harness. `ManagedHttpClusterSimulation.init` forces `async_transport = false`; virtual base URIs make host `start`/`stop` no-ops, and metadata background runtimes use manual mode. | Controlled in the first world. Broader non-virtual harness modes can transitively start listeners or async runtimes and are outside this claim. | Add construction-time capability checks so replayable scenarios require manual background runtimes, virtual endpoints, and synchronous transport; admit task wakeups only as scheduler transitions. |
| Filesystem | Campaigns allocate `std.testing.tmpDir`, construct LMDB/catalog paths, and execute real storage code beneath those roots. Directory names are nondeterministic but are not intended semantic inputs. Host filesystem behavior and persisted bytes are real. | Partially controlled. Unique clean roots prevent cross-run contamination, but paths, host I/O errors, and physical timing must not enter choices or observations. | Normalize paths out of artifacts, hash fixture contents, use modeled storage for injected failures, and retain physical-storage replay as a differential mode. |
| External network | The first metadata world replaces HTTP routes with `VirtualHttpNetwork`; blackhole endpoints use loopback only in other focused tests. | Controlled for the first world. | Fail scenario construction if any executor is not registered with the virtual network. |
| Iteration/order | Enabled VOPR actions are currently selected by integer ranges and local control flow; several maps/catalogs may have implementation-defined iteration order. | Not yet proven canonical. | Scenario adapters must emit explicit transition descriptors and call `List.canonicalize`; observations must use `Builder.canonicalize`. |
| Process/environment | Temp roots, target, optimize mode, and source revision can differ. | Diagnostic only unless they alter behavior. | Record them in the header, but determine replay compatibility from simulator ABI plus scenario version. |

Phase 0 establishes the contracts that enforce the right edge of this table.
Phase 1 must close the real-clock path, turn campaign PRNG calls into choices,
and add construction-time capability checks before claiming exact replay for the
metadata world.
