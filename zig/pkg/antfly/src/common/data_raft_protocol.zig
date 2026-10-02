// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

/// Version 1 adds the internal `_timestamp_ns` field to data-Raft batch log
/// entries. Version 2 adds the fail-closed, durable activation barrier used to
/// turn newer formats on without retaining capability probes in the write
/// path. Version 3 adds replicated merge source fences and receiver
/// checkpoints; those controls must never appear before a durable v3 barrier.
/// Version 4 adds predecessor-fenced split deltas so sparse source Raft-index
/// watermarks cannot be mistaken for omitted replication work.
/// Version 5 transfers authoritative merge artifacts through durable replay.
/// Version 6 fences merge copy attempts across donor leadership changes.
/// Version 7 commits source-bound merge page effects and cursor atomically.
/// Version 8 adds source retention admission and receiver tail-watermark fences.
/// Version 9 requires durable source pins before acknowledgement and supports
/// resumable row chunks whose completion atomically advances receiver progress.
/// Version 10 requires native-authoritative Raft snapshots for source groups,
/// preserving the retained journal and prepared transaction state on replicas.
/// Version 11 retains coordinated integrity effects with primary rows, fences
/// shadow-interval publication, and supports retained-source staged rewrites.
/// Version 12 binds retained source Scope v2 to an explicit Raft/native
/// authority. Older decoders must not reinterpret its clock or pin identity.
/// Version 13 batches coordinator acknowledgements. All applying replicas
/// must cross the durable activation barrier before this command is admitted.
pub const batch_acknowledge_many_protocol_version: u16 = 13;
/// Version 14 includes typed direct-vector snapshot/tail payloads and their
/// retained-transaction admission accounting. Ordered artifact merges require
/// the complete decoder before admission, even for an empty first page.
pub const batch_protocol_version: u16 = 19;
pub const batch_artifact_catalog_protocol_version: u16 = 14;
/// Full producer publications require a separate all-member barrier; the
/// direct-vector decoder proof does not authorize asynchronous effect writes.
pub const batch_artifact_publication_protocol_version: u16 = 15;
/// Version 16 carries bounded authenticated publication uploads and the
/// ordered finalize control. Version 15 peers cannot ignore stage entries or
/// reinterpret a missing final payload as an ordinary empty batch.
pub const batch_artifact_publication_transport_protocol_version: u16 = 16;
/// Receiver-local imported-proof adoption is a distinct ordered decision.
/// It cannot be replayed as an empty legacy batch or admitted before every
/// applying member understands its evidence fence and standby envelope.
pub const batch_merge_proof_adoption_protocol_version: u16 = 17;
pub const batch_timestamp_protocol_version: u16 = 1;
pub const batch_activation_barrier_protocol_version: u16 = 2;
pub const batch_merge_transition_protocol_version: u16 = 3;
pub const batch_split_delta_predecessor_protocol_version: u16 = 4;
pub const batch_merge_artifacts_protocol_version: u16 = 5;
pub const batch_merge_copy_attempt_protocol_version: u16 = 6;
pub const batch_merge_page_protocol_version: u16 = 7;
pub const batch_online_source_protocol_version: u16 = 8;
pub const batch_source_pin_protocol_version: u16 = batch_source_scope_protocol_version;
pub const batch_merge_chunk_protocol_version: u16 = 9;
pub const batch_native_snapshot_protocol_version: u16 = 10;
pub const batch_relational_transfer_protocol_version: u16 = 11;
pub const batch_source_scope_protocol_version: u16 = 12;

/// Version 18 transfers durable relationship retirements during merges.
pub const batch_merge_retirements_protocol_version: u16 = 18;

/// Version 19 carries incarnation-fenced endpoint cleanup pages and versioned jobs.
pub const batch_graph_cleanup_generation_protocol_version: u16 = 19;
