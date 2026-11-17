// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
/// Version 18 preserves JSON literal-null provenance and unique absence
/// predicates. Activate once per membership, before admitting these payloads.
pub const batch_protocol_version: u16 = 42;
/// Frozen completion BEGIN and owner/input-fenced PREPARE require durable
/// activation across every applying member before proposal or native apply.
pub const batch_completion_admission_protocol_version: u16 = 31;
/// Complete-certificate publication adds retained accounting and an ordered
/// publication journal. Version-31 admission support does not imply this phase.
pub const batch_completion_publication_protocol_version: u16 = 32;
/// Version 33 retains full original participant owners for restart recovery.
/// Old v1/v2 admissions and certificates retain their original protocol floors.
pub const batch_completion_recovery_protocol_version: u16 = 33;
/// Version 34 activates consumer-independent root policy and typed policy receipts.
pub const batch_history_policy_protocol_version: u16 = 34;
/// Version 35 retains whole-source purpose without copying root-local heads.
pub const batch_source_completion_protocol_version: u16 = 35;
/// Version 36 activates separately dispatched, frozen-owner source-plan staging.
pub const batch_source_plan_protocol_version: u16 = 36;
/// Version 37 admits atomic source-global checkpoint head intents.
pub const batch_source_checkpoint_protocol_version: u16 = 37;
/// Version 38 admits immutable bounded receipt windows before source BEGIN.
pub const batch_source_checkpoint_receipts_protocol_version: u16 = 38;
/// Version 39 retains and fences original source scope/attempt identity.
pub const batch_source_attempt_protocol_version: u16 = 39;
/// Version 40 dispatches source-scope bind/close/drop through the ordered owner.
pub const batch_source_scope_control_protocol_version: u16 = 40;
/// Version 41 applies original-coordinator-authorized source attempt retirement.
pub const batch_source_retirement_protocol_version: u16 = 41;
/// Version 42 retains explicit ordinary transaction producer identity.
pub const batch_ordinary_producer_protocol_version: u16 = 42;
fn isOrdinaryProducer(value: anytype) bool {
    if (comptime !@hasField(@TypeOf(value), "ordinary_producer")) return false;
    return value.ordinary_producer;
}
fn hasSourceScope(value: anytype) bool {
    if (comptime !@hasField(@TypeOf(value), "source_scope")) return false;
    return value.source_scope != null;
}
fn checkpointProtocol(value: anytype) u16 {
    // Generic callers may deliberately model older request layouts. This
    // branch is never selected by hasCheckpoint for those shapes.
    if (comptime !@hasField(@TypeOf(value), "source_checkpoint")) return batch_source_checkpoint_protocol_version;
    if (comptime @TypeOf(value.source_checkpoint) == bool) return if (value.source_checkpoint_receipts) batch_source_checkpoint_receipts_protocol_version else batch_source_checkpoint_protocol_version;
    return if (value.source_checkpoint.?.receipt_window != 0) batch_source_checkpoint_receipts_protocol_version else batch_source_checkpoint_protocol_version;
}
fn hasCheckpoint(value: anytype) bool {
    if (comptime !@hasField(@TypeOf(value), "source_checkpoint")) return false;
    return if (@TypeOf(value.source_checkpoint) == bool) value.source_checkpoint else value.source_checkpoint != null;
}
fn completionEnvelopeVersion(a: @import("std").mem.Allocator, bytes: []const u8, comptime certificate: bool) !u16 {
    const std = @import("std");
    if (bytes.len == 0 or bytes.len > 16 * 1024 * 1024) return error.InvalidTxnRecord;
    const Header = if (certificate) struct { certificate: struct { manifest: struct { version: u16 = 1 } } } else struct { admission: struct { version: u16 = 1 } };
    var parsed = std.json.parseFromSlice(Header, a, bytes, .{ .ignore_unknown_fields = true, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidTxnRecord;
    defer parsed.deinit();
    return if (certificate) parsed.value.certificate.manifest.version else parsed.value.admission.version;
}
pub fn completionProtocolVersion(a: @import("std").mem.Allocator, request: anytype) !?u16 {
    const mutation = request.transaction orelse return null;
    return switch (mutation) {
        .publish_completion => |value| if (try completionEnvelopeVersion(a, value.certificate, true) >= 3) batch_completion_recovery_protocol_version else batch_completion_publication_protocol_version,
        .begin => |value| if (isOrdinaryProducer(value)) batch_ordinary_producer_protocol_version else if (hasSourceScope(value)) batch_source_attempt_protocol_version else if (hasCheckpoint(value)) checkpointProtocol(value) else if (value.source_purpose != null) batch_source_completion_protocol_version else if (value.completion_admission) |bytes| (if (try completionEnvelopeVersion(a, bytes, false) >= 3) batch_completion_recovery_protocol_version else batch_completion_admission_protocol_version) else if (value.completion_owner != null) batch_completion_admission_protocol_version else null,
        .prepare => |value| if (isOrdinaryProducer(value)) batch_ordinary_producer_protocol_version else if (hasSourceScope(value)) batch_source_attempt_protocol_version else if (hasCheckpoint(value)) checkpointProtocol(value) else if (value.source_purpose != null) batch_source_completion_protocol_version else if (value.completion_owner != null) batch_completion_admission_protocol_version else null,
        .resolve => |value| if (hasSourceScope(value)) batch_source_attempt_protocol_version else if (value.source_purpose != null) batch_source_completion_protocol_version else if (value.completion_owner != null) batch_completion_admission_protocol_version else null,
        else => null,
    };
}

/// Dedicated retained-history control entries require native ordered dispatch
/// and a durable all-applying-member activation barrier.
pub const batch_history_control_protocol_version: u16 = 30;
/// Version 29 understands dense text-origin and int8-stub flags. Activation
/// certifies every applying replica before these envelopes or fragments appear.
pub const batch_dense_artifact_flags_protocol_version: u16 = 29;
/// Version 28 preserves neutral document-version semantics when decoding
/// guarded graph cleanup. Earlier v27 peers reject these commands, so the
/// corrected leader must activate this version across every applying replica.
pub const batch_graph_cleanup_neutral_protocol_version: u16 = 28;
/// Explicit document revision semantics and source-preserving retained tails.
/// Missing command tags retain the pre-v27 timestamp semantics on replay.
pub const batch_document_revisions_protocol_version: u16 = 27;
/// Version 26 binds generated vector publications to resolved model revisions
/// and admits catalog-aware grouped-source activation.
pub const batch_execution_identity_protocol_version: u16 = 26;
/// Version 25 carries leader-planned LEANN records (`_leann_record`), which
/// replicas persist and apply verbatim in log order. Older peers cannot
/// treat them as empty batches.
pub const batch_leann_record_protocol_version: u16 = 25;
/// Version 24 commits curator review CAS, overrides and replay atomically.
pub const batch_resolver_review_protocol_version: u16 = 24;
/// Version 23 applies grouped-source private state and unit sources atomically.
pub const batch_group_source_protocol_version: u16 = 23;
pub const batch_row_semantics_protocol_version: u16 = 18;
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

/// Version 20 transfers durable relationship retirements during merges.
pub const batch_merge_retirements_protocol_version: u16 = 20;

/// Version 22 adds qualified endpoint cleanup, ordered retirement stamps, and
/// bounded owner revival pages. It subsumes version 21 endpoint incarnations.
pub const batch_graph_cleanup_generation_protocol_version: u16 = 22;

/// A decoder ceiling must not claim the retention apply protocol when this
/// serving compiler unit has no compiled native owner dispatch.
pub fn maximumForHistoryApply(native_history_apply: bool) u16 {
    return if (native_history_apply) batch_protocol_version else @min(batch_protocol_version, batch_history_control_protocol_version - 1);
}

test "data raft protocol advertisement requires native history apply support" {
    const std = @import("std");
    try std.testing.expectEqual(batch_protocol_version, maximumForHistoryApply(true));
    try std.testing.expect(maximumForHistoryApply(false) < batch_history_control_protocol_version);
    try std.testing.expectEqual(@as(u16, 29), maximumForHistoryApply(false));
}

test "completion recovery protocol classifies decoded envelope versions without downgrading legacy" {
    const std = @import("std");
    const a = std.testing.allocator;
    const Mutation = union(enum) {
        begin: struct { completion_admission: ?[]const u8 = null, completion_owner: ?u8 = null, source_purpose: ?[32]u8 = null },
        prepare: struct { completion_owner: ?u8 = null, source_purpose: ?[32]u8 = null },
        resolve: struct { completion_owner: ?u8 = null, source_purpose: ?[32]u8 = null },
        publish_completion: struct { certificate: []const u8 },
        other,
    };
    const Request = struct { transaction: ?Mutation };
    try std.testing.expectEqual(@as(?u16, null), try completionProtocolVersion(a, Request{ .transaction = null }));
    try std.testing.expectEqual(@as(?u16, 31), try completionProtocolVersion(a, Request{ .transaction = .{ .begin = .{ .completion_admission = "{\"admission\":{\"version\":2}}" } } }));
    try std.testing.expectEqual(@as(?u16, 31), try completionProtocolVersion(a, Request{ .transaction = .{ .begin = .{ .completion_admission = "{\"admission\":{}}" } } }));
    try std.testing.expectEqual(@as(?u16, 33), try completionProtocolVersion(a, Request{ .transaction = .{ .begin = .{ .completion_admission = "{\"admission\":{\"ver\\u0073ion\":3}}" } } }));
    try std.testing.expectEqual(@as(?u16, 32), try completionProtocolVersion(a, Request{ .transaction = .{ .publish_completion = .{ .certificate = "{\"certificate\":{\"manifest\":{\"version\":2}}}" } } }));
    try std.testing.expectEqual(@as(?u16, 33), try completionProtocolVersion(a, Request{ .transaction = .{ .publish_completion = .{ .certificate = "{\"certificate\":{\"manifest\":{\"version\":3}}}" } } }));
    try std.testing.expectError(error.InvalidTxnRecord, completionProtocolVersion(a, Request{ .transaction = .{ .begin = .{ .completion_admission = "{\"admission\":{\"version\":2,\"version\":3}}" } } }));
    try std.testing.expectEqual(@as(?u16, 35), try completionProtocolVersion(a, Request{ .transaction = .{ .begin = .{ .completion_owner = 1, .source_purpose = @splat(1) } } }));
    try std.testing.expectEqual(@as(?u16, 35), try completionProtocolVersion(a, Request{ .transaction = .{ .prepare = .{ .completion_owner = 1, .source_purpose = @splat(1) } } }));
    try std.testing.expectEqual(@as(?u16, 35), try completionProtocolVersion(a, Request{ .transaction = .{ .resolve = .{ .completion_owner = 1, .source_purpose = @splat(1) } } }));
    try std.testing.expectError(error.InvalidTxnRecord, completionEnvelopeVersion(a, "invalid", false));
    const Failure = struct {
        fn run(allocator: std.mem.Allocator) !void {
            try std.testing.expectEqual(@as(u16, 3), try completionEnvelopeVersion(allocator, "{\"admission\":{\"version\":3}}", false));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.run, .{});
}

test "source checkpoint receipt windows require activation before every BEGIN" {
    const std = @import("std");
    const Mutation = union(enum) {
        begin: struct { completion_admission: ?[]const u8 = null, completion_owner: ?u8 = null, source_purpose: ?[32]u8 = @splat(1), source_checkpoint: bool = true, source_checkpoint_receipts: bool = false },
        prepare: struct { completion_owner: ?u8 = null, source_purpose: ?[32]u8 = @splat(1), source_checkpoint: ?struct { receipt_window: u32 = 0 } = .{} },
        resolve: struct { completion_owner: ?u8 = null, source_purpose: ?[32]u8 = null },
        publish_completion: struct { certificate: []const u8 },
        other,
    };
    const Request = struct { transaction: ?Mutation };
    const a = std.testing.allocator;
    try std.testing.expectEqual(@as(?u16, 37), try completionProtocolVersion(a, Request{ .transaction = .{ .begin = .{} } }));
    try std.testing.expectEqual(@as(?u16, 38), try completionProtocolVersion(a, Request{ .transaction = .{ .begin = .{ .source_checkpoint_receipts = true } } }));
    try std.testing.expectEqual(@as(?u16, 37), try completionProtocolVersion(a, Request{ .transaction = .{ .prepare = .{} } }));
    try std.testing.expectEqual(@as(?u16, 38), try completionProtocolVersion(a, Request{ .transaction = .{ .prepare = .{ .source_checkpoint = .{ .receipt_window = 3 } } } }));
    try std.testing.expectEqual(@as(?u16, 35), try completionProtocolVersion(a, Request{ .transaction = .{ .begin = .{ .source_checkpoint = false } } }));
    try std.testing.expectEqual(@as(?u16, 35), try completionProtocolVersion(a, Request{ .transaction = .{ .prepare = .{ .source_checkpoint = null } } }));
}

test "raft batch scoped source attempts require protocol 39 before every phase" {
    const Request = struct {
        transaction: ?union(enum) {
            begin: struct { completion_admission: ?[]const u8 = null, completion_owner: ?u8 = 1, source_purpose: ?[32]u8 = @splat(1), source_scope: ?u8 = 1 },
            prepare: struct { completion_owner: ?u8 = 1, source_purpose: ?[32]u8 = @splat(1), source_scope: ?u8 = 1 },
            resolve: struct { completion_owner: ?u8 = 1, source_purpose: ?[32]u8 = @splat(1), source_scope: ?u8 = 1 },
            publish_completion: struct { certificate: []const u8 },
            ordinary: void,
        },
    };
    const a = @import("std").testing.allocator;
    for ([_]Request{ .{ .transaction = .{ .begin = .{} } }, .{ .transaction = .{ .prepare = .{} } }, .{ .transaction = .{ .resolve = .{} } } }) |request|
        try @import("std").testing.expectEqual(@as(?u16, 39), try completionProtocolVersion(a, request));
}

test "data raft ordered history ordinary producer requires its own activation floor" {
    const std = @import("std");
    const Mutation = union(enum) {
        begin: struct { ordinary_producer: bool = false, completion_admission: ?[]const u8 = null, completion_owner: ?u8 = null, source_purpose: ?[32]u8 = null },
        prepare: struct { ordinary_producer: bool = false, completion_owner: ?u8 = null, source_purpose: ?[32]u8 = null },
        resolve: struct { completion_owner: ?u8 = null, source_purpose: ?[32]u8 = null },
        publish_completion: struct { certificate: []const u8 },
        other,
    };
    const Request = struct { transaction: ?Mutation };
    try std.testing.expectEqual(@as(?u16, 42), try completionProtocolVersion(std.testing.allocator, Request{ .transaction = .{ .begin = .{ .ordinary_producer = true } } }));
    try std.testing.expectEqual(@as(?u16, 42), try completionProtocolVersion(std.testing.allocator, Request{ .transaction = .{ .prepare = .{ .ordinary_producer = true } } }));
    try std.testing.expectEqual(@as(?u16, 31), try completionProtocolVersion(std.testing.allocator, Request{ .transaction = .{ .prepare = .{ .completion_owner = 1 } } }));
}
