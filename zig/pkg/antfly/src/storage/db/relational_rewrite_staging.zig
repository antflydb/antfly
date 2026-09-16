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

//! Explicit row transformation for shared restore staging. Both immutable
//! snapshot rows and retained after-images use the same program set. Prepared
//! batches contain final values; Raft/standby replay never re-evaluates a newer
//! schema. This layer does not grant source capture or publication authority.
const std = @import("std");
const contract = @import("relational_rewrite_contract.zig");
const staging = @import("restore_staging.zig");
const types = @import("types.zig");
const retained = @import("../retained_effects.zig");
const keys = @import("../internal_keys.zig");
const DB = @import("db.zig").DB;
const Allocator = std.mem.Allocator;
const LogicalRow = @import("relational_rewrite_program.zig").LogicalRow;

pub const ProgramSet = @import("relational_rewrite_program.zig").ProgramSet;
/// A numeric source schema version and a read-only handle do not identify the
/// snapshot cut. Check the imported, authenticated source-copy proof with one
/// point read before scanning; the materializer verifies the artifact itself.
pub fn requireSnapshotSource(alloc: Allocator, scope: staging.Scope, source: *DB) !void {
    try scope.validate();
    const binding = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
    const source_scope = binding.source_scope orelse return error.RestoreSourceProofMissing;
    if (source.open_mode != .query_readonly or !source.core.identity_namespace.eql(scope.source_namespace)) return error.RestoreStagingScopeChanged;
    const portable = @import("../portable_backup.zig");
    const raw = (try source.core.getStoreValue(alloc, portable.source_copy_proof_key)) orelse return error.RestoreSourceProofMissing;
    defer alloc.free(raw);
    const expected = try (portable.SourceCopyProof{ .scope = source_scope, .applied_index = binding.source_applied_index, .retained_start = binding.retained_start }).encode();
    if (!std.mem.eql(u8, raw, &expected)) return error.RestoreStagingScopeChanged;
}

fn page(arena: *std.heap.ArenaAllocator, raw: []const u8, next: staging.Progress, writes: []const types.BatchWrite, deletes: []const []const u8, timestamps: []const staging.Timestamp, source_effects: u32) !staging.PreparedPage {
    // The caller owns the arena until this succeeds.
    const encoded = try next.encode(arena.allocator());
    return .{ .arena = arena.*, .phase = next.phase, .batch = .{
        .writes = writes,
        .deletes = deletes,
        .sync_level = .write,
        .restore_staging = .{ .rewrite_page = .{ .expected = staging.digest(raw), .next = encoded, .scope = next.scope.digest(), .timestamps = timestamps, .source_effects = source_effects } },
    } };
}

/// Point-reads one checksum-verified REF3 frame. A frame may span many bounded
/// target pages, but its acknowledgement advances only after its last effect.
/// A retry reconstructs exactly the same values from the immutable program.
pub fn prepareTail(target: *DB, alloc: Allocator, scope: staging.Scope, source: *DB, programs: *const ProgramSet, max_effects: usize, cancellation: types.CancellationToken) !staging.PreparedPage {
    return prepareTailInternal(target, alloc, scope, source, null, programs, max_effects, cancellation);
}

/// Remote transport assembles one immutable frame, verifies its advertised
/// digest, then uses the same transformer as local capture. Source integrity
/// bytes never become target claims. The caller retains frame for this call.
pub fn prepareTailFrame(target: *DB, alloc: Allocator, scope: staging.Scope, frame: []const u8, expected_digest: contract.Digest, programs: *const ProgramSet, max_effects: usize, cancellation: types.CancellationToken) !staging.PreparedPage {
    if (frame.len > 16 * 1024 * 1024) return error.InvalidRestoreStagingCommand;
    return prepareTailInternal(target, alloc, scope, null, .{ .bytes = frame, .digest = expected_digest }, programs, max_effects, cancellation);
}

fn prepareTailInternal(target: *DB, alloc: Allocator, scope: staging.Scope, source: ?*DB, frame: ?struct { bytes: []const u8, digest: contract.Digest }, programs: *const ProgramSet, max_effects: usize, cancellation: types.CancellationToken) !staging.PreparedPage {
    try programs.requireScope(scope);
    if (max_effects == 0 or max_effects > 128 or (source != null and !source.?.core.identity_namespace.eql(scope.source_namespace)) or
        !target.core.identity_namespace.eql(scope.target_namespace)) return error.InvalidRestoreStagingCommand;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const raw = (try target.core.getStoreValue(owned, staging.key)) orelse return error.RestoreStagingScopeChanged;
    const parsed = try staging.Progress.decode(owned, raw);
    var next = parsed.value;
    if (!std.mem.eql(u8, &next.scope.digest(), &scope.digest())) return error.RestoreStagingScopeChanged;
    if (next.phase == .canceled) return error.RestoreStagingCanceled;
    if (next.phase != .importing) return error.RestoreStagingInProgress;
    const previous = next.rewrite orelse return error.InvalidRestoreStagingCommand;
    if (!previous.snapshot_complete or previous.final_cut != null) return error.RestoreStagingInProgress;
    var source_txn = if (source) |db| try db.core.store.beginReadTxn() else null;
    defer if (source_txn) |*txn| txn.abort();
    var namespace: [24]u8 = undefined;
    @import("doc_identity.zig").encodeNamespace(&namespace, scope.source_namespace);
    const binding = scope.rewrite.?;
    var reader = if (frame) |value| try retained.Reader.init(value.bytes, try std.math.add(u64, previous.sequence, 1)) else (try retained.read(&source_txn.?, namespace, binding.retained_epoch, binding.retained_pin, previous.sequence)) orelse
        return .{ .arena = arena, .phase = next.phase, .batch = null };
    if (frame) |value| if (!std.mem.eql(u8, &reader.frame_digest, &value.digest)) return error.RetainedEffectsCorrupt;
    if (previous.frame_offset != 0) {
        if (!std.mem.eql(u8, &reader.frame_digest, &previous.frame_digest)) return error.RestoreStagingProgressChanged;
        // Validate the stored byte offset against an actual frame boundary,
        // rather than trusting a cursor to skip or reinterpret committed bytes.
        while (reader.pos < previous.frame_offset) _ = (try reader.next()) orelse return error.InvalidRestoreStagingRecord;
        if (reader.pos != previous.frame_offset or reader.remaining != previous.frame_remaining) return error.InvalidRestoreStagingRecord;
    }
    var writes: std.ArrayList(types.BatchWrite) = .empty;
    var deletes: std.ArrayList([]const u8) = .empty;
    var timestamps: std.ArrayList(staging.Timestamp) = .empty;
    var output_bytes: usize = 0;
    var source_effects: u32 = 0;
    while (reader.remaining != 0 and writes.items.len + deletes.items.len < max_effects and source_effects < 1024) {
        try cancellation.check();
        const before = reader;
        const effect = (try reader.next()).?;
        if (effect.isIntegrity()) {
            source_effects += 1;
            continue;
        }
        if (keys.isRelationalRowKey(effect.key) == (programs.document_validator != null)) return error.InvalidRestoreStagingCommand;
        const key = (try keys.decodeStoredDocumentRowKeyAlloc(owned, effect.key)) orelse return error.InvalidRestoreStagingCommand;
        if (!target.core.byteRange().contains(key)) return error.RestoreStagingScopeChanged;
        const transformed: ?LogicalRow = if (effect.value) |value| if (programs.document_validator != null)
            .{ .json = try programs.preserveDocument(owned, value), .timestamp = effect.timestamp }
        else
            try programs.transformJson(owned, value) else null;
        const size = key.len +| if (transformed) |value| value.json.len else @as(usize, 0);
        if (size > 16 * 1024 * 1024) return error.RelationalRowResultTooLarge;
        if (output_bytes != 0 and output_bytes +| size > 1024 * 1024) {
            reader = before;
            break;
        }
        output_bytes += size;
        source_effects += 1;
        if (transformed) |value| {
            if (effect.timestamp == 0 or value.timestamp != effect.timestamp) return error.RetainedEffectsCorrupt;
            try writes.append(owned, .{ .key = key, .value = value.json });
            try timestamps.append(owned, .{ .key = key, .timestamp = value.timestamp });
        } else try deletes.append(owned, key);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(&next.logical_digest);
        hash.update(&reader.frame_digest);
        var offset: [8]u8 = undefined;
        std.mem.writeInt(u64, &offset, reader.pos, .little);
        hash.update(&offset);
        hash.final(&next.logical_digest);
        next.rows = std.math.add(u64, next.rows, 1) catch return error.InvalidRestoreStagingCommand;
    }
    if (reader.remaining == 0) {
        _ = try reader.next();
        next.rewrite.?.sequence = std.math.add(u64, previous.sequence, 1) catch return error.InvalidRestoreStagingCommand;
        next.rewrite.?.frame_digest = @splat(0);
        next.rewrite.?.frame_offset = 0;
        next.rewrite.?.frame_remaining = 0;
    } else {
        next.rewrite.?.frame_digest = reader.frame_digest;
        next.rewrite.?.frame_offset = @intCast(reader.pos);
        next.rewrite.?.frame_remaining = reader.remaining;
    }
    return page(&arena, raw, next, writes.items, deletes.items, timestamps.items, source_effects);
}

/// Called only with the coordinator's authenticated, drained final source cut.
/// No target validation/publication receipt exists until that exact sequence
/// has been consumed; caught-up-to-current is deliberately not completion.
pub fn prepareFinish(target: *DB, alloc: Allocator, scope: staging.Scope, cut: contract.FinalCut) !staging.PreparedPage {
    try scope.validate();
    if (scope.rewrite == null or !target.core.identity_namespace.eql(scope.target_namespace)) return error.InvalidRestoreStagingCommand;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const raw = (try target.core.getStoreValue(owned, staging.key)) orelse return error.RestoreStagingScopeChanged;
    const parsed = try staging.Progress.decode(owned, raw);
    var next = parsed.value;
    if (!std.mem.eql(u8, &next.scope.digest(), &scope.digest())) return error.RestoreStagingScopeChanged;
    const progress = next.rewrite orelse return error.InvalidRestoreStagingCommand;
    if (next.phase != .importing) {
        if (next.phase != .canceled and progress.final_cut != null and std.meta.eql(progress.final_cut.?, cut)) return .{ .arena = arena, .phase = next.phase, .batch = null };
        return error.RestoreStagingInProgress;
    }
    if (!progress.snapshot_complete or progress.frame_offset != 0 or progress.sequence != cut.sequence) return error.RestoreStagingInProgress;
    next.rewrite.?.final_cut = cut;
    next.phase = .imported;
    return page(&arena, raw, next, &.{}, &.{}, &.{}, 0);
}

/// Local-owner bridge for the final source proof. Distributed callers must
/// obtain the equivalent status under their authenticated ReadIndex boundary;
/// a raw caller-supplied sequence is not a final-fence certificate.
pub fn prepareFinishFromSource(target: *DB, alloc: Allocator, scope: staging.Scope, source: *DB) !staging.PreparedPage {
    const binding = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
    const source_scope = binding.source_scope orelse return error.InvalidRestoreStagingCommand;
    try scope.validate();
    if (!source.core.identity_namespace.eql(scope.source_namespace)) return error.RestoreStagingScopeChanged;
    const status = try source.onlineSourceStatus(source_scope);
    if (status.phase != .fenced or status.snapshot_phase != .published or status.start != binding.retained_start or status.admitted_applied_index != binding.source_applied_index)
        return error.RestoreStagingInProgress;
    return prepareFinish(target, alloc, scope, .{ .sequence = status.through_sequence, .applied_index = status.applied_index, .digest = status.cut_digest });
}
