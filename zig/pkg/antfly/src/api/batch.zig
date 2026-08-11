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

const std = @import("std");
const db_mod = @import("../storage/db/mod.zig");
const document_mapper = @import("../storage/db/document_mapper.zig");
const public_limits = @import("public_limits.zig");

pub const BatchResult = struct {
    status: []const u8 = "committed",
    inserted: u32,
    deleted: u32,
    transformed: u32 = 0,
};

pub const OwnedBatchRequest = struct {
    writes: []db_mod.types.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    relational_identity_rewrites: []db_mod.types.RelationalIdentityRewrite = &.{},
    transforms: []db_mod.types.DocumentTransform = &.{},
    predicates: []db_mod.types.TransactionVersionPredicate = &.{},
    transaction_participants: [][]const u8 = &.{},
    split_checkpoint_range_start: ?[]u8 = null,
    split_checkpoint_range_end: ?[]u8 = null,
    split_transition_key: ?[]u8 = null,
    req: db_mod.types.BatchRequest = .{},

    pub fn deinit(self: *OwnedBatchRequest, alloc: std.mem.Allocator) void {
        for (self.writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes) |key| alloc.free(key);
        if (self.deletes.len > 0) alloc.free(self.deletes);
        for (self.relational_identity_rewrites) |rewrite| {
            alloc.free(@constCast(rewrite.old_key));
            alloc.free(@constCast(rewrite.new_key));
            alloc.free(@constCast(rewrite.value));
        }
        if (self.relational_identity_rewrites.len > 0) alloc.free(self.relational_identity_rewrites);
        for (self.transforms) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(transform.operations);
        }
        if (self.transforms.len > 0) alloc.free(self.transforms);
        for (self.predicates) |predicate| alloc.free(@constCast(predicate.key));
        if (self.predicates.len > 0) alloc.free(self.predicates);
        for (self.transaction_participants) |participant| alloc.free(participant);
        if (self.transaction_participants.len > 0) alloc.free(self.transaction_participants);
        if (self.split_checkpoint_range_start) |value| alloc.free(value);
        if (self.split_checkpoint_range_end) |value| alloc.free(value);
        if (self.split_transition_key) |value| alloc.free(value);
        self.* = undefined;
    }

    pub fn result(self: OwnedBatchRequest) BatchResult {
        return .{
            .inserted = @intCast(self.writes.len),
            .deleted = @intCast(self.deletes.len + self.relational_identity_rewrites.len),
            .transformed = @intCast(self.transforms.len),
        };
    }

    pub fn resultWithStatus(self: OwnedBatchRequest, status: []const u8) BatchResult {
        var result_value = self.result();
        result_value.status = status;
        return result_value;
    }
};

pub fn parseBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    return try parseBatchRequestWithOptions(alloc, body, .{
        .allocate = .alloc_always,
        .max_value_len = public_limits.max_json_value_len,
    }, false);
}

pub fn parseInternalBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    return try parseBatchRequestWithOptions(alloc, body, .{
        .allocate = .alloc_always,
        .max_value_len = public_limits.max_json_value_len,
    }, true);
}

fn parseBatchRequestWithOptions(
    alloc: std.mem.Allocator,
    body: []const u8,
    options: std.json.ParseOptions,
    allow_internal: bool,
) !OwnedBatchRequest {
    if (body.len == 0) return .{};

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, options) catch |err| switch (err) {
        error.ValueTooLong => return error.ValueTooLong,
        else => return error.InvalidBatchRequest,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidBatchRequest;
    const root = parsed.value.object;

    const writes: []db_mod.types.BatchWrite = writes: {
        if (root.get("inserts")) |inserts| {
            if (inserts == .null) break :writes &.{};
            const parsed_writes = try parseInserts(alloc, inserts);
            errdefer freeWrites(alloc, parsed_writes);
            break :writes parsed_writes;
        }
        break :writes &.{};
    };
    errdefer freeWrites(alloc, writes);

    const deletes: [][]const u8 = deletes: {
        if (root.get("deletes")) |deletes_value| {
            if (deletes_value == .null) break :deletes &.{};
            const parsed_deletes = try parseDeletes(alloc, deletes_value);
            errdefer freeDeletes(alloc, parsed_deletes);
            break :deletes parsed_deletes;
        }
        break :deletes &.{};
    };
    errdefer freeDeletes(alloc, deletes);

    const relational_identity_rewrites: []db_mod.types.RelationalIdentityRewrite = rewrites: {
        if (root.get("relational_identity_rewrites")) |rewrites_value| {
            if (rewrites_value == .null) break :rewrites &.{};
            const parsed_rewrites = try parseRelationalIdentityRewrites(alloc, rewrites_value);
            errdefer freeRelationalIdentityRewrites(alloc, parsed_rewrites);
            break :rewrites parsed_rewrites;
        }
        break :rewrites &.{};
    };
    errdefer freeRelationalIdentityRewrites(alloc, relational_identity_rewrites);

    const transforms: []db_mod.types.DocumentTransform = transforms: {
        if (root.get("transforms")) |transforms_value| {
            if (transforms_value == .null) break :transforms &.{};
            const parsed_transforms = try parseTransforms(alloc, transforms_value);
            errdefer freeTransforms(alloc, parsed_transforms);
            break :transforms parsed_transforms;
        }
        break :transforms &.{};
    };
    errdefer freeTransforms(alloc, transforms);

    const predicates: []db_mod.types.TransactionVersionPredicate = predicates: {
        const value = root.get("_predicates") orelse break :predicates &.{};
        if (!allow_internal or value != .array) return error.InvalidBatchRequest;
        const items = try alloc.alloc(db_mod.types.TransactionVersionPredicate, value.array.items.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |predicate| alloc.free(@constCast(predicate.key));
            alloc.free(items);
        }
        for (value.array.items, 0..) |item, i| {
            if (item != .object) return error.InvalidBatchRequest;
            const key_value = item.object.get("key") orelse return error.InvalidBatchRequest;
            const version_value = item.object.get("expected_version") orelse return error.InvalidBatchRequest;
            if (key_value != .string) return error.InvalidBatchRequest;
            items[i] = .{
                .key = try alloc.dupe(u8, key_value.string),
                .expected_version = try parseInternalU64(version_value),
            };
            initialized += 1;
        }
        break :predicates items;
    };
    errdefer {
        for (predicates) |predicate| alloc.free(@constCast(predicate.key));
        if (predicates.len > 0) alloc.free(predicates);
    }

    const sync_level = sync_level: {
        if (root.get("sync_level")) |sync_level_value| {
            if (sync_level_value == .null) break :sync_level db_mod.types.SyncLevel.propose;
            break :sync_level try syncLevelFromValue(sync_level_value);
        }
        break :sync_level db_mod.types.SyncLevel.propose;
    };

    var checkpoint_start: ?[]u8 = null;
    errdefer if (checkpoint_start) |value| alloc.free(value);
    var checkpoint_end: ?[]u8 = null;
    errdefer if (checkpoint_end) |value| alloc.free(value);
    const split_checkpoint: ?db_mod.types.SplitReplicationCheckpoint = checkpoint: {
        const value = root.get("_split_checkpoint") orelse break :checkpoint null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const source_value = object.get("source_group_id") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        const sequence_value = object.get("delta_sequence") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const source_group_id = try parseInternalU64(source_value);
        const destination_group_id = try parseInternalU64(destination_value);
        const delta_sequence = try parseInternalU64(sequence_value);
        const kind: db_mod.types.SplitReplicationCheckpoint.Kind = std.meta.stringToEnum(
            db_mod.types.SplitReplicationCheckpoint.Kind,
            kind_value.string,
        ) orelse return error.InvalidBatchRequest;
        const range_start = if (object.get("range_start")) |item| start: {
            if (item != .string) return error.InvalidBatchRequest;
            break :start item.string;
        } else "";
        const range_end = if (object.get("range_end")) |item| end: {
            if (item != .string) return error.InvalidBatchRequest;
            break :end item.string;
        } else "";
        checkpoint_start = try alloc.dupe(u8, range_start);
        checkpoint_end = try alloc.dupe(u8, range_end);
        if (transition_id == 0 or attempt_epoch == 0) return error.InvalidBatchRequest;
        break :checkpoint .{
            .kind = kind,
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .range_start = checkpoint_start.?,
            .range_end = checkpoint_end.?,
            .delta_sequence = delta_sequence,
        };
    };

    const split_replication: ?db_mod.types.SplitReplicationContext = replication: {
        const value = root.get("_split_replication") orelse break :replication null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const source_value = object.get("source_group_id") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        const table_value = object.get("namespace_table_id") orelse return error.InvalidBatchRequest;
        const shard_value = object.get("namespace_shard_id") orelse return error.InvalidBatchRequest;
        const range_value = object.get("namespace_range_id") orelse return error.InvalidBatchRequest;
        const operation_value = object.get("operation") orelse return error.InvalidBatchRequest;
        const sequence_value = object.get("sequence") orelse return error.InvalidBatchRequest;
        if (operation_value != .string) return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const source_group_id = try parseInternalU64(source_value);
        const destination_group_id = try parseInternalU64(destination_value);
        const table_id = try parseInternalU64(table_value);
        const shard_id = try parseInternalU64(shard_value);
        const range_id = try parseInternalU64(range_value);
        const sequence = try parseInternalU64(sequence_value);
        const bootstrap_sequence = if (object.get("bootstrap_sequence")) |bootstrap_value|
            try parseInternalU64(bootstrap_value)
        else
            null;
        const operation = std.meta.stringToEnum(db_mod.types.SplitReplicationContext.Operation, operation_value.string) orelse
            return error.InvalidBatchRequest;
        if (transition_id == 0 or attempt_epoch == 0 or source_group_id == 0 or destination_group_id == 0 or table_id == 0 or shard_id == 0 or range_id == 0) return error.InvalidBatchRequest;
        break :replication .{
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .identity_namespace = .{
                .table_id = table_id,
                .shard_id = shard_id,
                .range_id = range_id,
            },
            .operation = operation,
            .sequence = sequence,
            .bootstrap_sequence = bootstrap_sequence,
        };
    };

    var transition_key: ?[]u8 = null;
    errdefer if (transition_key) |value| alloc.free(value);
    const split_transition: ?db_mod.types.SplitTransitionMutation = transition: {
        const value = root.get("_split_transition") orelse break :transition null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const destination_group_id = try parseInternalU64(destination_value);
        if (transition_id == 0 or attempt_epoch == 0 or destination_group_id == 0) return error.InvalidBatchRequest;
        const kind = std.meta.stringToEnum(db_mod.types.SplitTransitionMutation.Kind, kind_value.string) orelse
            return error.InvalidBatchRequest;
        const split_key = if (object.get("split_key")) |item| key: {
            if (item != .string) return error.InvalidBatchRequest;
            break :key item.string;
        } else "";
        if ((kind == .prepare or kind == .start) and split_key.len == 0) return error.InvalidBatchRequest;
        transition_key = try alloc.dupe(u8, split_key);
        break :transition .{
            .kind = kind,
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .destination_group_id = destination_group_id,
            .split_key = transition_key.?,
        };
    };

    var transaction_participants: [][]const u8 = &.{};
    var transaction_participants_initialized: usize = 0;
    errdefer {
        for (transaction_participants[0..transaction_participants_initialized]) |participant| alloc.free(participant);
        if (transaction_participants.len > 0) alloc.free(transaction_participants);
    }
    const transaction: ?db_mod.types.TransactionMutation = transaction: {
        const value = root.get("_transaction") orelse break :transaction null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const phase_value = object.get("phase") orelse return error.InvalidBatchRequest;
        const txn_value = object.get("txn_id") orelse return error.InvalidBatchRequest;
        if (phase_value != .string or txn_value != .string or txn_value.string.len != 32) return error.InvalidBatchRequest;
        var txn_id: db_mod.types.TxnId = undefined;
        _ = std.fmt.hexToBytes(&txn_id, txn_value.string) catch return error.InvalidBatchRequest;
        if (std.mem.eql(u8, phase_value.string, "begin")) {
            const participants_value = object.get("participants") orelse return error.InvalidBatchRequest;
            if (participants_value != .array or participants_value.array.items.len == 0) return error.InvalidBatchRequest;
            transaction_participants = try alloc.alloc([]const u8, participants_value.array.items.len);
            for (participants_value.array.items, 0..) |participant, i| {
                if (participant != .string or participant.string.len == 0) return error.InvalidBatchRequest;
                transaction_participants[i] = try alloc.dupe(u8, participant.string);
                transaction_participants_initialized += 1;
            }
            break :transaction .{ .begin = .{
                .txn_id = txn_id,
                .begin_timestamp = try parseInternalU64(object.get("begin_timestamp") orelse return error.InvalidBatchRequest),
                .created_at_ns = try parseInternalU64(object.get("created_at_ns") orelse return error.InvalidBatchRequest),
                .topology_epoch = try parseInternalU64(object.get("topology_epoch") orelse return error.InvalidBatchRequest),
                .retain_terminal = if (object.get("retain_terminal")) |item| switch (item) {
                    .bool => |flag| flag,
                    else => return error.InvalidBatchRequest,
                } else false,
                .participants = transaction_participants,
            } };
        }
        if (std.mem.eql(u8, phase_value.string, "prepare")) break :transaction .{ .prepare = .{
            .txn_id = txn_id,
            .topology_epoch = try parseInternalU64(object.get("topology_epoch") orelse return error.InvalidBatchRequest),
        } };
        if (std.mem.eql(u8, phase_value.string, "resolve")) {
            const status_value = object.get("status") orelse return error.InvalidBatchRequest;
            if (status_value != .string) return error.InvalidBatchRequest;
            const status = std.meta.stringToEnum(db_mod.types.TxnStatus, status_value.string) orelse return error.InvalidBatchRequest;
            if (status == .pending) return error.InvalidBatchRequest;
            break :transaction .{ .resolve = .{
                .txn_id = txn_id,
                .status = status,
                .commit_version = try parseInternalU64(object.get("commit_version") orelse return error.InvalidBatchRequest),
            } };
        }
        if (std.mem.eql(u8, phase_value.string, "acknowledge")) {
            const participant_value = object.get("participant") orelse return error.InvalidBatchRequest;
            if (participant_value != .string or participant_value.string.len == 0) return error.InvalidBatchRequest;
            transaction_participants = try alloc.alloc([]const u8, 1);
            transaction_participants[0] = try alloc.dupe(u8, participant_value.string);
            transaction_participants_initialized = 1;
            break :transaction .{ .acknowledge = .{
                .txn_id = txn_id,
                .participant = transaction_participants[0],
            } };
        }
        if (std.mem.eql(u8, phase_value.string, "cleanup")) break :transaction .{ .cleanup = .{
            .txn_id = txn_id,
            .cutoff_timestamp = try parseInternalU64(object.get("cutoff_timestamp") orelse return error.InvalidBatchRequest),
            .retained_cutoff_timestamp = try parseInternalU64(object.get("retained_cutoff_timestamp") orelse return error.InvalidBatchRequest),
        } };
        return error.InvalidBatchRequest;
    };

    if (split_transition != null and
        (writes.len != 0 or deletes.len != 0 or transforms.len != 0 or
            predicates.len != 0 or split_checkpoint != null or split_replication != null or transaction != null))
    {
        return error.InvalidBatchRequest;
    }
    if (transaction != null and (split_checkpoint != null or split_replication != null or split_transition != null)) {
        return error.InvalidBatchRequest;
    }
    if (predicates.len != 0 and transaction == null) return error.InvalidBatchRequest;
    if (transaction) |mutation| switch (mutation) {
        .begin, .resolve, .acknowledge, .cleanup => if (writes.len != 0 or deletes.len != 0 or transforms.len != 0 or predicates.len != 0)
            return error.InvalidBatchRequest,
        .prepare => {},
    };
    if (split_checkpoint != null and split_checkpoint.?.kind == .source_ack and
        (writes.len != 0 or deletes.len != 0 or transforms.len != 0 or
            split_replication != null))
    {
        return error.InvalidBatchRequest;
    }

    return .{
        .writes = writes,
        .deletes = deletes,
        .relational_identity_rewrites = relational_identity_rewrites,
        .transforms = transforms,
        .predicates = predicates,
        .transaction_participants = transaction_participants,
        .split_checkpoint_range_start = checkpoint_start,
        .split_checkpoint_range_end = checkpoint_end,
        .split_transition_key = transition_key,
        .req = .{
            .writes = writes,
            .deletes = deletes,
            .relational_identity_rewrites = relational_identity_rewrites,
            .transforms = transforms,
            .predicates = predicates,
            .sync_level = sync_level,
            .split_checkpoint = split_checkpoint,
            .split_replication = split_replication,
            .split_transition = split_transition,
            .transaction = transaction,
        },
    };
}

pub fn encodeBatchResponse(alloc: std.mem.Allocator, result: BatchResult) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{{\"status\":{f},\"inserted\":{d},\"deleted\":{d},\"transformed\":{d}}}", .{
        std.json.fmt(result.status, .{}),
        result.inserted,
        result.deleted,
        result.transformed,
    });
}

pub fn encodeBatchRequest(alloc: std.mem.Allocator, req: db_mod.types.BatchRequest) ![]u8 {
    if (req.graph_writes.len > 0 or req.graph_deletes.len > 0 or (req.predicates.len > 0 and req.transaction == null)) {
        return error.UnsupportedBatchRequestEncoding;
    }
    if (req.split_transition != null and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
            req.predicates.len != 0 or req.split_checkpoint != null or req.split_replication != null or req.transaction != null))
    {
        return error.InvalidBatchRequest;
    }
    if (req.split_checkpoint != null and req.split_checkpoint.?.kind == .source_ack and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
            req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.predicates.len != 0 or
            req.split_replication != null))
    {
        return error.InvalidBatchRequest;
    }

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"inserts\":{");
    for (req.writes, 0..) |write, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(write.key, .{})});
        try writer.writeAll(write.value);
    }
    try writer.writeAll("},\"deletes\":[");
    for (req.deletes, 0..) |key, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}", .{std.json.fmt(key, .{})});
    }
    try writer.writeAll("]");
    if (req.relational_identity_rewrites.len > 0) {
        try writer.writeAll(",\"relational_identity_rewrites\":[");
        for (req.relational_identity_rewrites, 0..) |rewrite, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"old_key\":{f},\"new_key\":{f},\"value\":", .{
                std.json.fmt(rewrite.old_key, .{}),
                std.json.fmt(rewrite.new_key, .{}),
            });
            try writer.writeAll(rewrite.value);
            try writer.writeByte('}');
        }
        try writer.writeAll("]");
    }
    if (req.transforms.len > 0) {
        try writer.writeAll(",\"transforms\":[");
        for (req.transforms, 0..) |transform, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"key\":{f},\"operations\":[", .{std.json.fmt(transform.key, .{})});
            for (transform.operations, 0..) |op, op_index| {
                if (op_index != 0) try writer.writeByte(',');
                try writer.print("{{\"op\":{f},\"path\":{f}", .{
                    std.json.fmt(db_mod.transform.transformOpText(op.op), .{}),
                    std.json.fmt(op.path, .{}),
                });
                if (op.value_json) |value_json| {
                    try writer.writeAll(",\"value\":");
                    try writer.writeAll(value_json);
                }
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
            if (transform.upsert) try writer.writeAll(",\"upsert\":true");
            try writer.writeByte('}');
        }
        try writer.writeAll("]");
    }
    if (req.predicates.len > 0) {
        try writer.writeAll(",\"_predicates\":[");
        for (req.predicates, 0..) |predicate, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"key\":{f},\"expected_version\":\"{d}\"}}", .{
                std.json.fmt(predicate.key, .{}), predicate.expected_version,
            });
        }
        try writer.writeByte(']');
    }
    if (req.split_checkpoint) |checkpoint| {
        try writer.print(",\"_split_checkpoint\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"source_group_id\":\"{d}\",\"destination_group_id\":\"{d}\",\"range_start\":{f},\"range_end\":{f},\"delta_sequence\":\"{d}\"}}", .{
            std.json.fmt(@tagName(checkpoint.kind), .{}),
            checkpoint.transition_id,
            checkpoint.attempt_epoch,
            checkpoint.source_group_id,
            checkpoint.destination_group_id,
            std.json.fmt(checkpoint.range_start, .{}),
            std.json.fmt(checkpoint.range_end, .{}),
            checkpoint.delta_sequence,
        });
    }
    if (req.split_replication) |replication| {
        try writer.print(",\"_split_replication\":{{\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"source_group_id\":\"{d}\",\"destination_group_id\":\"{d}\",\"namespace_table_id\":\"{d}\",\"namespace_shard_id\":\"{d}\",\"namespace_range_id\":\"{d}\",\"operation\":{f},\"sequence\":\"{d}\"", .{
            replication.transition_id,
            replication.attempt_epoch,
            replication.source_group_id,
            replication.destination_group_id,
            replication.identity_namespace.table_id,
            replication.identity_namespace.shard_id,
            replication.identity_namespace.range_id,
            std.json.fmt(@tagName(replication.operation), .{}),
            replication.sequence,
        });
        if (replication.bootstrap_sequence) |sequence| {
            try writer.print(",\"bootstrap_sequence\":\"{d}\"", .{sequence});
        }
        try writer.writeByte('}');
    }
    if (req.split_transition) |transition| {
        try writer.print(",\"_split_transition\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"destination_group_id\":\"{d}\",\"split_key\":{f}}}", .{
            std.json.fmt(@tagName(transition.kind), .{}),
            transition.transition_id,
            transition.attempt_epoch,
            transition.destination_group_id,
            std.json.fmt(transition.split_key, .{}),
        });
    }
    if (req.transaction) |mutation| {
        try writer.writeAll(",\"_transaction\":{");
        switch (mutation) {
            .begin => |begin| {
                const txn_hex = std.fmt.bytesToHex(begin.txn_id, .lower);
                try writer.print("\"phase\":\"begin\",\"txn_id\":\"{s}\",\"begin_timestamp\":\"{d}\",\"created_at_ns\":\"{d}\",\"topology_epoch\":\"{d}\",\"retain_terminal\":{},\"participants\":[", .{
                    &txn_hex, begin.begin_timestamp, begin.created_at_ns, begin.topology_epoch, begin.retain_terminal,
                });
                for (begin.participants, 0..) |participant, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{f}", .{std.json.fmt(participant, .{})});
                }
                try writer.writeByte(']');
            },
            .prepare => |prepare| {
                const txn_hex = std.fmt.bytesToHex(prepare.txn_id, .lower);
                try writer.print("\"phase\":\"prepare\",\"txn_id\":\"{s}\",\"topology_epoch\":\"{d}\"", .{ &txn_hex, prepare.topology_epoch });
            },
            .resolve => |resolve| {
                const txn_hex = std.fmt.bytesToHex(resolve.txn_id, .lower);
                try writer.print("\"phase\":\"resolve\",\"txn_id\":\"{s}\",\"status\":{f},\"commit_version\":\"{d}\"", .{
                    &txn_hex, std.json.fmt(@tagName(resolve.status), .{}), resolve.commit_version,
                });
            },
            .acknowledge => |ack| {
                const txn_hex = std.fmt.bytesToHex(ack.txn_id, .lower);
                try writer.print("\"phase\":\"acknowledge\",\"txn_id\":\"{s}\",\"participant\":{f}", .{
                    &txn_hex, std.json.fmt(ack.participant, .{}),
                });
            },
            .cleanup => |cleanup| {
                const txn_hex = std.fmt.bytesToHex(cleanup.txn_id, .lower);
                try writer.print("\"phase\":\"cleanup\",\"txn_id\":\"{s}\",\"cutoff_timestamp\":\"{d}\",\"retained_cutoff_timestamp\":\"{d}\"", .{
                    &txn_hex, cleanup.cutoff_timestamp, cleanup.retained_cutoff_timestamp,
                });
            },
        }
        try writer.writeByte('}');
    }
    try writer.print(",\"sync_level\":\"{s}\"}}", .{syncLevelName(req.sync_level)});
    return try out.toOwnedSlice();
}

fn syncLevelName(sync_level: db_mod.types.SyncLevel) []const u8 {
    return switch (sync_level) {
        .propose => "propose",
        .write => "write",
        .full_text => "query",
        .enrichments => "enrichments",
        .full_index => "full_index",
    };
}

fn parseInternalU64(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else error.InvalidBatchRequest,
        .number_string, .string => |text| blk: {
            if (text.len == 0) break :blk error.InvalidBatchRequest;
            for (text) |byte| if (byte < '0' or byte > '9') break :blk error.InvalidBatchRequest;
            break :blk std.fmt.parseUnsigned(u64, text, 10) catch error.InvalidBatchRequest;
        },
        else => error.InvalidBatchRequest,
    };
}

fn parseInserts(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.BatchWrite {
    if (value != .object) return error.InvalidBatchRequest;
    const inserts = value.object;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, inserts.count());
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    var it = inserts.iterator();
    while (it.next()) |entry| {
        writes[initialized] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{}),
        };
        initialized += 1;
    }
    return writes;
}

fn parseDeletes(alloc: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const deletes = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (deletes[0..initialized]) |key| alloc.free(key);
        alloc.free(deletes);
    }
    for (values) |item| {
        if (item != .string) return error.InvalidBatchRequest;
        deletes[initialized] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return deletes;
}

fn parseRelationalIdentityRewrites(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.RelationalIdentityRewrite {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const rewrites = try alloc.alloc(db_mod.types.RelationalIdentityRewrite, values.len);
    var initialized: usize = 0;
    errdefer {
        freeRelationalIdentityRewrites(alloc, rewrites[0..initialized]);
        alloc.free(rewrites);
    }
    for (values) |item| {
        if (item != .object) return error.InvalidBatchRequest;
        const old_key_value = item.object.get("old_key") orelse return error.InvalidBatchRequest;
        if (old_key_value != .string) return error.InvalidBatchRequest;
        const new_key_value = item.object.get("new_key") orelse return error.InvalidBatchRequest;
        if (new_key_value != .string) return error.InvalidBatchRequest;
        const value_json = item.object.get("value") orelse return error.InvalidBatchRequest;
        const old_key = try alloc.dupe(u8, old_key_value.string);
        errdefer alloc.free(old_key);
        const new_key = try alloc.dupe(u8, new_key_value.string);
        errdefer alloc.free(new_key);
        const owned_value = try std.json.Stringify.valueAlloc(alloc, value_json, .{});
        errdefer alloc.free(owned_value);
        rewrites[initialized] = .{
            .old_key = old_key,
            .new_key = new_key,
            .value = owned_value,
        };
        initialized += 1;
    }
    return rewrites;
}

fn parseTransforms(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.DocumentTransform {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const transforms = try alloc.alloc(db_mod.types.DocumentTransform, values.len);
    var initialized: usize = 0;
    errdefer {
        freeTransforms(alloc, transforms[0..initialized]);
        alloc.free(transforms);
    }

    for (values) |item| {
        if (item != .object) return error.InvalidBatchRequest;
        const key_value = item.object.get("key") orelse return error.InvalidBatchRequest;
        if (key_value != .string) return error.InvalidBatchRequest;
        const operations_value = item.object.get("operations") orelse return error.InvalidBatchRequest;
        const operations = try parseTransformOps(alloc, operations_value);
        errdefer freeTransformOps(alloc, operations);
        const key = try alloc.dupe(u8, key_value.string);
        errdefer alloc.free(key);
        const upsert = if (item.object.get("upsert")) |upsert_value| blk: {
            if (upsert_value == .null) break :blk false;
            if (upsert_value != .bool) return error.InvalidBatchRequest;
            break :blk upsert_value.bool;
        } else false;

        transforms[initialized] = .{
            .key = key,
            .operations = operations,
            .upsert = upsert,
        };
        initialized += 1;
    }
    return transforms;
}

fn parseTransformOps(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.TransformOp {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const ops = try alloc.alloc(db_mod.types.TransformOp, values.len);
    var initialized: usize = 0;
    errdefer {
        freeTransformOpElements(alloc, ops[0..initialized]);
        alloc.free(ops);
    }

    for (values) |item| {
        if (item != .object) return error.InvalidBatchRequest;
        const op_value = item.object.get("op") orelse return error.InvalidBatchRequest;
        if (op_value != .string) return error.InvalidBatchRequest;
        const path_value = item.object.get("path") orelse return error.InvalidBatchRequest;
        if (path_value != .string) return error.InvalidBatchRequest;
        const op = try transformOpTypeFromString(op_value.string);
        const value_json = if (item.object.get("value")) |raw| try std.json.Stringify.valueAlloc(alloc, raw, .{}) else null;
        errdefer if (value_json) |json| alloc.free(json);
        const path = try alloc.dupe(u8, path_value.string);
        errdefer alloc.free(path);
        const parsed_op: db_mod.types.TransformOp = .{
            .op = op,
            .path = path,
            .value_json = value_json,
        };
        db_mod.transform.validateDocumentTransform(alloc, .{
            .key = "",
            .operations = @constCast((&[_]db_mod.types.TransformOp{parsed_op})[0..]),
        }) catch return error.InvalidBatchRequest;
        ops[initialized] = parsed_op;
        initialized += 1;
    }
    return ops;
}

fn transformOpTypeFromString(op: []const u8) !db_mod.types.TransformOpType {
    if (std.mem.eql(u8, op, "$set")) return .set;
    if (std.mem.eql(u8, op, "$setOnInsert")) return .set_on_insert;
    if (std.mem.eql(u8, op, "$unset")) return .unset;
    if (std.mem.eql(u8, op, "$inc")) return .inc;
    if (std.mem.eql(u8, op, "$push")) return .push;
    if (std.mem.eql(u8, op, "$addToSet")) return .add_to_set;
    if (std.mem.eql(u8, op, "$max")) return .max;
    return error.InvalidBatchRequest;
}

fn syncLevelFromValue(value: std.json.Value) !db_mod.types.SyncLevel {
    if (value != .string) return error.InvalidBatchRequest;
    const level = value.string;
    if (std.mem.eql(u8, level, "propose")) return .propose;
    if (std.mem.eql(u8, level, "write")) return .write;
    if (std.mem.eql(u8, level, "query")) return .full_text;
    if (std.mem.eql(u8, level, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, level, "full_index")) return .full_index;
    return error.InvalidBatchRequest;
}

fn freeWrites(alloc: std.mem.Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(writes);
}

fn freeDeletes(alloc: std.mem.Allocator, deletes: [][]const u8) void {
    for (deletes) |key| alloc.free(key);
    if (deletes.len > 0) alloc.free(deletes);
}

fn freeRelationalIdentityRewrites(alloc: std.mem.Allocator, rewrites: []db_mod.types.RelationalIdentityRewrite) void {
    for (rewrites) |rewrite| {
        alloc.free(@constCast(rewrite.old_key));
        alloc.free(@constCast(rewrite.new_key));
        alloc.free(@constCast(rewrite.value));
    }
    if (rewrites.len > 0) alloc.free(rewrites);
}

fn freeTransforms(alloc: std.mem.Allocator, transforms: []db_mod.types.DocumentTransform) void {
    for (transforms) |transform| {
        alloc.free(@constCast(transform.key));
        freeTransformOps(alloc, transform.operations);
    }
}

fn freeTransformOps(alloc: std.mem.Allocator, ops: []const db_mod.types.TransformOp) void {
    freeTransformOpElements(alloc, ops);
    if (ops.len > 0) alloc.free(@constCast(ops));
}

fn freeTransformOpElements(alloc: std.mem.Allocator, ops: []const db_mod.types.TransformOp) void {
    for (ops) |op| {
        alloc.free(@constCast(op.path));
        if (op.value_json) |value_json| alloc.free(@constCast(value_json));
    }
}

test "batch parser accepts inserts and deletes" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha"}},"deletes":["doc:b"]}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owned.writes.len);
    try std.testing.expectEqual(@as(usize, 1), owned.deletes.len);
}

test "batch parser preserves oversized value errors" {
    const body =
        \\{"inserts":{"doc:a":{"raw_payload":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}
    ;
    try std.testing.expectError(error.ValueTooLong, parseBatchRequestWithOptions(std.testing.allocator, body, .{ .allocate = .alloc_always, .max_value_len = 64 }, false));
}

test "internal batch parser owns and round trips split checkpoint" {
    const body =
        \\{"inserts":{},"deletes":[],"_split_checkpoint":{"kind":"destination_complete","transition_id":40,"attempt_epoch":1,"source_group_id":41,"destination_group_id":42,"range_start":"doc:m","range_end":"doc:z","delta_sequence":7}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

    var owned = try parseInternalBatchRequest(std.testing.allocator, body);
    defer owned.deinit(std.testing.allocator);
    const checkpoint = owned.req.split_checkpoint orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(db_mod.types.SplitReplicationCheckpoint.Kind.destination_complete, checkpoint.kind);
    try std.testing.expectEqual(@as(u64, 40), checkpoint.transition_id);
    try std.testing.expectEqual(@as(u64, 41), checkpoint.source_group_id);
    try std.testing.expectEqual(@as(u64, 42), checkpoint.destination_group_id);
    try std.testing.expectEqualStrings("doc:m", checkpoint.range_start);
    try std.testing.expectEqualStrings("doc:z", checkpoint.range_end);
    try std.testing.expectEqual(@as(u64, 7), checkpoint.delta_sequence);

    const encoded = try encodeBatchRequest(std.testing.allocator, owned.req);
    defer std.testing.allocator.free(encoded);
    var reparsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
    defer reparsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 7), reparsed.req.split_checkpoint.?.delta_sequence);
}

test "internal batch parser requires source acknowledgements to be metadata-only" {
    const mixed =
        \\{"inserts":{"doc:m":{}},"_split_checkpoint":{"kind":"source_ack","transition_id":40,"attempt_epoch":1,"source_group_id":41,"destination_group_id":42,"delta_sequence":7}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(std.testing.allocator, mixed));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, .{
        .writes = &.{.{ .key = "doc:m", .value = "{}" }},
        .split_checkpoint = .{
            .kind = .source_ack,
            .transition_id = 40,
            .attempt_epoch = 1,
            .source_group_id = 41,
            .destination_group_id = 42,
            .delta_sequence = 7,
        },
    }));
}

test "internal batch parser rejects public split replication identity" {
    const body =
        \\{"inserts":{"doc:m":{}},"_split_replication":{"transition_id":40,"attempt_epoch":1,"source_group_id":41,"destination_group_id":42,"namespace_table_id":7,"namespace_shard_id":41,"namespace_range_id":4100,"operation":"bootstrap_chunk","sequence":0}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

    var owned = try parseInternalBatchRequest(std.testing.allocator, body);
    defer owned.deinit(std.testing.allocator);
    const replication = owned.req.split_replication orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 40), replication.transition_id);
    try std.testing.expectEqual(@as(u64, 1), replication.attempt_epoch);
    try std.testing.expectEqual(@as(u64, 41), replication.source_group_id);
    try std.testing.expectEqual(@as(u64, 42), replication.destination_group_id);
    try std.testing.expectEqual(@as(u64, 4100), replication.identity_namespace.range_id);
}

test "internal batch split identity round trips the full u64 id space" {
    const max = std.math.maxInt(u64);
    const encoded = try encodeBatchRequest(std.testing.allocator, .{
        .writes = &.{.{ .key = "doc:m", .value = "{}" }},
        .split_replication = .{
            .transition_id = max - 5,
            .attempt_epoch = 1,
            .source_group_id = max - 4,
            .destination_group_id = max - 3,
            .identity_namespace = .{
                .table_id = max,
                .shard_id = max - 1,
                .range_id = max - 2,
            },
        },
    });
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"namespace_table_id\":\"18446744073709551615\"") != null);

    var parsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
    defer parsed.deinit(std.testing.allocator);
    const replication = parsed.req.split_replication orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(max - 5, replication.transition_id);
    try std.testing.expectEqual(max - 4, replication.source_group_id);
    try std.testing.expectEqual(max - 3, replication.destination_group_id);
    try std.testing.expectEqual(max, replication.identity_namespace.table_id);
    try std.testing.expectEqual(max - 1, replication.identity_namespace.shard_id);
    try std.testing.expectEqual(max - 2, replication.identity_namespace.range_id);
}

test "internal batch parser owns and round trips split transition" {
    const body =
        \\{"_split_transition":{"kind":"start","transition_id":40,"attempt_epoch":1,"destination_group_id":42,"split_key":"doc:m"}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

    var owned = try parseInternalBatchRequest(std.testing.allocator, body);
    defer owned.deinit(std.testing.allocator);
    const transition = owned.req.split_transition orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(db_mod.types.SplitTransitionMutation.Kind.start, transition.kind);
    try std.testing.expectEqual(@as(u64, 40), transition.transition_id);
    try std.testing.expectEqual(@as(u64, 1), transition.attempt_epoch);
    try std.testing.expectEqual(@as(u64, 42), transition.destination_group_id);
    try std.testing.expectEqualStrings("doc:m", transition.split_key);

    const encoded = try encodeBatchRequest(std.testing.allocator, owned.req);
    defer std.testing.allocator.free(encoded);
    var reparsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
    defer reparsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.SplitTransitionMutation.Kind.start, reparsed.req.split_transition.?.kind);
}

test "internal batch codec round trips replicated transaction phases" {
    const alloc = std.testing.allocator;
    const txn_id: db_mod.types.TxnId = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    const encoded = try encodeBatchRequest(alloc, .{
        .writes = &.{.{ .key = "doc:a", .value = "{}" }},
        .predicates = &.{.{ .key = "doc:a", .expected_version = 41 }},
        .transaction = .{ .prepare = .{ .txn_id = txn_id, .topology_epoch = 7 } },
    });
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));

    var decoded = try parseInternalBatchRequest(alloc, encoded);
    defer decoded.deinit(alloc);
    const prepare = switch (decoded.req.transaction orelse return error.TestExpectedEqual) {
        .prepare => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(txn_id, prepare.txn_id);
    try std.testing.expectEqual(@as(u64, 7), prepare.topology_epoch);
    try std.testing.expectEqual(@as(usize, 1), decoded.req.writes.len);
    try std.testing.expectEqual(@as(u64, 41), decoded.req.predicates[0].expected_version);

    const begin_encoded = try encodeBatchRequest(alloc, .{
        .transaction = .{ .begin = .{
            .txn_id = txn_id,
            .begin_timestamp = 42,
            .created_at_ns = 43,
            .topology_epoch = 7,
            .retain_terminal = true,
            .participants = &.{"table2:4:docs:group:7"},
        } },
    });
    defer alloc.free(begin_encoded);
    var begin_decoded = try parseInternalBatchRequest(alloc, begin_encoded);
    defer begin_decoded.deinit(alloc);
    const begin = switch (begin_decoded.req.transaction.?) {
        .begin => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(begin.retain_terminal);
    try std.testing.expectEqualStrings("table2:4:docs:group:7", begin.participants[0]);

    const ack_encoded = try encodeBatchRequest(alloc, .{
        .transaction = .{ .acknowledge = .{
            .txn_id = txn_id,
            .participant = "table2:4:docs:group:8",
        } },
    });
    defer alloc.free(ack_encoded);
    var ack_decoded = try parseInternalBatchRequest(alloc, ack_encoded);
    defer ack_decoded.deinit(alloc);
    const ack = switch (ack_decoded.req.transaction.?) {
        .acknowledge => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("table2:4:docs:group:8", ack.participant);

    const cleanup_encoded = try encodeBatchRequest(alloc, .{
        .transaction = .{ .cleanup = .{
            .txn_id = txn_id,
            .cutoff_timestamp = 100,
            .retained_cutoff_timestamp = 50,
        } },
    });
    defer alloc.free(cleanup_encoded);
    var cleanup_decoded = try parseInternalBatchRequest(alloc, cleanup_encoded);
    defer cleanup_decoded.deinit(alloc);
    const cleanup = switch (cleanup_decoded.req.transaction.?) {
        .cleanup => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(u64, 100), cleanup.cutoff_timestamp);
    try std.testing.expectEqual(@as(u64, 50), cleanup.retained_cutoff_timestamp);
}

test "internal batch parser rejects mixed split transition commands" {
    const body =
        \\{"inserts":{"doc:m":{}},"_split_transition":{"kind":"prepare","transition_id":40,"attempt_epoch":1,"destination_group_id":42,"split_key":"doc:m"}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(std.testing.allocator, body));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, .{
        .writes = &.{.{ .key = "doc:m", .value = "{}" }},
        .split_transition = .{
            .kind = .prepare,
            .transition_id = 40,
            .attempt_epoch = 1,
            .destination_group_id = 42,
            .split_key = "doc:m",
        },
    }));
}

test "batch parser accepts raw payload value under public request cap" {
    const alloc = std.testing.allocator;
    const payload = try alloc.alloc(u8, 6 * 1024 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');

    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"inserts\":{\"doc:a\":{\"raw_payload\":\"");
    try writer.writeAll(payload);
    try writer.writeAll("\"}}}");

    var owned = try parseBatchRequest(alloc, out.written());
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), owned.writes.len);
    try std.testing.expect(std.mem.indexOf(u8, owned.writes[0].value, "\"raw_payload\"") != null);
}

test "batch parser accepts public sync levels" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha"}},"sync_level":"full_index"}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, owned.req.sync_level);
}

test "batch parser rejects removed aknn sync level" {
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha"}},"sync_level":"aknn"}
    ));
}

test "batch parser accepts transforms" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"transforms":[{"key":"doc:a","operations":[{"op":"$max","path":"version","value":3},{"op":"$set","path":"status","value":"updated"}],"upsert":true}]}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owned.transforms.len);
    try std.testing.expect(owned.transforms[0].upsert);
    try std.testing.expectEqual(db_mod.types.TransformOpType.max, owned.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("version", owned.transforms[0].operations[0].path);
}

test "batch parser and encoder preserve relational identity rewrites" {
    const alloc = std.testing.allocator;
    var owned = try parseBatchRequest(alloc,
        \\{"relational_identity_rewrites":[{"old_key":"doc:a","new_key":"doc:z","value":{"id":"z","status":"renamed"}}],"sync_level":"write"}
    );
    defer owned.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), owned.relational_identity_rewrites.len);
    try std.testing.expectEqualStrings("doc:a", owned.relational_identity_rewrites[0].old_key);
    try std.testing.expectEqualStrings("doc:z", owned.relational_identity_rewrites[0].new_key);
    try std.testing.expect(std.mem.indexOf(u8, owned.relational_identity_rewrites[0].value, "\"renamed\"") != null);
    try std.testing.expectEqual(@as(usize, 1), owned.req.relational_identity_rewrites.len);
    try std.testing.expectEqual(db_mod.types.SyncLevel.write, owned.req.sync_level);

    const encoded = try encodeBatchRequest(alloc, owned.req);
    defer alloc.free(encoded);
    var reparsed = try parseBatchRequest(alloc, encoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), reparsed.relational_identity_rewrites.len);
    try std.testing.expectEqualStrings("doc:a", reparsed.relational_identity_rewrites[0].old_key);
    try std.testing.expectEqualStrings("doc:z", reparsed.relational_identity_rewrites[0].new_key);
    try std.testing.expect(std.mem.indexOf(u8, reparsed.relational_identity_rewrites[0].value, "\"renamed\"") != null);
}

test "batch parser accepts supported Go transform op spelling" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"transforms":[{"key":"doc:a","operations":[{"op":"$addToSet","path":"tags","value":"zig"},{"op":"$setOnInsert","path":"created_at","value":"now"}]}]}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owned.transforms.len);
    try std.testing.expectEqual(@as(usize, 2), owned.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.add_to_set, owned.transforms[0].operations[0].op);
    try std.testing.expectEqual(db_mod.types.TransformOpType.set_on_insert, owned.transforms[0].operations[1].op);
}

test "batch parser accepts push for graph edge arrays" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"transforms":[{"key":"doc:a","operations":[{"op":"$push","path":"edges","value":{"to":"doc:b"}}]}]}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.TransformOpType.push, owned.transforms[0].operations[0].op);
}

test "batch parser rejects every recognized but unsupported transform operator" {
    const unsupported = [_][]const u8{
        "$pull",
        "$pop",
        "$mul",
        "$min",
        "$currentDate",
        "$rename",
    };
    for (unsupported) |op| {
        const body = try std.fmt.allocPrint(
            std.testing.allocator,
            "{{\"transforms\":[{{\"key\":\"doc:missing\",\"operations\":[{{\"op\":\"{s}\",\"path\":\"field\",\"value\":1}}]}}]}}",
            .{op},
        );
        defer std.testing.allocator.free(body);
        try std.testing.expectError(
            error.InvalidBatchRequest,
            parseBatchRequest(std.testing.allocator, body),
        );
    }
}

test "batch parser safely rejects unsupported transform after initialized operations" {
    const body =
        \\{"transforms":[{"key":"doc:missing","operations":[{"op":"$set","path":"ready","value":true},{"op":"$pull","path":"items","value":1}]}]}
    ;
    for (0..32) |_| {
        try std.testing.expectError(
            error.InvalidBatchRequest,
            parseBatchRequest(std.testing.allocator, body),
        );
    }
}

test "batch parser preserves packed embeddings for mapper extraction" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","_embeddings":{"dense_idx":"AACAPwAAAEAAAEBA","sparse_idx":{"packed_indices":"AQAAAAUAAAA=","packed_values":"AAAAPwAAQD8="}}}}}
    );
    defer owned.deinit(std.testing.allocator);

    var extracted = try document_mapper.extractWrite(std.testing.allocator, owned.writes[0].key, owned.writes[0].value);
    defer extracted.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
    try std.testing.expectEqual(@as(usize, 1), extracted.sparse_embeddings.len);
    try std.testing.expect(extracted.cleaned_value != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "_embeddings") == null);
}

test "batch parser accepts compact vdbbench-shaped embeddings batch" {
    const alloc = std.testing.allocator;
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"inserts\":{");
    for (0..500) |i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print(
            "\"key:{d}\":{{\"id\":{d},\"metadata\":{{\"source\":\"vdbbench\",\"ordinal\":{d}}},\"vec_data\":[0.1,0.2,0.3],\"_embeddings\":{{\"vec\":[0.1,0.2,0.3]}}}}",
            .{ i, i, i },
        );
    }
    try writer.writeAll("},\"sync_level\":\"write\"}");

    var owned = try parseBatchRequest(alloc, out.written());
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 500), owned.writes.len);
    try std.testing.expectEqual(db_mod.types.SyncLevel.write, owned.req.sync_level);

    var extracted = try document_mapper.extractWrite(alloc, owned.writes[0].key, owned.writes[0].value);
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
    try std.testing.expectEqualStrings("vec", extracted.dense_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 3), extracted.dense_embeddings[0].vector.len);
    try std.testing.expect(extracted.cleaned_value != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "\"vec_data\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "_embeddings") == null);
}
