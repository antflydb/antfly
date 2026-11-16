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

//! Dedicated data-Raft retention entry. The owner must activate the complete
//! decoder/apply protocol across all applying members before proposal. Merely
//! providing this codec does not advertise or activate that capability.
const std = @import("std");
const descriptor = @import("../storage/kernel_owner_descriptor.zig");
const control = @import("antfly_local_sources").api_local_table_writes.table_write_source.history_consumer;
pub const required_protocol_version = @import("../common/data_raft_protocol.zig").batch_history_control_protocol_version;
pub fn requiredProtocol(command: control.Command) u16 {
    return switch (command) {
        .configure_policy, .inspect_policy => @import("../common/data_raft_protocol.zig").batch_history_policy_protocol_version,
        else => required_protocol_version,
    };
}
pub const max_bytes = 16 * 1024 * 1024;
pub const max_catalog_bytes = 1024 * 1024;
pub const max_range_key_bytes = 64 * 1024;
pub const Entry = struct {
    table: []const u8,
    /// Null deliberately poisons decoders which only understand row batches.
    batch: ?u8 = null,
    protocol: u16 = required_protocol_version,
    storage_owner: descriptor.Descriptor,
    history_control: struct { version: u8 = 1, command: control.Command },

    pub fn validate(self: Entry) !void {
        if (self.batch != null or self.protocol != requiredProtocol(self.history_control.command) or self.history_control.version != 1) return error.UnsupportedRaftBatchProtocolVersion;
        if (self.table.len == 0 or self.table.len > 1024 or !std.unicode.utf8ValidateSlice(self.table) or std.mem.indexOfScalar(u8, self.table, 0) != null) return error.InvalidReplicatedBatch;
        const owner = self.storage_owner;
        if (owner.identity.table_id == 0 or owner.identity.shard_id == 0 or owner.identity.range_id == 0 or owner.schema_json.len > max_catalog_bytes or owner.indexes_json.len > max_catalog_bytes) return error.InvalidReplicatedBatch;
        if (owner.restore != null or owner.restore_bootstrap_json.len != 0 or owner.initial_child_bootstrap_json.len != 0 or owner.restore_cancel_recovery or owner.restore_ha_replay) return error.InvalidReplicatedBatch;
        if (owner.table_storage) |storage| if (storage.engine != .local) return error.InvalidReplicatedBatch;
        if (owner.initial_range) |range| if (range.start.len > max_range_key_bytes or range.end.len > max_range_key_bytes) return error.InvalidReplicatedBatch;
        const namespace = self.history_control.command.namespace();
        if (std.mem.readInt(u64, namespace[0..8], .big) != owner.identity.table_id or std.mem.readInt(u64, namespace[8..16], .big) != owner.identity.shard_id or std.mem.readInt(u64, namespace[16..24], .big) != owner.identity.range_id) return error.HistoryResynchronizationRequired;
    }
};
pub const Owned = struct {
    parsed: std.json.Parsed(Entry),
    pub fn deinit(self: *Owned) void {
        self.parsed.deinit();
        self.* = undefined;
    }
    pub fn value(self: *const Owned) Entry {
        return self.parsed.value;
    }
};
pub fn encode(a: std.mem.Allocator, entry: Entry, activated_protocol: u16) ![]u8 {
    if (activated_protocol < requiredProtocol(entry.history_control.command)) return error.UnsupportedRaftBatchProtocolVersion;
    try entry.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, entry, .{ .emit_null_optional_fields = true });
    errdefer a.free(bytes);
    if (bytes.len > max_bytes) return error.InvalidReplicatedBatch;
    return bytes;
}
pub fn decode(a: std.mem.Allocator, bytes: []const u8) !Owned {
    if (bytes.len > max_bytes) return error.InvalidReplicatedBatch;
    var raw = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
    defer raw.deinit();
    return decodeValue(a, raw.value);
}
/// Reuse the applying owner's already parsed envelope. All strings in the
/// returned value are independently owned; the original parse may be released.
pub fn decodeValue(a: std.mem.Allocator, value: std.json.Value) !Owned {
    if (value != .object) return error.InvalidReplicatedBatch;
    for ([_][]const u8{ "table", "batch", "protocol", "storage_owner", "history_control" }) |name| if (!value.object.contains(name)) return error.InvalidReplicatedBatch;
    const control_value = value.object.get("history_control").?;
    if (control_value != .object or !control_value.object.contains("version") or !control_value.object.contains("command")) return error.InvalidReplicatedBatch;
    const owner_value = value.object.get("storage_owner").?;
    if (owner_value != .object) return error.InvalidReplicatedBatch;
    for ([_][]const u8{ "lsm_root_generation", "identity", "schema_json", "indexes_json", "initial_range", "table_storage" }) |name| if (!owner_value.object.contains(name)) return error.InvalidReplicatedBatch;
    var parsed = try std.json.parseFromValue(Entry, a, value, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return .{ .parsed = parsed };
}

test "history control entry binds bounded immutable owner data and requires activation" {
    const a = std.testing.allocator;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 11, .big);
    std.mem.writeInt(u64, namespace[16..24], 13, .big);
    const entry: Entry = .{ .table = "docs", .storage_owner = .{ .lsm_root_generation = 4, .identity = .{ .table_id = 7, .shard_id = 11, .range_id = 13 }, .schema_json = "{}", .indexes_json = "{}", .initial_range = .{ .start = "a", .end = "z" } }, .history_control = .{ .command = .{ .enroll = .{ .namespace = namespace, .authority = @splat(1), .sequence = 1, .consumer = @splat(2), .definition = @splat(3), .expires_at_seconds = 1000, .policy_revision = 1 } } } };
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, entry, required_protocol_version - 1));
    const bytes = try encode(a, entry, required_protocol_version);
    defer a.free(bytes);
    var owned = try decode(a, bytes);
    defer owned.deinit();
    try std.testing.expectEqualDeep(entry, owned.value());
    var wrong = entry;
    wrong.storage_owner.identity.range_id += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, encode(a, wrong, required_protocol_version));
    wrong = entry;
    wrong.batch = 1;
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, wrong, required_protocol_version));
    wrong = entry;
    wrong.history_control.version = 2;
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, wrong, required_protocol_version));
    const extra = try std.fmt.allocPrint(a, "{s},\"unexpected\":true}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(extra);
    try std.testing.expectError(error.UnknownField, decode(a, extra));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"table\":\"other\"}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.DuplicateField, decode(a, duplicate));
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, @import("raft_batch.zig").decode(a, bytes));
    {
        var applying = try @import("raft_batch.zig").decodeForApply(a, bytes);
        defer applying.deinit(a);
        try std.testing.expectEqualDeep(entry, applying.history_control.value());
        try std.testing.expectEqualStrings("docs", applying.history_control.value().table);
    }
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, @import("raft_batch.zig").decode(a, "{\"table\":\"docs\",\"batch\":{},\"history_control\":{}}"));
    {
        var raw = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
        defer raw.deinit();
        try std.testing.expect(raw.value.object.swapRemove("protocol"));
        const missing = try std.json.Stringify.valueAlloc(a, raw.value, .{});
        defer a.free(missing);
        try std.testing.expectError(error.InvalidReplicatedBatch, decode(a, missing));
    }
    for ([_][]const u8{ "schema_json", "indexes_json", "initial_range", "table_storage" }) |name| {
        var raw = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
        defer raw.deinit();
        try std.testing.expect(raw.value.object.getPtr("storage_owner").?.object.swapRemove(name));
        const missing = try std.json.Stringify.valueAlloc(a, raw.value, .{});
        defer a.free(missing);
        try std.testing.expectError(error.InvalidReplicatedBatch, decode(a, missing));
    }
    {
        var raw = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
        defer raw.deinit();
        try std.testing.expect(raw.value.object.getPtr("history_control").?.object.swapRemove("version"));
        const missing = try std.json.Stringify.valueAlloc(a, raw.value, .{});
        defer a.free(missing);
        try std.testing.expectError(error.InvalidReplicatedBatch, decode(a, missing));
    }
    const ref: control.Reference = .{ .namespace = namespace, .authority = @splat(1), .consumer = @splat(2), .epoch = 1, .grant = 1, .definition = @splat(3) };
    for ([_]control.Command{
        .{ .inspect = ref },
        .{ .acknowledge = .{ .reference = ref, .previous = 0, .next = 1 } },
        .{ .renew = .{ .reference = ref, .previous = 1000, .next = 2000 } },
        .{ .retire = .{ .namespace = namespace, .authority = ref.authority, .sequence = 2, .consumer = ref.consumer, .definition = ref.definition, .grant = ref.grant } },
        .{ .reclaim = .{ .reference = ref, .max_frames = 32 } },
        .{ .collect = .{ .namespace = namespace, .authority = ref.authority, .max_frames = 1 } },
    }) |command| {
        var next = entry;
        next.history_control.command = command;
        const encoded = try encode(a, next, required_protocol_version);
        defer a.free(encoded);
        var decoded = try decode(a, encoded);
        defer decoded.deinit();
        try std.testing.expectEqualDeep(next, decoded.value());
    }
    const oversized = try a.alloc(u8, max_bytes + 1);
    defer a.free(oversized);
    try std.testing.expectError(error.InvalidReplicatedBatch, decode(a, oversized));
    wrong = entry;
    wrong.storage_owner.schema_json = oversized[0 .. max_catalog_bytes + 1];
    try std.testing.expectError(error.InvalidReplicatedBatch, encode(a, wrong, required_protocol_version));
    var binary = entry;
    binary.storage_owner.lsm_root_generation = std.math.maxInt(u64);
    binary.storage_owner.identity = .{ .table_id = std.math.maxInt(u64), .shard_id = std.math.maxInt(u64) - 1, .range_id = std.math.maxInt(u64) - 2 };
    binary.storage_owner.initial_range = .{ .start = &.{ 0, 128, 255 }, .end = &.{ 1, 0, 255 } };
    var maximum_grant = entry.history_control.command.enroll;
    std.mem.writeInt(u64, maximum_grant.namespace[0..8], binary.storage_owner.identity.table_id, .big);
    std.mem.writeInt(u64, maximum_grant.namespace[8..16], binary.storage_owner.identity.shard_id, .big);
    std.mem.writeInt(u64, maximum_grant.namespace[16..24], binary.storage_owner.identity.range_id, .big);
    maximum_grant.sequence = std.math.maxInt(u64);
    maximum_grant.expires_at_seconds = std.math.maxInt(u64);
    maximum_grant.policy_revision = std.math.maxInt(u64);
    binary.history_control.command = .{ .enroll = maximum_grant };
    // No ordinary batch decoder, public or private, may silently accept the
    // poison null payload as an empty write.
    try std.testing.expectError(error.InvalidBatchRequest, @import("antfly_local_sources").api_internal_batch.parseInternalBatchRequest(a, "null"));
    for ([_]Entry{ entry, binary }) |candidate| try std.testing.checkAllAllocationFailures(a, struct {
        fn run(alloc: std.mem.Allocator, value: Entry) !void {
            const encoded = try encode(alloc, value, required_protocol_version);
            defer alloc.free(encoded);
            var result = try decode(alloc, encoded);
            defer result.deinit();
            try std.testing.expectEqualDeep(value, result.value());
            var applying = try @import("raft_batch.zig").decodeForApply(alloc, encoded);
            defer applying.deinit(alloc);
            try std.testing.expectEqualDeep(value, applying.history_control.value());
        }
    }.run, .{candidate});
}

test "history control entry root policy requires its complete activation floor" {
    const a = std.testing.allocator;
    const command: control.Command = .{ .configure_policy = .{ .namespace = @splat(1), .authority = @splat(2), .previous_revision = 0, .revision = 1, .limit = 16 * 1024 * 1024 } };
    var entry: Entry = .{ .table = "docs", .protocol = requiredProtocol(command), .storage_owner = .{ .lsm_root_generation = 0, .identity = .{ .table_id = 0x0101010101010101, .shard_id = 0x0101010101010101, .range_id = 0x0101010101010101 }, .schema_json = "{}", .indexes_json = "{}" }, .history_control = .{ .command = command } };
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, entry, required_protocol_version));
    const bytes = try encode(a, entry, requiredProtocol(command));
    defer a.free(bytes);
    var decoded = try decode(a, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(entry, decoded.value());
    entry.protocol = required_protocol_version;
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, entry.validate());
}

test "history control entry policy receipts retain strict bounded canonical encoding" {
    const ordered = @import("antfly_local_sources").api_local_table_writes.table_write_source.history_ordered;
    const record: ordered.Record = .{ .namespace = @splat(1), .entry = .{ .term = 1, .index = 2 }, .command = @splat(3), .outcome = .{ .applied = .{ .policy = .{ .revision = 1, .limit = 16 * 1024 * 1024 } } } };
    var bytes = try record.encode();
    try std.testing.expectEqualStrings("HCO2", bytes[0..4]);
    try std.testing.expectEqualDeep(record, try ordered.Record.decode(&bytes));
    bytes[152] = 1;
    std.crypto.hash.sha2.Sha256.hash(bytes[0..160], bytes[160..192], .{});
    try std.testing.expectError(error.HistoryCorrupt, ordered.Record.decode(&bytes));
    bytes = try record.encode();
    @memset(bytes[136..144], 0);
    std.crypto.hash.sha2.Sha256.hash(bytes[0..160], bytes[160..192], .{});
    try std.testing.expectError(error.HistoryCorrupt, ordered.Record.decode(&bytes));
    bytes = try record.encode();
    @memcpy(bytes[0..4], "HCO1");
    std.crypto.hash.sha2.Sha256.hash(bytes[0..160], bytes[160..192], .{});
    try std.testing.expectError(error.HistoryCorrupt, ordered.Record.decode(&bytes));
}
