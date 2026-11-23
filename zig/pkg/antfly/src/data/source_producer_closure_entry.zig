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

//! Physical producer intake closure is a distinct activated control entry.
//! Its actual Raft receipt closes legacy/unlabelled admission; it proves no
//! pending-transaction drainage and cannot activate a source scope.
const std = @import("std");
const descriptor = @import("../storage/kernel_owner_descriptor.zig");
const local = @import("antfly_local_sources");
const completion = local.api_local_table_writes.table_write_source.history_resolution_wire.completion;
pub const required_protocol_version = @import("../common/data_raft_protocol.zig").batch_source_producer_closure_protocol_version;
pub const max_bytes = 16 * 1024 * 1024;
pub const Entry = struct {
    table: []const u8,
    batch: ?u8 = null,
    protocol: u16 = required_protocol_version,
    storage_owner: descriptor.Descriptor,
    source_owner: completion.RoutedOwner,
    source_producer_closure: bool,
    pub fn validate(self: Entry) !void {
        if (self.batch != null or self.protocol != required_protocol_version or !self.source_producer_closure) return error.UnsupportedRaftBatchProtocolVersion;
        if (self.table.len == 0 or self.table.len > 1024 or !std.unicode.utf8ValidateSlice(self.table) or std.mem.indexOfScalar(u8, self.table, 0) != null) return error.InvalidReplicatedBatch;
        const owner = self.storage_owner;
        if (owner.identity.table_id == 0 or owner.identity.shard_id == 0 or owner.identity.range_id == 0 or owner.schema_json.len > 1024 * 1024 or owner.indexes_json.len > 1024 * 1024) return error.InvalidReplicatedBatch;
        if (owner.restore != null or owner.restore_bootstrap_json.len != 0 or owner.initial_child_bootstrap_json.len != 0 or owner.restore_cancel_recovery or owner.restore_ha_replay) return error.InvalidReplicatedBatch;
        if (owner.table_storage) |storage| if (storage.engine != .local) return error.InvalidReplicatedBatch;
        if (owner.initial_range) |range| if (range.start.len > 64 * 1024 or range.end.len > 64 * 1024) return error.InvalidReplicatedBatch;
        try self.source_owner.validate();
        if (self.source_owner.root_generation != owner.lsm_root_generation or std.mem.readInt(u64, self.source_owner.namespace[0..8], .big) != owner.identity.table_id or std.mem.readInt(u64, self.source_owner.namespace[8..16], .big) != owner.identity.shard_id or std.mem.readInt(u64, self.source_owner.namespace[16..24], .big) != owner.identity.range_id) return error.HistoryResynchronizationRequired;
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
pub fn encode(a: std.mem.Allocator, entry: Entry, activated: u16) ![]u8 {
    if (activated < required_protocol_version) return error.UnsupportedRaftBatchProtocolVersion;
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
pub fn decodeValue(a: std.mem.Allocator, value: std.json.Value) !Owned {
    if (value != .object) return error.InvalidReplicatedBatch;
    for ([_][]const u8{ "table", "batch", "protocol", "storage_owner", "source_owner", "source_producer_closure" }) |name| if (!value.object.contains(name)) return error.InvalidReplicatedBatch;
    const owner = value.object.get("storage_owner").?;
    if (owner != .object) return error.InvalidReplicatedBatch;
    for ([_][]const u8{ "lsm_root_generation", "identity", "schema_json", "indexes_json", "initial_range", "table_storage" }) |name| if (!owner.object.contains(name)) return error.InvalidReplicatedBatch;
    const routed = value.object.get("source_owner").?;
    if (routed != .object) return error.InvalidReplicatedBatch;
    for ([_][]const u8{ "metadata_group_id", "metadata_incarnation", "namespace", "table_binding", "root_generation", "group_id", "topology_epoch" }) |name| if (!routed.object.contains(name)) return error.InvalidReplicatedBatch;
    var parsed = try std.json.parseFromValue(Entry, a, value, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return .{ .parsed = parsed };
}

test "source producer closure entry requires activation and never becomes row work" {
    const a = std.testing.allocator;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 11, .big);
    std.mem.writeInt(u64, namespace[16..24], 13, .big);
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = namespace, .table_binding = @splat(3), .root_generation = 4, .group_id = 601, .topology_epoch = 5 };
    const entry: Entry = .{ .table = "docs", .storage_owner = .{ .lsm_root_generation = 4, .identity = .{ .table_id = 7, .shard_id = 11, .range_id = 13 }, .schema_json = "{}", .indexes_json = "{}" }, .source_owner = owner, .source_producer_closure = true };
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, entry, required_protocol_version - 1));
    const bytes = try encode(a, entry, required_protocol_version);
    defer a.free(bytes);
    var roundtrip = try decode(a, bytes);
    defer roundtrip.deinit();
    try std.testing.expectEqualDeep(entry, roundtrip.value());
    const batch = @import("raft_batch.zig");
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, batch.decode(a, bytes));
    var applying = try batch.decodeForApply(a, bytes);
    defer applying.deinit(a);
    try std.testing.expect(applying == .source_producer_closure);
    var wrong = entry;
    wrong.source_owner.root_generation += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, encode(a, wrong, required_protocol_version));
    wrong = entry;
    wrong.source_producer_closure = false;
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, wrong, required_protocol_version));
    wrong = entry;
    wrong.protocol -= 1;
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, wrong, required_protocol_version));
    var stripped = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer stripped.deinit();
    _ = stripped.value.object.swapRemove("source_producer_closure");
    const missing_tag = try std.json.Stringify.valueAlloc(a, stripped.value, .{});
    defer a.free(missing_tag);
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, batch.decodeForApply(a, missing_tag));
    _ = stripped.value.object.getPtr("source_owner").?.object.swapRemove("root_generation");
    try stripped.value.object.put(stripped.arena.allocator(), "source_producer_closure", .{ .bool = true });
    const missing_generation = try std.json.Stringify.valueAlloc(a, stripped.value, .{});
    defer a.free(missing_generation);
    try std.testing.expectError(error.InvalidReplicatedBatch, batch.decodeForApply(a, missing_generation));
    const mixed = try std.fmt.allocPrint(a, "{s},\"history_control\":{{}}}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(mixed);
    try std.testing.expectError(error.UnknownField, batch.decodeForApply(a, mixed));
}

test "history control entry producer closure observations bind original root and native cut" {
    const a = std.testing.allocator;
    const policy = local.storage_source_producer_policy;
    const scope = local.storage_source_scope;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 11, .big);
    std.mem.writeInt(u64, namespace[16..24], 13, .big);
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = namespace, .table_binding = @splat(3), .root_generation = 4, .group_id = 601, .topology_epoch = 5 };
    const query: policy.Query = .{ .version = 1, .owner = owner, .minimum_applied_index = 9 };
    const absent: policy.Observation = .{ .version = 1, .owner = owner, .cut = .{ .term = 2, .index = 10 }, .closure = null };
    const bytes = try policy.encodeObservation(a, query, absent);
    defer a.free(bytes);
    try std.testing.expect((try policy.decodeObservation(a, bytes, query)).closure == null);
    var present = absent;
    present.closure = .{ .root = try scope.Basis.fromOwner(owner), .first_closed_at = .{ .term = 1, .index = 9 } };
    const closed_bytes = try policy.encodeObservation(a, query, present);
    defer a.free(closed_bytes);
    try std.testing.expectEqualDeep(present, try policy.decodeObservation(a, closed_bytes, query));
    var changed = query;
    changed.owner.root_generation += 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, policy.decodeObservation(a, closed_bytes, changed));
    changed = query;
    changed.minimum_applied_index = 11;
    try std.testing.expectError(error.SourceAdmissionCorrupt, policy.decodeObservation(a, closed_bytes, changed));
    var forged = present;
    forged.closure.?.first_closed_at.index = 11;
    try std.testing.expectError(error.SourceAdmissionCorrupt, policy.encodeObservation(a, query, forged));
    forged = present;
    forged.closure.?.root.root_generation += 1;
    try std.testing.expectError(error.SourceAdmissionCorrupt, policy.encodeObservation(a, query, forged));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"closure\":null}}", .{closed_bytes[0 .. closed_bytes.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.SourceAdmissionCorrupt, policy.decodeObservation(a, duplicate, query));
}
