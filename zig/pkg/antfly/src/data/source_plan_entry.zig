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

//! Frozen source-plan staging has its own activated Raft envelope. It is
//! never an empty document batch and never manufactures document history.
const std = @import("std");
const descriptor = @import("../storage/kernel_owner_descriptor.zig");
const local = @import("antfly_local_sources");
pub const wire = local.storage_source_plan_wire;
const completion = local.api_local_table_writes.table_write_source.history_resolution_wire.completion;
pub const required_protocol_version = @import("../common/data_raft_protocol.zig").batch_source_plan_protocol_version;
pub const max_bytes = 16 * 1024 * 1024;
pub fn rootFor(owner: completion.RoutedOwner) !wire.control.storage.Root {
    try owner.validate();
    return .{ .authority = .raft, .namespace = owner.namespace, .generation = owner.root_generation, .fence = owner.digest() };
}
pub const Entry = struct {
    table: []const u8,
    batch: ?u8 = null,
    protocol: u16 = required_protocol_version,
    storage_owner: descriptor.Descriptor,
    source_owner: completion.RoutedOwner,
    source_plan: wire.Command,
    pub fn validate(self: Entry) !void {
        if (self.batch != null or self.protocol != required_protocol_version or self.source_plan.version != 1) return error.UnsupportedRaftBatchProtocolVersion;
        if (self.table.len == 0 or self.table.len > 1024 or !std.unicode.utf8ValidateSlice(self.table) or std.mem.indexOfScalar(u8, self.table, 0) != null) return error.InvalidReplicatedBatch;
        const owner = self.storage_owner;
        if (owner.identity.table_id == 0 or owner.identity.shard_id == 0 or owner.identity.range_id == 0 or owner.schema_json.len > 1024 * 1024 or owner.indexes_json.len > 1024 * 1024) return error.InvalidReplicatedBatch;
        if (owner.restore != null or owner.restore_bootstrap_json.len != 0 or owner.initial_child_bootstrap_json.len != 0 or owner.restore_cancel_recovery or owner.restore_ha_replay) return error.InvalidReplicatedBatch;
        if (owner.table_storage) |storage| if (storage.engine != .local) return error.InvalidReplicatedBatch;
        if (owner.initial_range) |range| if (range.start.len > 64 * 1024 or range.end.len > 64 * 1024) return error.InvalidReplicatedBatch;
        const root = try rootFor(self.source_owner);
        if (!std.meta.eql(root, self.source_plan.root) or root.generation != owner.lsm_root_generation or std.mem.readInt(u64, root.namespace[0..8], .big) != owner.identity.table_id or std.mem.readInt(u64, root.namespace[8..16], .big) != owner.identity.shard_id or std.mem.readInt(u64, root.namespace[16..24], .big) != owner.identity.range_id) return error.HistoryResynchronizationRequired;
        try self.source_plan.spec.validate(root);
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
    var command = try wire.fromCommand(a, entry.source_plan);
    defer command.deinit();
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
    for ([_][]const u8{ "table", "batch", "protocol", "storage_owner", "source_owner", "source_plan" }) |name| if (!value.object.contains(name)) return error.InvalidReplicatedBatch;
    const owner = value.object.get("storage_owner").?;
    if (owner != .object) return error.InvalidReplicatedBatch;
    for ([_][]const u8{ "lsm_root_generation", "identity", "schema_json", "indexes_json", "initial_range", "table_storage" }) |name| if (!owner.object.contains(name)) return error.InvalidReplicatedBatch;
    var parsed = try std.json.parseFromValue(Entry, a, value, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    errdefer parsed.deinit();
    try parsed.value.validate();
    var command = try wire.fromCommand(a, parsed.value.source_plan);
    defer command.deinit();
    return .{ .parsed = parsed };
}

test "source plan entry requires activation and binds complete original owner" {
    const a = std.testing.allocator;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 11, .big);
    std.mem.writeInt(u64, namespace[16..24], 13, .big);
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = namespace, .table_binding = @splat(3), .root_generation = 4, .group_id = 601, .topology_epoch = 5 };
    const root = try rootFor(owner);
    var attempt: [16]u8 = @splat(1);
    std.mem.writeInt(u64, attempt[0..8], 1, .big);
    const hashes = [_]wire.control.Digest{@splat(4)};
    const entry: Entry = .{ .table = "docs", .storage_owner = .{ .lsm_root_generation = 4, .identity = .{ .table_id = 7, .shard_id = 11, .range_id = 13 }, .schema_json = "{}", .indexes_json = "{}" }, .source_owner = owner, .source_plan = .{ .version = 1, .root = root, .spec = .{ .namespace = wire.control.storage.namespaceFor(root, 1), .storage_epoch = 1, .binding = @splat(2), .domain = wire.control.storage.domainFor(@splat(2)), .attempt = attempt, .bytes = 64, .checksum = @splat(5), .plan_digest = @splat(6), .chunk_hashes = &hashes }, .action = .begin } };
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, encode(a, entry, required_protocol_version - 1));
    const bytes = try encode(a, entry, required_protocol_version);
    defer a.free(bytes);
    var owned = try decode(a, bytes);
    defer owned.deinit();
    try std.testing.expectEqualDeep(entry, owned.value());
    try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, @import("raft_batch.zig").decode(a, bytes));
    var applying = try @import("raft_batch.zig").decodeForApply(a, bytes);
    defer applying.deinit(a);
    try std.testing.expect(applying == .source_plan);
    var wrong = entry;
    wrong.source_owner.table_binding[0] ^= 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, encode(a, wrong, required_protocol_version));
    wrong = entry;
    wrong.storage_owner.lsm_root_generation += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, encode(a, wrong, required_protocol_version));
    const duplicate = try std.fmt.allocPrint(a, "{s},\"protocol\":36}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(duplicate);
    try std.testing.expectError(error.DuplicateField, @import("raft_batch.zig").decodeForApply(a, duplicate));
    const mixed = try std.fmt.allocPrint(a, "{s},\"history_control\":{{}}}}", .{bytes[0 .. bytes.len - 1]});
    defer a.free(mixed);
    try std.testing.expectError(error.UnknownField, @import("raft_batch.zig").decodeForApply(a, mixed));
}
