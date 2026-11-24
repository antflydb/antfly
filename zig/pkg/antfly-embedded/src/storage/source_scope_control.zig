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

//! Trusted source-scope controls. Metadata qualification and first-bind legacy
//! drainage happen at the owner boundary, never from a caller-supplied boolean.
//! Reducer writes and the real ordered apply marker share one native transaction.
const std = @import("std");
pub const scope = @import("source_scope.zig");
const completion = @import("transaction_completion.zig");
const A = std.mem.Allocator;
pub const max_bytes = 8192;
pub const FenceAttempt = struct {
    reference: scope.Reference,
    transaction_id: [16]u8,
    reservation: scope.Digest,
};
pub const CheckpointBytes = struct {
    const capacity = @import("source_checkpoint.zig").max_record_bytes;
    const encoded_capacity = std.base64.standard.Encoder.calcSize(capacity);
    bytes: [capacity]u8 = @splat(0),
    len: usize = 0,

    pub fn fromSlice(raw: []const u8) !CheckpointBytes {
        if (raw.len == 0 or raw.len > capacity) return error.InvalidSourceAdmission;
        var result: CheckpointBytes = .{ .len = raw.len };
        @memcpy(result.bytes[0..raw.len], raw);
        return result;
    }

    pub fn slice(self: *const CheckpointBytes) []const u8 {
        return self.bytes[0..self.len];
    }

    pub fn jsonStringify(self: CheckpointBytes, stream: anytype) !void {
        var buffer: [encoded_capacity]u8 = undefined;
        try stream.write(std.base64.standard.Encoder.encode(&buffer, self.slice()));
    }

    pub fn jsonParse(a: A, source: anytype, options: std.json.ParseOptions) !CheckpointBytes {
        return fromEncoded(try std.json.innerParse([]const u8, a, source, options));
    }

    pub fn jsonParseFromValue(a: A, value: std.json.Value, options: std.json.ParseOptions) !CheckpointBytes {
        return fromEncoded(try std.json.innerParseFromValue([]const u8, a, value, options));
    }

    fn fromEncoded(encoded: []const u8) !CheckpointBytes {
        if (encoded.len == 0 or encoded.len > encoded_capacity) return error.InvalidCharacter;
        const len = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidCharacter;
        if (len == 0 or len > capacity) return error.InvalidCharacter;
        var result: CheckpointBytes = .{ .len = len };
        std.base64.standard.Decoder.decode(result.bytes[0..len], encoded) catch return error.InvalidCharacter;
        return result;
    }
};
pub const InstallCheckpoint = struct {
    donor: scope.Record,
    bytes: ?CheckpointBytes,

    pub fn validate(self: *const InstallCheckpoint) !void {
        if (self.bytes) |*bytes| {
            if (bytes.len == 0 or bytes.len > CheckpointBytes.capacity or !std.mem.allEqual(u8, bytes.bytes[bytes.len..], 0))
                return error.InvalidSourceAdmission;
        }
        try self.handoff().validate();
    }

    pub fn handoff(self: *const InstallCheckpoint) scope.CheckpointHandoff {
        return .{ .donor = self.donor, .checkpoint = if (self.bytes) |*bytes| bytes.slice() else null };
    }
};
pub const Action = union(enum) { bind: void, close: void, drop: scope.Cut, fence_attempt: FenceAttempt, close_checkpoint: void, install_checkpoint: InstallCheckpoint };
pub const Intent = struct {
    version: u8,
    owner: completion.RoutedOwner,
    spec: scope.Spec,
    minimum_metadata_index: u64,
    action: Action,
    pub fn validate(self: Intent) !void {
        try self.owner.validate();
        try self.spec.validate();
        if (self.version != 1 or self.minimum_metadata_index == 0 or self.owner.metadata_group_id != self.spec.checkpoint_owner.metadata_group_id or !std.meta.eql(self.owner.metadata_incarnation, self.spec.checkpoint_owner.metadata_incarnation)) return error.InvalidSourceAdmission;
        if (self.action == .drop and (self.action.drop.term == 0 or self.action.drop.index == 0)) return error.InvalidSourceAdmission;
        if (self.action == .close_checkpoint and !std.meta.eql(try scope.Basis.fromOwner(self.owner), self.spec.checkpoint_owner))
            return error.InvalidSourceAdmission;
        if (self.action == .install_checkpoint) {
            const install = &self.action.install_checkpoint;
            try install.validate();
            const root = try scope.Basis.fromOwner(self.owner);
            var successor = install.donor.spec;
            successor.checkpoint_owner = root;
            if (!std.meta.eql(successor, self.spec) or std.meta.eql(root, install.donor.spec.checkpoint_owner))
                return error.InvalidSourceAdmission;
        }
        if (self.action == .fence_attempt) {
            const attempt = self.action.fence_attempt;
            try attempt.reference.validate();
            if (!std.meta.eql(try scope.Basis.fromOwner(self.owner), self.spec.checkpoint_owner) or
                !std.meta.eql(attempt.reference.domain, self.spec.domain()) or
                !std.meta.eql(attempt.reference.binding, self.spec.binding) or attempt.reference.generation != self.spec.generation or
                std.mem.allEqual(u8, &attempt.transaction_id, 0) or std.mem.allEqual(u8, &attempt.reservation, 0))
                return error.InvalidSourceAdmission;
        }
    }
};
pub const Command = struct {
    version: u8,
    intent: Intent,
    metadata_cut: scope.MetadataCut,
    pub fn validate(self: Command) !void {
        try self.intent.validate();
        const valid_version = switch (self.version) {
            1 => self.intent.action != .fence_attempt and self.intent.action != .install_checkpoint and self.intent.action != .close_checkpoint,
            2 => self.intent.action == .bind,
            3 => self.intent.action == .fence_attempt,
            4 => self.intent.action == .bind or self.intent.action == .close_checkpoint or self.intent.action == .fence_attempt or (self.intent.action == .install_checkpoint and self.metadata_cut.index > self.intent.action.install_checkpoint.donor.close_metadata_index),
            else => false,
        };
        if (!valid_version or self.metadata_cut.index < self.intent.minimum_metadata_index or self.metadata_cut.group_id != self.intent.owner.metadata_group_id or !std.meta.eql(self.metadata_cut.incarnation, self.intent.owner.metadata_incarnation)) return error.InvalidSourceAdmission;
    }
};
pub const Query = struct {
    version: u8,
    owner: completion.RoutedOwner,
    domain: scope.Digest,
    minimum_applied_index: u64,
    checkpoint_evidence: ?enum { donor, successor } = null,

    pub fn jsonStringify(self: Query, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Query).@"struct".field_names) |name| {
            if (!std.mem.eql(u8, name, "checkpoint_evidence") or self.checkpoint_evidence != null) {
                try stream.objectField(name);
                try stream.write(@field(self, name));
            }
        }
        try stream.endObject();
    }

    pub fn validate(self: Query) !void {
        try self.owner.validate();
        if (self.version != 1 or self.minimum_applied_index == 0 or std.mem.allEqual(u8, &self.domain, 0)) return error.InvalidSourceAdmission;
    }
};
pub const Observation = struct {
    version: u8,
    owner: completion.RoutedOwner,
    domain: scope.Digest,
    cut: scope.Cut,
    header: scope.Header,
    record: ?scope.Record,
    checkpoint_install: ?scope.CheckpointInstallReceipt = null,
    checkpoint_donor: ?InstallCheckpoint = null,

    pub fn jsonStringify(self: Observation, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Observation).@"struct".field_names) |name| {
            const evidence = comptime std.mem.eql(u8, name, "checkpoint_install") or std.mem.eql(u8, name, "checkpoint_donor");
            const include = if (comptime evidence)
                @field(self, name) != null
            else
                @TypeOf(@field(self, name)) != ?scope.Record or stream.options.emit_null_optional_fields or self.record != null;
            if (include) {
                try stream.objectField(name);
                try stream.write(@field(self, name));
            }
        }
        try stream.endObject();
    }
    pub fn validate(self: Observation, query: Query) !void {
        try query.validate();
        if (self.version != 1 or !std.meta.eql(self.owner, query.owner) or !std.meta.eql(self.domain, query.domain) or self.cut.term == 0 or self.cut.index < query.minimum_applied_index or !std.meta.eql(self.header.root, try scope.Basis.fromOwner(query.owner))) return error.SourceAdmissionCorrupt;
        try self.header.validate();
        if ((query.checkpoint_evidence == .donor) != (self.checkpoint_donor != null) or
            (query.checkpoint_evidence == .successor and self.checkpoint_install == null) or
            (self.checkpoint_donor != null and self.checkpoint_install != null)) return error.SourceAdmissionCorrupt;
        if (self.checkpoint_donor) |capture| {
            try capture.validate();
            if (self.record == null or !std.meta.eql(self.record.?, capture.donor) or
                !std.meta.eql(capture.donor.spec.checkpoint_owner, self.header.root) or
                capture.donor.closed_at.?.index > self.cut.index or
                (capture.donor.closed_at.?.index == self.cut.index and capture.donor.closed_at.?.term != self.cut.term))
                return error.SourceAdmissionCorrupt;
        }

        if (self.checkpoint_install) |install| {
            try install.validate();
            const record = self.record orelse return error.SourceAdmissionCorrupt;
            if (!std.meta.eql(install.domain, self.domain) or !std.meta.eql(install.successor, self.header.root) or
                !std.meta.eql(install.successor, record.spec.checkpoint_owner) or install.applied.index > self.cut.index or
                (install.applied.index == self.cut.index and install.applied.term != self.cut.term) or
                install.authorization.index > self.header.metadata_floor)
                return error.SourceAdmissionCorrupt;
        }
        if (self.record) |record| {
            try record.validate();
            if (!self.header.strict or self.header.domains == 0 or !std.meta.eql(record.spec.domain(), query.domain) or record.spec.checkpoint_owner.metadata_group_id != self.owner.metadata_group_id or !std.meta.eql(record.spec.checkpoint_owner.metadata_incarnation, self.owner.metadata_incarnation)) return error.SourceAdmissionCorrupt;
        }
    }
};
/// The serving owner must independently qualify ReadIndex and the full catalog
/// owner before opening this snapshot. An absent record is not a transaction
/// outcome, a legacy-drain certificate, or authorization to replay effects.
pub fn observe(a: A, txn: anytype, query: Query, applied: scope.Cut) !Observation {
    try query.validate();
    const root = try scope.Basis.fromOwner(query.owner);
    var result: Observation = .{ .version = 1, .owner = query.owner, .domain = query.domain, .cut = applied, .header = try scope.Store.readHeader(a, txn, root), .record = try scope.Store.readRecord(a, txn, query.domain) };
    if (query.checkpoint_evidence) |evidence| switch (evidence) {
        .donor => {
            const record = result.record orelse return error.SourceOutcomeUnresolved;
            const handoff = try scope.Store.captureCheckpointHandoff(a, txn, root, record.spec, record.last_committed_sequence, null);
            defer if (handoff.checkpoint) |raw| a.free(raw);
            result.checkpoint_donor = .{ .donor = handoff.donor, .bytes = if (handoff.checkpoint) |raw| try CheckpointBytes.fromSlice(raw) else null };
        },
        .successor => result.checkpoint_install = (try scope.Store.readCheckpointInstall(a, txn, query.domain)) orelse return error.SourceOutcomeUnresolved,
    };
    try result.validate(query);
    return result;
}
/// `gate.requireLegacyDrained(txn, root)` is an independent, trusted migration
/// check under this same writer. The request deliberately carries no drain flag.
/// On any error the enclosing transaction, including partial writes, must abort.
/// A reply acknowledges only this physical root; it is not an all-owner fence.
pub fn apply(a: A, txn: anytype, gate: anytype, command: Command, applied: scope.Cut) !Observation {
    try command.validate();
    if (applied.term == 0 or applied.index == 0) return error.InvalidSourceAdmission;
    const intent = command.intent;
    const root = try scope.Basis.fromOwner(intent.owner);
    const header = try scope.Store.readHeader(a, txn, root);
    // Closure on an absent scope also enables strict admission. It needs the
    // same legacy handover as first binding; otherwise old workers could write.
    if (!header.strict) try gate.requireLegacyDrained(txn, root);
    var checkpoint_install: ?scope.CheckpointInstallReceipt = null;
    switch (intent.action) {
        .bind => _ = if (command.version == 4)
            try scope.Store.bindAfterCheckpointTransfer(a, txn, root, intent.spec, command.metadata_cut)
        else
            try scope.Store.bind(a, txn, root, intent.spec, command.metadata_cut),
        .close_checkpoint => try scope.Store.closeCheckpointForTransfer(a, txn, root, intent.spec, command.metadata_cut, applied),
        .install_checkpoint => |*install| checkpoint_install = try scope.Store.installCheckpointHandoff(a, txn, root, install.handoff(), command.metadata_cut, applied),
        .close => {
            if (try scope.Store.readRecord(a, txn, intent.spec.domain()) != null) {
                _ = try scope.Store.close(a, txn, root, intent.spec.domain(), intent.spec, command.metadata_cut, applied);
            } else _ = try scope.Store.closeAbsent(a, txn, root, intent.spec, command.metadata_cut);
        },
        .fence_attempt => |attempt| {
            const GateType = switch (@typeInfo(@TypeOf(gate))) {
                .pointer => |ptr| ptr.child,
                else => @TypeOf(gate),
            };
            if (comptime @hasDecl(GateType, "fenceUnadmittedAttempt")) {
                try gate.fenceUnadmittedAttempt(txn, root, attempt);
            } else return error.SourcePendingWork;
            var advanced = header;
            advanced.metadata_floor = @max(advanced.metadata_floor, command.metadata_cut.index);
            try scope.Store.writeHeader(a, txn, advanced);
        },
        .drop => |closed_at| {
            if (try scope.Store.readRecord(a, txn, intent.spec.domain())) |record| {
                if (!std.meta.eql(record.spec, intent.spec)) return error.SourceCheckpointConflict;
            }
            try scope.Store.drop(a, txn, root, intent.spec.domain(), command.metadata_cut, intent.spec.generation, closed_at);
        },
    }
    const query: Query = .{ .version = 1, .owner = intent.owner, .domain = intent.spec.domain(), .minimum_applied_index = applied.index };
    var result = try observe(a, txn, query, applied);
    result.checkpoint_install = checkpoint_install;
    try result.validate(query);
    return result;
}
pub fn decodeCommand(a: A, bytes: []const u8) !Command {
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidSourceAdmission;
    var parsed = std.json.parseFromSlice(Command, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSourceAdmission;
    defer parsed.deinit();
    try parsed.value.validate();
    return parsed.value;
}
pub fn encodeCommand(a: A, command: Command) ![]u8 {
    try command.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_bytes) return error.InvalidSourceAdmission;
    return bytes;
}
pub fn decodeObservation(a: A, bytes: []const u8, query: Query) !Observation {
    if (bytes.len == 0 or bytes.len > max_bytes) return error.SourceAdmissionCorrupt;
    var parsed = std.json.parseFromSlice(Observation, a, bytes, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.SourceAdmissionCorrupt;
    defer parsed.deinit();
    try parsed.value.validate(query);
    return parsed.value;
}

const TestTxn = struct {
    map: std.StringHashMapUnmanaged([]u8) = .empty,
    pub fn deinit(self: *@This()) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            std.testing.allocator.free(entry.key_ptr.*);
            std.testing.allocator.free(entry.value_ptr.*);
        }
        self.map.deinit(std.testing.allocator);
    }
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        return self.map.get(key) orelse error.NotFound;
    }
    pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        const a = std.testing.allocator;
        const k = try a.dupe(u8, key);
        errdefer a.free(k);
        const v = try a.dupe(u8, value);
        errdefer a.free(v);
        const slot = try self.map.getOrPut(a, k);
        if (slot.found_existing) {
            a.free(k);
            a.free(slot.value_ptr.*);
        }
        slot.value_ptr.* = v;
    }
    pub fn delete(self: *@This(), key: []const u8) !void {
        const entry = self.map.fetchRemove(key) orelse return error.NotFound;
        std.testing.allocator.free(entry.key);
        std.testing.allocator.free(entry.value);
    }
};

const Gate = struct {
    drained: bool = false,
    calls: usize = 0,
    pub fn requireLegacyDrained(self: *@This(), _: anytype, _: scope.Basis) !void {
        self.calls += 1;
        if (!self.drained) return error.SourcePendingWork;
    }
};
fn fixture() Command {
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 3 };
    return .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .spec = .{ .actor_table_id = 3, .source_id = @splat(4), .generation = 1, .binding = @splat(5), .checkpoint_owner = scope.Basis.fromOwner(owner) catch unreachable }, .minimum_metadata_index = 10, .action = .bind }, .metadata_cut = .{ .group_id = 1, .incarnation = owner.metadata_incarnation, .index = 10 } };
}
test "scope controls require independent legacy drain and close advances the root floor" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: TestTxn = .{};
    defer txn.deinit();
    var gate: Gate = .{};
    var command = fixture();
    try std.testing.expectError(error.SourcePendingWork, apply(a, &txn, &gate, command, .{ .term = 1, .index = 20 }));
    try std.testing.expectEqual(@as(u32, 0), txn.map.count());
    gate.drained = true;
    const bound = try apply(a, &txn, &gate, command, .{ .term = 1, .index = 21 });
    try std.testing.expectEqual(scope.Phase.active, bound.record.?.phase);
    try std.testing.expectEqual(@as(usize, 2), gate.calls);
    gate.drained = false; // strict roots no longer use migration authorization
    command.intent.action = .close;
    command.metadata_cut.index = 11;
    const closed = try apply(a, &txn, &gate, command, .{ .term = 1, .index = 22 });
    try std.testing.expectEqual(scope.Phase.closed, closed.record.?.phase);
    try std.testing.expectEqual(@as(u64, 11), closed.header.metadata_floor);
    const retried = try apply(a, &txn, &gate, command, .{ .term = 2, .index = 23 });
    try std.testing.expectEqualDeep(closed.record.?.closed_at, retried.record.?.closed_at);
    var stale = fixture();
    stale.intent.spec.source_id = @splat(6);
    try std.testing.expectError(error.SourceCheckpointConflict, apply(a, &txn, &gate, stale, .{ .term = 2, .index = 24 }));
    command.intent.action = .{ .drop = closed.record.?.closed_at.? };
    command.metadata_cut.index = 12;
    command.intent.spec.binding = @splat(7);
    try std.testing.expectError(error.SourceCheckpointConflict, apply(a, &txn, &gate, command, .{ .term = 2, .index = 25 }));
    command.intent.spec.binding = fixture().intent.spec.binding;
    const dropped = try apply(a, &txn, &gate, command, .{ .term = 2, .index = 26 });
    try std.testing.expect(dropped.record == null);
    try std.testing.expectEqual(@as(u16, 0), dropped.header.domains);
    try std.testing.expect(dropped.header.strict);
    try std.testing.expectEqual(@as(u64, 12), dropped.header.metadata_floor);
    _ = try apply(a, &txn, &gate, command, .{ .term = 2, .index = 27 });
    try std.testing.expectEqual(@as(usize, 2), gate.calls);
}
test "scope absent close fences late bind without charging a domain" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: TestTxn = .{};
    defer txn.deinit();
    var gate: Gate = .{ .drained = true };
    var command = fixture();
    command.intent.action = .close;
    command.metadata_cut.index = 11;
    const closed = try apply(a, &txn, &gate, command, .{ .term = 1, .index = 20 });
    try std.testing.expect(closed.record == null);
    try std.testing.expectEqual(@as(u16, 0), closed.header.domains);
    try std.testing.expectEqual(@as(u32, 1), txn.map.count());
    try std.testing.expectError(error.SourceCheckpointConflict, apply(a, &txn, &gate, fixture(), .{ .term = 1, .index = 21 }));
}
test "scope control codec binds original owners cuts and durable record invariants" {
    const a = std.testing.allocator;
    var command = fixture();
    const bytes = try encodeCommand(a, command);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(command, try decodeCommand(a, bytes));
    command.metadata_cut.index = 9;
    try std.testing.expectError(error.InvalidSourceAdmission, encodeCommand(a, command));
    command = fixture();
    command.intent.spec.checkpoint_owner.metadata_group_id = 9;
    try std.testing.expectError(error.InvalidSourceAdmission, encodeCommand(a, command));
    const query: Query = .{ .version = 1, .owner = fixture().intent.owner, .domain = fixture().intent.spec.domain(), .minimum_applied_index = 20 };
    var observation: Observation = .{ .version = 1, .owner = query.owner, .domain = query.domain, .cut = .{ .term = 1, .index = 20 }, .header = .{ .root = try scope.Basis.fromOwner(query.owner), .strict = true, .domains = 1, .metadata_floor = 10 }, .record = .{ .spec = fixture().intent.spec, .bound_metadata_index = 10 } };
    const valid = try std.json.Stringify.valueAlloc(a, observation, .{});
    defer a.free(valid);
    try std.testing.expectEqualDeep(observation, try decodeObservation(a, valid, query));
    observation.record.?.phase = .closed; // missing the corresponding durable close cut
    const corrupt = try std.json.Stringify.valueAlloc(a, observation, .{});
    defer a.free(corrupt);
    try std.testing.expectError(error.SourceAdmissionCorrupt, decodeObservation(a, corrupt, query));
    observation.cut.index = 19;
    try std.testing.expectError(error.SourceAdmissionCorrupt, observation.validate(query));
}

test "scope repeated drop advances a fresh retired metadata cut without a domain tombstone" {
    const a = std.testing.allocator;
    var txn: TestTxn = .{};
    defer txn.deinit();
    var gate: Gate = .{ .drained = true };
    var command = fixture();
    _ = try apply(a, &txn, &gate, command, .{ .term = 1, .index = 20 });
    command.intent.action = .close;
    command.metadata_cut.index = 11;
    const closed = try apply(a, &txn, &gate, command, .{ .term = 1, .index = 21 });
    command.intent.action = .{ .drop = closed.record.?.closed_at.? };
    command.metadata_cut.index = 12;
    _ = try apply(a, &txn, &gate, command, .{ .term = 1, .index = 22 });
    command.metadata_cut.index = 13;
    const refreshed = try apply(a, &txn, &gate, command, .{ .term = 1, .index = 23 });
    try std.testing.expect(refreshed.record == null);
    try std.testing.expect(refreshed.header.strict);
    try std.testing.expectEqual(@as(u16, 0), refreshed.header.domains);
    try std.testing.expectEqual(@as(u64, 13), refreshed.header.metadata_floor);
    try std.testing.expectEqual(@as(u32, 1), txn.map.count());
}
