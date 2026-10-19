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

//! Data-only schema validation commands and durable progress codecs.
//! Canonical decoding grants no metadata permission, commit cut or publication
//! authority. Physical preparation and execution remain in the storage owner.
const std = @import("std");
const A = std.mem.Allocator;
const Digest = [32]u8;
const topology = @import("relational_integrity_topology_contract.zig");
const source_authority = @import("../source_authority.zig");
const positions = @import("receipt_position.zig");
const integrity = @import("relational_integrity_catalog.zig");
pub const max_columns = 4096;
pub const max_schema = 2 * 1024 * 1024;
pub const max_names = 1024 * 1024;
pub const max_cursor = @import("relational_index_limits.zig").max_cursor_key_bytes;
pub const max_header = 8192;
pub const max_input_bytes = 56 + 2 * max_schema + integrity.max_catalog_bytes + max_names;

pub fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn eq(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
pub const Binding = struct {
    id: [16]u8,
    plan_digest: Digest,
    owner: Digest,
    fence: topology.Fence,
    authority: source_authority.Kind,
    before_version: u32,
    after_version: u32,
    columns: u16,
    before_schema: Digest,
    after_schema: Digest,
    before_public: Digest,
    after_public: Digest,
    after_catalog: Digest,
    physical_catalog: Digest,
    inputs: Digest,
    projection_identity: Digest,

    pub fn validate(self: Binding) !void {
        _ = try self.fence.encode();
        if (self.fence.role != .schema_validation or self.fence.owner_group_id != self.fence.peer_group_id or
            self.fence.namespace.shard_id == 0 or self.fence.namespace.range_id == 0 or
            self.columns == 0 or self.columns > max_columns or std.mem.allEqual(u8, &self.id, 0) or
            self.after_version != (std.math.add(u32, self.before_version, 1) catch return error.InvalidSchemaValidationJob))
            return error.InvalidSchemaValidationJob;
        inline for (.{ "plan_digest", "owner", "before_schema", "after_schema", "before_public", "after_public", "after_catalog", "physical_catalog", "inputs", "projection_identity" }) |field|
            if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidSchemaValidationJob;
    }
};

pub const Phase = enum { fencing, scanning, ready, failed, canceling, canceled, installed };
/// Durable local installation evidence, not a signed publication receipt.
/// The outer owner must authenticate decision/root/cohort authority before
/// staging this record and commit its replay marker/outbox in the same txn.
pub const Installation = struct {
    decision: Digest,
    ready_digest: Digest,
    ready_position: positions.Position,
    ready_last_effect: Digest,
    physical_catalog: Digest,
    activation: Digest,
    position: positions.Position,
};
pub const Header = struct {
    binding: Binding,
    phase: Phase,
    admitted: ?positions.Position,
    frozen: ?positions.Position = null,
    updated: positions.Position,
    revision: u64 = 1,
    records: u64 = 0,
    rows_scanned: u64 = 0,
    cursor_digest: Digest,
    chain: Digest,
    last_effect: Digest,
    failure_column: ?u16 = null,
    installation: ?Installation = null,

    pub fn validate(self: Header) !void {
        try self.binding.validate();
        inline for (.{ "cursor_digest", "chain", "last_effect" }) |field|
            if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidSchemaValidationJob;
        if (self.revision == 0 or self.rows_scanned > self.records or
            (self.phase == .failed) != (self.failure_column != null) or
            (self.failure_column != null and self.failure_column.? >= self.binding.columns) or
            (self.phase != .canceling and self.phase != .canceled and self.admitted == null) or
            (self.frozen != null and self.admitted == null) or
            (self.phase == .installed) != (self.installation != null)) return error.InvalidSchemaValidationJob;
        if (self.phase == .fencing and (self.frozen != null or self.records != 0 or self.rows_scanned != 0 or !eq(self.cursor_digest, hash(""))))
            return error.InvalidSchemaValidationJob;
        if ((self.phase == .scanning or self.phase == .ready or self.phase == .failed or self.phase == .installed) and self.frozen == null) return error.InvalidSchemaValidationJob;
        for ([_]?positions.Position{ self.admitted, self.frozen, self.updated }) |maybe| if (maybe) |position| {
            try position.requireNamespace(self.binding.fence.namespace);
            if ((position == .native) != (self.binding.authority == .native)) return error.InvalidControlReceiptPosition;
            try precedesOrEqual(position, self.updated);
        };
        if (self.frozen) |cut| try precedes(self.admitted.?, cut);
        if (self.installation) |installed| {
            inline for (.{ "decision", "ready_digest", "ready_last_effect", "physical_catalog", "activation" }) |field|
                if (std.mem.allEqual(u8, &@field(installed, field), 0)) return error.InvalidSchemaValidationJob;
            if (!std.meta.eql(installed.position, self.updated)) return error.InvalidControlReceiptPosition;
            try precedes(installed.ready_position, installed.position);
            // Installation preserves the exact validation predecessor rather
            // than adopting an opaque digest beside unrelated row counters.
            var predecessor = self;
            predecessor.phase = .ready;
            predecessor.installation = null;
            predecessor.updated = installed.ready_position;
            predecessor.last_effect = installed.ready_last_effect;
            predecessor.revision = std.math.sub(u64, self.revision, 1) catch return error.InvalidSchemaValidationJob;
            if (predecessor.revision == 0) return error.InvalidSchemaValidationJob;
            try precedesOrEqual(predecessor.frozen.?, predecessor.updated);
            if (!eq(try predecessor.digest(), installed.ready_digest)) return error.InvalidSchemaValidationJob;
        }
    }
    pub fn encode(self: Header, a: A) ![]u8 {
        try self.validate();
        var body: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&body);
        try std.json.Stringify.value(self, .{}, &writer);
        const json = writer.buffered();
        const bytes = try a.alloc(u8, 12 + json.len + 32);
        @memcpy(bytes[0..8], "ASVJ\x01\x00\x00\x00");
        std.mem.writeInt(u32, bytes[8..12], @intCast(json.len), .little);
        @memcpy(bytes[12..][0..json.len], json);
        @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Header {
        if (bytes.len < 44 or bytes.len > 44 + max_header or !std.mem.eql(u8, bytes[0..8], "ASVJ\x01\x00\x00\x00") or
            std.mem.readInt(u32, bytes[8..12], .little) != bytes.len - 44 or !eq(hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*))
            return error.InvalidSchemaValidationJob;
        var scratch: [64 * 1024]u8 = undefined;
        var fixed = std.heap.FixedBufferAllocator.init(&scratch);
        const result = std.json.parseFromSliceLeaky(Header, fixed.allocator(), bytes[12 .. bytes.len - 32], .{}) catch return error.InvalidSchemaValidationJob;
        try result.validate();
        var canonical: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&canonical);
        try std.json.Stringify.value(result, .{}, &writer);
        if (!std.mem.eql(u8, writer.buffered(), bytes[12 .. bytes.len - 32])) return error.InvalidSchemaValidationJob;
        return result;
    }
    pub fn digest(self: Header) !Digest {
        var buffer: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(self, .{}, &writer);
        return hash(writer.buffered());
    }
};

pub fn precedesOrEqual(a: positions.Position, b: positions.Position) !void {
    if (a == .native and b == .native) {
        if (!a.native.namespace.eql(b.native.namespace) or a.native.sequence > b.native.sequence) return error.InvalidControlReceiptPosition;
    } else if (a == .raft and b == .raft) {
        if (a.raft.index > b.raft.index or a.raft.term > b.raft.term or (a.raft.index == b.raft.index and a.raft.term != b.raft.term))
            return error.InvalidControlReceiptPosition;
    } else return error.InvalidControlReceiptPosition;
}
pub fn precedes(a: positions.Position, b: positions.Position) !void {
    try precedesOrEqual(a, b);
    if (std.meta.eql(a, b)) return error.InvalidControlReceiptPosition;
}
pub const NullabilityReplayBoundary = struct {
    records_examined: usize,
    rows_examined: usize,
    violation_column: ?usize,
    after: []const u8,
    more: bool,

    pub fn validate(self: @This()) !void {
        if (self.records_examined > 65_536 or self.rows_examined > self.records_examined or
            (self.more and self.records_examined == 0) or (!self.more and self.records_examined >= 65_536) or
            self.after.len > max_cursor or
            (self.violation_column != null and (self.violation_column.? >= max_columns or self.rows_examined == 0 or !self.more)))
            return error.InvalidNullabilityReplay;
    }
};

pub const PageCommand = struct {
    binding: Binding,
    expected: Digest,
    boundary: NullabilityReplayBoundary,
    pub const max_wire_bytes = 48 + max_header + max_cursor;
    const Descriptor = struct {
        binding: Binding,
        expected: Digest,
        records: usize,
        rows: usize,
        violation: ?usize,
        more: bool,
    };
    pub fn validate(self: @This()) !void {
        try self.binding.validate();
        try self.boundary.validate();
        if (std.mem.allEqual(u8, &self.expected, 0) or
            (self.boundary.violation_column != null and self.boundary.violation_column.? >= self.binding.columns)) return error.InvalidSchemaValidationPageCommand;
    }
    pub fn effect(self: @This()) !Digest {
        try self.validate();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("schema-validation-page/v1");
        state.update(&self.expected);
        state.update(&hash(self.boundary.after));
        var numbers: [25]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.boundary.rows_examined, .little);
        std.mem.writeInt(u64, numbers[8..16], self.boundary.records_examined, .little);
        std.mem.writeInt(u64, numbers[16..24], if (self.boundary.violation_column) |column| column else std.math.maxInt(u64), .little);
        numbers[24] = @intFromBool(self.boundary.more);
        state.update(&numbers);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
    pub fn encode(self: @This(), a: A) ![]u8 {
        try self.validate();
        const descriptor: Descriptor = .{ .binding = self.binding, .expected = self.expected, .records = self.boundary.records_examined, .rows = self.boundary.rows_examined, .violation = self.boundary.violation_column, .more = self.boundary.more };
        var buffer: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(descriptor, .{}, &writer);
        const metadata = writer.buffered();
        const bytes = try a.alloc(u8, 48 + metadata.len + self.boundary.after.len);
        @memcpy(bytes[0..8], "ASVP\x01\x00\x00\x00");
        std.mem.writeInt(u32, bytes[8..12], @intCast(metadata.len), .little);
        std.mem.writeInt(u32, bytes[12..16], @intCast(self.boundary.after.len), .little);
        @memcpy(bytes[16..][0..metadata.len], metadata);
        @memcpy(bytes[16 + metadata.len ..][0..self.boundary.after.len], self.boundary.after);
        @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
        return bytes;
    }
    /// The returned cursor borrows bytes. All descriptor fields are owned
    /// values; no JSON parser allocation or request arena escapes this call.
    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len < 48 or bytes.len > max_wire_bytes or !std.mem.eql(u8, bytes[0..8], "ASVP\x01\x00\x00\x00") or
            !eq(hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*)) return error.InvalidSchemaValidationPageCommand;
        const metadata_len: usize = std.mem.readInt(u32, bytes[8..12], .little);
        const cursor_len: usize = std.mem.readInt(u32, bytes[12..16], .little);
        if (metadata_len == 0 or metadata_len > max_header or cursor_len > max_cursor or metadata_len + cursor_len != bytes.len - 48)
            return error.InvalidSchemaValidationPageCommand;
        var buffer: [64 * 1024]u8 = undefined;
        var scratch = std.heap.FixedBufferAllocator.init(&buffer);
        const descriptor = std.json.parseFromSliceLeaky(Descriptor, scratch.allocator(), bytes[16..][0..metadata_len], .{}) catch return error.InvalidSchemaValidationPageCommand;
        var canonical: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&canonical);
        try std.json.Stringify.value(descriptor, .{}, &writer);
        if (!std.mem.eql(u8, writer.buffered(), bytes[16..][0..metadata_len])) return error.InvalidSchemaValidationPageCommand;
        const result: @This() = .{ .binding = descriptor.binding, .expected = descriptor.expected, .boundary = .{ .records_examined = descriptor.records, .rows_examined = descriptor.rows, .violation_column = descriptor.violation, .more = descriptor.more, .after = bytes[16 + metadata_len ..][0..cursor_len] } };
        try result.validate();
        return result;
    }
};

pub fn inputParts(bytes: []const u8) ![4][]const u8 {
    if (bytes.len < 56 or bytes.len > max_input_bytes or !std.mem.eql(u8, bytes[0..8], "ASVI\x01\x00\x00\x00") or !eq(hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*))
        return error.InvalidSchemaValidationJob;
    var result: [4][]const u8 = undefined;
    var offset: usize = 24;
    const limits = [_]usize{ max_schema, max_schema, integrity.max_catalog_bytes, max_names };
    for (&result, limits, 0..) |*part, limit, i| {
        const length: usize = std.mem.readInt(u32, bytes[8 + 4 * i ..][0..4], .little);
        if (length == 0 or length > limit or length > bytes.len - 32 - offset) return error.InvalidSchemaValidationJob;
        part.* = bytes[offset..][0..length];
        offset += length;
    }
    if (offset != bytes.len - 32) return error.InvalidSchemaValidationJob;
    return result;
}
pub const NativeObservation = struct {
    pub const max_wire_bytes = 32 * 1024;
    binding: Binding,
    physical_root_incarnation: u128,
    sequence: u64,
    header: ?Header,
    replication_pending: bool,

    pub fn validateFor(self: @This(), binding: Binding) !void {
        try binding.validate();
        if (binding.authority != .native or self.physical_root_incarnation == 0 or
            !std.meta.eql(self.binding, binding)) return error.SchemaValidationJobChanged;
        if (self.header) |header| {
            try header.validate();
            if (!std.meta.eql(header.binding, binding) or header.updated != .native or
                header.updated.native.sequence > self.sequence) return error.SchemaValidationJobChanged;
        }
    }
};

pub const Action = enum { begin, freeze, page, cancel, install };
pub const CommandDescriptor = struct { binding: Binding, action: Action };
pub const raft_magic = "ASVR\x01\x00\x00\x00";

pub const Command = struct {
    pub const max_raft_wire_bytes = 48 + max_header + max_input_bytes;
    binding: Binding,
    action: Action,
    /// Borrowed immutable ASVI inputs, ASVP page, or decision+ready digests.
    payload: []const u8 = "",

    /// Owned transport bytes, never a committed-entry receipt. Keeping the
    /// large immutable inputs raw avoids a JSON/base64 copy at the ABI edge.
    pub fn encodeRaft(self: @This(), a: A) ![]u8 {
        try self.validateFor(.raft);
        var buffer: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(CommandDescriptor{ .binding = self.binding, .action = self.action }, .{}, &writer);
        const metadata = writer.buffered();
        if (self.payload.len > max_input_bytes) return error.InvalidSchemaOwnerControl;
        const bytes = try a.alloc(u8, 48 + metadata.len + self.payload.len);
        @memcpy(bytes[0..8], raft_magic);
        std.mem.writeInt(u32, bytes[8..12], @intCast(metadata.len), .little);
        std.mem.writeInt(u32, bytes[12..16], @intCast(self.payload.len), .little);
        @memcpy(bytes[16..][0..metadata.len], metadata);
        @memcpy(bytes[16 + metadata.len ..][0..self.payload.len], self.payload);
        @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
        return bytes;
    }

    /// Only payload borrows input. Binding/action are owned fixed values;
    /// canonical descriptor decoding uses bounded stack scratch, not an arena.
    pub fn decodeRaft(bytes: []const u8) !@This() {
        if (bytes.len < 48 or bytes.len > max_raft_wire_bytes or !std.mem.eql(u8, bytes[0..8], raft_magic) or
            !eq(hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*)) return error.InvalidSchemaOwnerControl;
        const metadata_len: usize = std.mem.readInt(u32, bytes[8..12], .little);
        const payload_len: usize = std.mem.readInt(u32, bytes[12..16], .little);
        if (metadata_len == 0 or metadata_len > max_header or payload_len > max_input_bytes or
            metadata_len + payload_len != bytes.len - 48) return error.InvalidSchemaOwnerControl;
        var buffer: [64 * 1024]u8 = undefined;
        var scratch = std.heap.FixedBufferAllocator.init(&buffer);
        const descriptor = std.json.parseFromSliceLeaky(CommandDescriptor, scratch.allocator(), bytes[16..][0..metadata_len], .{}) catch return error.InvalidSchemaOwnerControl;
        var canonical: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&canonical);
        try std.json.Stringify.value(descriptor, .{}, &writer);
        if (!std.mem.eql(u8, writer.buffered(), bytes[16..][0..metadata_len])) return error.InvalidSchemaOwnerControl;
        const command: @This() = .{ .binding = descriptor.binding, .action = descriptor.action, .payload = bytes[16 + metadata_len ..][0..payload_len] };
        try command.validateFor(.raft);
        return command;
    }

    pub fn validate(self: @This()) !void {
        return self.validateFor(.native);
    }
    pub fn validateFor(self: @This(), authority: @FieldType(Binding, "authority")) !void {
        try self.binding.validate();
        if (self.binding.authority != authority) return error.InvalidControlReceiptPosition;
        switch (self.action) {
            .begin => {
                _ = try inputParts(self.payload);
                if (!eq(hash(self.payload), self.binding.inputs)) return error.SchemaValidationJobChanged;
            },
            .page => {
                const command = try PageCommand.decode(self.payload);
                if (!std.meta.eql(command.binding, self.binding)) return error.SchemaValidationJobChanged;
            },
            .freeze, .cancel => if (self.payload.len != 0) return error.InvalidSchemaOwnerControl,
            .install => if (self.payload.len != 64 or std.mem.allEqual(u8, self.payload[0..32], 0) or
                std.mem.allEqual(u8, self.payload[32..64], 0)) return error.InvalidSchemaOwnerControl,
        }
    }
};
