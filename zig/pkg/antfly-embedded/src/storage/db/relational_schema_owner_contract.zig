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

/// Shared ordered-column identity for native preparation and metadata plan
/// verification. This commits names, not a layout, scan result or permission.
pub const ProjectionHasher = struct {
    state: std.crypto.hash.Blake3,
    count: usize = 0,
    name_bytes: usize = 0,

    pub fn init() ProjectionHasher {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/nullability-projection/v1");
        return .{ .state = state };
    }
    pub fn add(self: *ProjectionHasher, name: []const u8) !void {
        const total = std.math.add(usize, self.name_bytes, name.len) catch return error.InvalidNullabilityProjection;
        if (self.count >= max_columns or name.len == 0 or total > max_names or std.mem.indexOfScalar(u8, name, 0) != null)
            return error.InvalidNullabilityProjection;
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, name.len, .little);
        self.state.update(&length);
        self.state.update(name);
        self.count += 1;
        self.name_bytes = total;
    }
    pub fn finish(self: ProjectionHasher) !Digest {
        if (self.count == 0) return error.InvalidNullabilityProjection;
        var state = self.state;
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
};

pub fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn eq(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
// Delegate field values to std.json. In particular byte-array formatting must
// remain identical to existing signed NOT NULL records and receipt identities.
fn writeFields(value: anytype, writer: anytype) @TypeOf(writer.*).Error!void {
    try writer.beginObject();
    inline for (@typeInfo(@TypeOf(value)).@"struct".field_names) |field| {
        if (!value.nativeJsonSkipField(field)) {
            try writer.objectField(field);
            try writer.write(@field(value, field));
        }
    }
    try writer.endObject();
}

pub const Purpose = enum { not_null, foreign_key_source, foreign_key_parent, foreign_key_dual };
pub const ForeignKeyParent = struct {
    constraint_name: []const u8,
    declared_parent: []const u8,
    parent_table: []const u8,
    parent_table_id: u64,
    parent_schema_version: u32,
    parent_schema: [32]u8,
    parent_public: [32]u8,
    parent_catalog: [32]u8,
    parent_unique: []const u8,
    parent_generation: @import("relational_integrity_contract.zig").Generation,
    /// Public admission pins only the selected UNIQUE definition, permitting
    /// unrelated parent schema epochs while the validation job is active.
    parent_definition: ?Digest = null,

    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return std.mem.eql(u8, field, "parent_definition") and self.parent_definition == null;
    }
    pub fn jsonStringify(self: @This(), writer: anytype) @TypeOf(writer.*).Error!void {
        return writeFields(self, writer);
    }
};
pub const ForeignKeySource = struct {
    purpose: Purpose = .foreign_key_source,
    table_name: []const u8,
    parents: []const ForeignKeyParent,

    pub fn validate(self: @This()) !void {
        if (self.purpose != .foreign_key_source or self.parents.len == 0 or self.parents.len > 256 or self.table_name.len == 0 or self.table_name.len > 4096 or std.mem.indexOfScalar(u8, self.table_name, 0) != null) return error.InvalidSchemaValidationJob;
        var name_bytes: usize = self.table_name.len;
        for (self.parents, 0..) |scope, i| {
            // Initial public table schemas use epoch zero; identity is bound by
            // the table ID and exact schema/catalog/generation digests.
            if (scope.parent_table_id == 0 or std.mem.allEqual(u8, &scope.parent_generation, 0)) return error.InvalidSchemaValidationJob;
            inline for (.{ "parent_schema", "parent_public", "parent_catalog" }) |field| if (std.mem.allEqual(u8, &@field(scope, field), 0)) return error.InvalidSchemaValidationJob;
            if (scope.parent_definition) |fingerprint| if (std.mem.allEqual(u8, &fingerprint, 0)) return error.InvalidSchemaValidationJob;
            inline for (.{ "constraint_name", "declared_parent", "parent_table", "parent_unique" }) |field| {
                const name = @field(scope, field);
                name_bytes = std.math.add(usize, name_bytes, name.len) catch return error.InvalidSchemaValidationJob;
                if (name.len == 0 or name.len > 4096 or name_bytes > 1024 * 1024 or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSchemaValidationJob;
            }
            for (self.parents[0..i]) |prior| if (std.mem.eql(u8, prior.constraint_name, scope.constraint_name)) return error.InvalidSchemaValidationJob;
        }
    }
};

/// Logical selection supplied by ingress; physical parent generations are
/// captured separately through the read-only owner preparation boundary.
pub const Selection = union(enum) {
    not_null: []const []const u8,
    foreign_keys: []const []const u8,

    pub fn names(self: @This()) []const []const u8 {
        return switch (self) {
            inline else => |value| value,
        };
    }
    pub fn validate(self: @This()) !void {
        if (self == .foreign_keys and self.foreign_keys.len > 256) return error.InvalidSchemaValidationJob;
        var projection = ProjectionHasher.init();
        for (self.names(), 0..) |name, i| {
            try projection.add(name);
            for (self.names()[0..i]) |prior| if (std.mem.eql(u8, prior, name)) return error.InvalidSchemaValidationJob;
        }
        _ = try projection.finish();
    }
};

/// Discriminated immutable inputs. NOT NULL hashes its typed column names;
/// FK projection identity additionally binds the real claim generations.
pub const InputSelection = union(enum) {
    not_null: []const []const u8,
    foreign_keys: ForeignKeySource,

    pub fn validate(self: @This()) !void {
        switch (self) {
            .not_null => |names| try (Selection{ .not_null = names }).validate(),
            .foreign_keys => |source| try source.validate(),
        }
    }
    pub fn count(self: @This()) usize {
        return switch (self) {
            .not_null => |names| names.len,
            .foreign_keys => |source| source.parents.len,
        };
    }
    pub fn purpose(self: @This()) Purpose {
        return switch (self) {
            .not_null => .not_null,
            .foreign_keys => .foreign_key_source,
        };
    }
    pub fn name(self: @This(), index: usize) []const u8 {
        return switch (self) {
            .not_null => |names| names[index],
            .foreign_keys => |source| source.parents[index].constraint_name,
        };
    }
    pub fn requireSelection(self: @This(), selection: Selection) !void {
        try self.validate();
        try selection.validate();
        if ((self == .not_null) != (selection == .not_null) or self.count() != selection.names().len) return error.InvalidSchemaValidationJob;
        for (selection.names(), 0..) |selected, i| if (!std.mem.eql(u8, self.name(i), selected)) return error.InvalidSchemaValidationJob;
    }
    pub fn logical(self: @This(), a: A) !Selection {
        const names = try a.alloc([]const u8, self.count());
        for (names, 0..) |*value, i| value.* = self.name(i);
        return switch (self) {
            .not_null => .{ .not_null = names },
            .foreign_keys => .{ .foreign_keys = names },
        };
    }
    pub fn encodePayload(self: @This(), a: A) ![]u8 {
        try self.validate();
        var count_writer = std.Io.Writer.Discarding.init(&.{});
        switch (self) {
            inline else => |value| try std.json.Stringify.value(value, .{}, &count_writer.writer),
        }
        if (count_writer.fullCount() > max_names) return error.InvalidSchemaValidationJob;
        const bytes = try a.alloc(u8, @intCast(count_writer.fullCount()));
        errdefer a.free(bytes);
        var writer = std.Io.Writer.fixed(bytes);
        switch (self) {
            inline else => |value| try std.json.Stringify.value(value, .{}, &writer),
        }
        return bytes;
    }
};

pub const ReferenceCoverage = struct {
    expected: @import("relational_reference_witness.zig").Witness = .{},
    observed: @import("relational_reference_witness.zig").Witness = .{},

    pub fn validate(self: @This(), purpose: Purpose, records: u64, rows: u64, columns: u16) !void {
        try self.expected.validate();
        try self.observed.validate();
        const maximum = std.math.mul(u64, rows, columns) catch return error.InvalidSchemaValidationJob;
        if (purpose == .not_null or self.expected.count > maximum or self.observed.count > records or
            (purpose == .foreign_key_source and self.observed.count != 0) or
            (purpose == .foreign_key_parent and self.expected.count != 0)) return error.InvalidSchemaValidationJob;
    }
};

pub const Binding = struct {
    purpose: Purpose = .not_null,
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

    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return std.mem.eql(u8, field, "purpose") and self.purpose == .not_null;
    }
    pub fn jsonStringify(self: @This(), writer: anytype) @TypeOf(writer.*).Error!void {
        return writeFields(self, writer);
    }
    pub fn validate(self: Binding) !void {
        _ = try self.fence.encode();
        if (self.fence.role != .schema_validation or self.fence.owner_group_id != self.fence.peer_group_id or
            self.fence.namespace.shard_id == 0 or self.fence.namespace.range_id == 0 or
            self.columns == 0 or self.columns > max_columns or std.mem.allEqual(u8, &self.id, 0) or
            self.after_version != (if (self.purpose == .foreign_key_parent) self.before_version else (std.math.add(u32, self.before_version, 1) catch return error.InvalidSchemaValidationJob)))
            return error.InvalidSchemaValidationJob;
        if (self.purpose == .foreign_key_parent and (!eq(self.before_schema, self.after_schema) or
            !eq(self.before_public, self.after_public) or !eq(self.fence.catalog_digest, self.after_catalog))) return error.InvalidSchemaValidationJob;
        inline for (.{ "plan_digest", "owner", "before_schema", "after_schema", "before_public", "after_public", "after_catalog", "physical_catalog", "inputs", "projection_identity" }) |field|
            if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidSchemaValidationJob;
    }
};

pub const Phase = enum { fencing, scanning, ready, failed, canceling, canceled, installed, released };
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
/// Immutable terminal predecessor, retained after admission is lifted. The
/// coordinator's publication digest is distinct from the install decision.
pub const Release = struct {
    publication: Digest,
    installed_digest: Digest,
    installed_last_effect: Digest,
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
    release: ?Release = null,
    reference_coverage: ?ReferenceCoverage = null,
    /// Completed ordinary FK transactions, distinct from a read-only reference
    /// witness. The actual record is checked again under the publication fence.
    transaction_coverage: ?struct { record: Digest, physical_root: u128 } = null,

    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return (std.mem.eql(u8, field, "reference_coverage") and self.reference_coverage == null) or
            (std.mem.eql(u8, field, "transaction_coverage") and self.transaction_coverage == null);
    }
    pub fn jsonStringify(self: @This(), writer: anytype) @TypeOf(writer.*).Error!void {
        return writeFields(self, writer);
    }
    pub fn validate(self: Header) !void {
        try self.binding.validate();
        if (self.transaction_coverage) |coverage| {
            if (self.binding.purpose != .foreign_key_source or self.reference_coverage != null or
                std.mem.allEqual(u8, &coverage.record, 0) or ((self.binding.authority == .native) != (coverage.physical_root != 0)))
                return error.InvalidSchemaValidationJob;
        } else if ((self.binding.purpose == .not_null) != (self.reference_coverage == null)) return error.InvalidSchemaValidationJob;
        if (self.reference_coverage) |coverage| try coverage.validate(self.binding.purpose, self.records, self.rows_scanned, self.binding.columns);
        inline for (.{ "cursor_digest", "chain", "last_effect" }) |field|
            if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidSchemaValidationJob;
        if (self.revision == 0 or self.rows_scanned > self.records or
            (self.phase == .failed) != (self.failure_column != null) or
            (self.failure_column != null and self.failure_column.? >= self.binding.columns) or
            (self.phase != .canceling and self.phase != .canceled and self.admitted == null) or
            (self.frozen != null and self.admitted == null) or
            ((self.phase == .installed or self.phase == .released) != (self.installation != null)) or
            ((self.phase == .released) != (self.release != null))) return error.InvalidSchemaValidationJob;
        if (self.phase == .fencing and (self.frozen != null or self.records != 0 or self.rows_scanned != 0 or !eq(self.cursor_digest, hash(""))))
            return error.InvalidSchemaValidationJob;
        if ((self.phase == .scanning or self.phase == .ready or self.phase == .failed or self.phase == .installed or self.phase == .released) and self.frozen == null) return error.InvalidSchemaValidationJob;
        for ([_]?positions.Position{ self.admitted, self.frozen, self.updated }) |maybe| if (maybe) |position| {
            try position.requireNamespace(self.binding.fence.namespace);
            if ((position == .native) != (self.binding.authority == .native)) return error.InvalidControlReceiptPosition;
            try precedesOrEqual(position, self.updated);
        };
        if (self.frozen) |cut| try precedes(self.admitted.?, cut);
        if (self.release) |released| {
            inline for (.{ "publication", "installed_digest", "installed_last_effect" }) |field|
                if (std.mem.allEqual(u8, &@field(released, field), 0)) return error.InvalidSchemaValidationJob;
            var predecessor = self;
            predecessor.phase = .installed;
            predecessor.release = null;
            predecessor.updated = self.installation.?.position;
            predecessor.last_effect = released.installed_last_effect;
            predecessor.revision = std.math.sub(u64, self.revision, 1) catch return error.InvalidSchemaValidationJob;
            try predecessor.validate();
            try precedes(predecessor.updated, self.updated);
            if (!eq(try predecessor.digest(), released.installed_digest)) return error.InvalidSchemaValidationJob;
            return;
        }
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
    reference_coverage: ?ReferenceCoverage = null,
    pub const max_wire_bytes = 48 + max_header + max_cursor;
    const Descriptor = struct {
        binding: Binding,
        expected: Digest,
        records: usize,
        rows: usize,
        violation: ?usize,
        more: bool,
        reference_coverage: ?ReferenceCoverage = null,

        pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
            return std.mem.eql(u8, field, "reference_coverage") and self.reference_coverage == null;
        }
        pub fn jsonStringify(self: @This(), writer: anytype) @TypeOf(writer.*).Error!void {
            return writeFields(self, writer);
        }
    };
    pub fn validate(self: @This()) !void {
        try self.binding.validate();
        try self.boundary.validate();
        if ((self.binding.purpose == .not_null) != (self.reference_coverage == null)) return error.InvalidSchemaValidationPageCommand;
        if (self.reference_coverage) |coverage| try coverage.validate(self.binding.purpose, self.boundary.records_examined, self.boundary.rows_examined, self.binding.columns);
        if (std.mem.allEqual(u8, &self.expected, 0) or
            (self.boundary.violation_column != null and self.boundary.violation_column.? >= self.binding.columns)) return error.InvalidSchemaValidationPageCommand;
    }
    pub fn effect(self: @This()) !Digest {
        try self.validate();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update(if (self.reference_coverage == null) "schema-validation-page/v1" else "schema-validation-page/v2");
        state.update(&self.expected);
        state.update(&hash(self.boundary.after));
        var numbers: [25]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.boundary.rows_examined, .little);
        std.mem.writeInt(u64, numbers[8..16], self.boundary.records_examined, .little);
        std.mem.writeInt(u64, numbers[16..24], if (self.boundary.violation_column) |column| column else std.math.maxInt(u64), .little);
        numbers[24] = @intFromBool(self.boundary.more);
        state.update(&numbers);
        if (self.reference_coverage) |coverage| {
            for ([_]@import("relational_reference_witness.zig").Witness{ coverage.expected, coverage.observed }) |witness| {
                var count: [8]u8 = undefined;
                std.mem.writeInt(u64, &count, witness.count, .little);
                state.update(&count);
                state.update(&witness.sum);
            }
        }
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }
    pub fn encode(self: @This(), a: A) ![]u8 {
        try self.validate();
        const descriptor: Descriptor = .{ .binding = self.binding, .expected = self.expected, .records = self.boundary.records_examined, .rows = self.boundary.rows_examined, .violation = self.boundary.violation_column, .more = self.boundary.more, .reference_coverage = self.reference_coverage };
        var buffer: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(descriptor, .{}, &writer);
        const metadata = writer.buffered();
        const bytes = try a.alloc(u8, 48 + metadata.len + self.boundary.after.len);
        @memcpy(bytes[0..8], if (self.reference_coverage == null) "ASVP\x01\x00\x00\x00" else "ASVP\x02\x00\x00\x00");
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
        if (bytes.len < 48 or bytes.len > max_wire_bytes or (!std.mem.eql(u8, bytes[0..8], "ASVP\x01\x00\x00\x00") and !std.mem.eql(u8, bytes[0..8], "ASVP\x02\x00\x00\x00")) or
            !eq(hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*)) return error.InvalidSchemaValidationPageCommand;
        const metadata_len: usize = std.mem.readInt(u32, bytes[8..12], .little);
        const cursor_len: usize = std.mem.readInt(u32, bytes[12..16], .little);
        if (metadata_len == 0 or metadata_len > max_header or cursor_len > max_cursor or metadata_len + cursor_len != bytes.len - 48)
            return error.InvalidSchemaValidationPageCommand;
        var buffer: [64 * 1024]u8 = undefined;
        var scratch = std.heap.FixedBufferAllocator.init(&buffer);
        const descriptor = std.json.parseFromSliceLeaky(Descriptor, scratch.allocator(), bytes[16..][0..metadata_len], .{}) catch return error.InvalidSchemaValidationPageCommand;
        if ((bytes[4] == 1) != (descriptor.reference_coverage == null)) return error.InvalidSchemaValidationPageCommand;
        var canonical: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&canonical);
        try std.json.Stringify.value(descriptor, .{}, &writer);
        if (!std.mem.eql(u8, writer.buffered(), bytes[16..][0..metadata_len])) return error.InvalidSchemaValidationPageCommand;
        const result: @This() = .{ .binding = descriptor.binding, .expected = descriptor.expected, .reference_coverage = descriptor.reference_coverage, .boundary = .{ .records_examined = descriptor.records, .rows_examined = descriptor.rows, .violation_column = descriptor.violation, .more = descriptor.more, .after = bytes[16 + metadata_len ..][0..cursor_len] } };
        try result.validate();
        return result;
    }
};

/// Raw immutable inputs shared by metadata dispatch and physical preparation.
/// Encoding validates framing/bounds, not schema semantics or authorization.
pub fn encodeInputs(a: A, parts: [4][]const u8) ![]u8 {
    const limits = [_]usize{ max_schema, max_schema, integrity.max_catalog_bytes, max_names };
    var length: usize = 56;
    for (parts, limits) |part, limit| {
        if (part.len == 0 or part.len > limit) return error.InvalidSchemaValidationJob;
        length += part.len;
    }
    const bytes = try a.alloc(u8, length);
    @memcpy(bytes[0..8], "ASVI\x01\x00\x00\x00");
    var offset: usize = 24;
    for (parts, 0..) |part, i| {
        std.mem.writeInt(u32, bytes[8 + 4 * i ..][0..4], @intCast(part.len), .little);
        @memcpy(bytes[offset..][0..part.len], part);
        offset += part.len;
    }
    @memcpy(bytes[offset..][0..32], &hash(bytes[0..offset]));
    return bytes;
}

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

test "schema validation publication immutable inputs remain raw bounded and allocation atomic" {
    const parts = [4][]const u8{ "{\"version\":1}", "{\"version\":2}", "\x00\xffcatalog", "[\"n\"]" };
    const Check = struct {
        fn run(a: A, values: [4][]const u8) !void {
            const bytes = try encodeInputs(a, values);
            defer a.free(bytes);
            const decoded = try inputParts(bytes);
            for (values, decoded) |expected, actual| try std.testing.expectEqualSlices(u8, expected, actual);
            bytes[bytes.len - 1] ^= 1;
            try std.testing.expectError(error.InvalidSchemaValidationJob, inputParts(bytes));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{parts});
    var invalid = parts;
    invalid[3] = "";
    try std.testing.expectError(error.InvalidSchemaValidationJob, encodeInputs(std.testing.failing_allocator, invalid));
    invalid = parts;
    invalid[3] = @as([*]const u8, @ptrFromInt(1))[0 .. max_names + 1];
    try std.testing.expectError(error.InvalidSchemaValidationJob, encodeInputs(std.testing.failing_allocator, invalid));
}
/// Read-only pre-admission facts from one retained physical owner cut. A null
/// position denotes an actually empty source clock, never a synthetic receipt.
/// The provisional plan digest is deliberately excluded from intent identity:
/// the final immutable cohort commits all preparations before it can exist.
pub const PreparationDescriptor = struct {
    purpose: Purpose = .not_null,
    id: [16]u8,
    provisional_plan: Digest,
    fence: topology.Fence,
    authority: @import("../source_authority.zig").Kind,

    pub fn nativeJsonSkipField(self: @This(), comptime field: []const u8) bool {
        return std.mem.eql(u8, field, "purpose") and self.purpose == .not_null;
    }
    pub fn jsonStringify(self: @This(), writer: anytype) @TypeOf(writer.*).Error!void {
        return writeFields(self, writer);
    }
    pub fn validate(self: @This()) !void {
        _ = try self.fence.encode();
        if (std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.provisional_plan, 0) or
            self.fence.role != .schema_validation or self.fence.owner_group_id != self.fence.peer_group_id or
            self.fence.namespace.shard_id == 0 or self.fence.namespace.range_id == 0) return error.InvalidSchemaValidationJob;
    }
};
pub const Preparation = struct {
    pub const max_wire_bytes = 16 * 1024;
    binding: Binding,
    physical_root_incarnation: u128,
    position: ?positions.Position,
    replication_pending: bool = false,

    pub fn validate(self: @This()) !void {
        try self.binding.validate();
        if (self.physical_root_incarnation == 0) return error.SchemaValidationJobChanged;
        if (self.position) |position| {
            try position.requireNamespace(self.binding.fence.namespace);
            if ((position == .native) != (self.binding.authority == .native)) return error.InvalidControlReceiptPosition;
        }
    }
    pub fn requireFor(self: @This(), binding: Binding) !void {
        try self.validate();
        try binding.validate();
        var provisional = self.binding;
        provisional.plan_digest = binding.plan_digest;
        if (!std.meta.eql(provisional, binding)) return error.SchemaValidationJobChanged;
    }
};

/// Bounded owner-local facts, not a metadata grant or a signed readiness ACK.
/// The receiver qualifies the directory against the complete immutable binding.
pub const TransactionDebtObservation = struct {
    revision: u64,
    debt: u64,
    ready: bool,
    retired: bool,
    expected: Digest,

    pub fn validate(self: @This()) !void {
        if (self.revision == 0 or std.mem.allEqual(u8, &self.expected, 0) or
            (self.retired and (self.ready or self.debt != 0))) return error.SchemaValidationJobChanged;
    }
};

pub const NativeObservation = struct {
    pub const max_wire_bytes = 32 * 1024;
    binding: Binding,
    physical_root_incarnation: u128,
    sequence: u64,
    header: ?Header,
    replication_pending: bool,
    transaction_debt: ?TransactionDebtObservation = null,

    pub fn validateFor(self: @This(), binding: Binding) !void {
        try binding.validate();
        if (binding.authority != .native or self.physical_root_incarnation == 0 or
            !std.meta.eql(self.binding, binding)) return error.SchemaValidationJobChanged;
        if (self.header) |header| {
            try header.validate();
            if (!std.meta.eql(header.binding, binding) or header.updated != .native or
                header.updated.native.sequence > self.sequence) return error.SchemaValidationJobChanged;
        }
        if (self.transaction_debt) |directory| try directory.validate();
    }
};

/// Native effect acknowledgement, not a signed phase/publication receipt.
/// Decoders must own or copy error_name before releasing their JSON storage.
pub const NativeOutcome = struct {
    header: ?Header,
    position: positions.Native,
    duplicate: bool,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,

    pub fn validateFor(self: @This(), binding: Binding) !void {
        try binding.validate();
        try self.position.validate();
        if (binding.authority != .native) return error.InvalidControlReceiptPosition;
        if (!self.position.namespace.eql(binding.fence.namespace)) return error.IdentityNamespaceMismatch;
        if (self.header) |header| {
            try header.validate();
            if (!std.meta.eql(header.binding, binding)) return error.SchemaValidationJobChanged;
            try precedesOrEqual(.{ .native = self.position }, header.updated);
        }
    }
};

/// Read from an actual retained owner snapshot. The applied entry is supplied
/// by the storage owner, never by the observer request or a metadata report.
/// An empty log is represented by null, not a fabricated term/index pair.
pub const OwnerState = struct {
    namespace: @import("doc_identity_namespace.zig").Namespace,
    physical_root_incarnation: u128,
    header: ?Header,
};

pub const RaftObservation = struct {
    pub const max_wire_bytes = 32 * 1024;
    binding: Binding,
    physical_root_incarnation: u128,
    applied: ?@FieldType(positions.Position, "raft"),
    header: ?Header,
    transaction_debt: ?TransactionDebtObservation = null,

    pub fn validateFor(self: @This(), binding: Binding) !void {
        try binding.validate();
        if (binding.authority != .raft or self.physical_root_incarnation == 0 or
            !std.meta.eql(self.binding, binding)) return error.SchemaValidationJobChanged;
        if (self.applied) |cut| try (positions.Position{ .raft = cut }).validate();
        if (self.transaction_debt) |directory| try directory.validate();
        if (self.header) |header| {
            try header.validate();
            if (!std.meta.eql(header.binding, binding) or header.updated != .raft)
                return error.SchemaValidationJobChanged;
            const applied = self.applied orelse return error.InvalidControlReceiptPosition;
            try precedesOrEqual(header.updated, .{ .raft = applied });
        }
    }
};

pub const Action = enum { begin, reconcile_transactions, freeze, page, cancel, install, release };

pub fn transactionDebtEffect(expected: Digest) Digest {
    const domain = "schema-validation-transaction-debt/v1";
    var bytes: [domain.len + 32]u8 = undefined;
    @memcpy(bytes[0..domain.len], domain);
    @memcpy(bytes[domain.len..], &expected);
    return hash(&bytes);
}
pub const CommandDescriptor = struct { binding: Binding, action: Action };
pub const raft_magic = "ASVR\x01\x00\x00\x00";

pub const Command = struct {
    pub const max_raft_wire_bytes = 48 + max_header + max_input_bytes;
    binding: Binding,
    action: Action,
    /// Borrowed immutable ASVI inputs, ASVP page, install decision+ready
    /// digests, or install decision+installed digest+publication digest.
    payload: []const u8 = "",

    /// Exact canonical ASVR identity, prepared off the serialized apply lane.
    /// Stream the immutable payload rather than allocating a second frame.
    pub fn raftIdentity(self: @This()) !Digest {
        try self.validateFor(.raft);
        var buffer: [max_header]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        try std.json.Stringify.value(CommandDescriptor{ .binding = self.binding, .action = self.action }, .{}, &writer);
        var prefix: [16]u8 = undefined;
        @memcpy(prefix[0..8], raft_magic);
        std.mem.writeInt(u32, prefix[8..12], @intCast(writer.buffered().len), .little);
        std.mem.writeInt(u32, prefix[12..16], @intCast(self.payload.len), .little);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update(&prefix);
        state.update(writer.buffered());
        state.update(self.payload);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }

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
            .reconcile_transactions => if (self.payload.len != 32 or std.mem.allEqual(u8, self.payload, 0)) return error.InvalidSchemaOwnerControl,
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
            .release => if (self.payload.len != 96 or std.mem.allEqual(u8, self.payload[0..32], 0) or
                std.mem.allEqual(u8, self.payload[32..64], 0) or std.mem.allEqual(u8, self.payload[64..96], 0)) return error.InvalidSchemaOwnerControl,
        }
    }
};

const ValidationContractFixture = struct {
    fn binding() Binding {
        return .{ .id = @splat(1), .plan_digest = @splat(2), .owner = @splat(3), .fence = .{ .transition_id = 4, .attempt = 1, .admission_epoch = 1, .owner_group_id = 72, .peer_group_id = 72, .namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .catalog_digest = @splat(5), .role = .schema_validation }, .authority = .native, .before_version = 1, .after_version = 2, .columns = 1, .before_schema = @splat(6), .after_schema = @splat(7), .before_public = @splat(8), .after_public = @splat(9), .after_catalog = @splat(10), .physical_catalog = @splat(11), .inputs = @splat(12), .projection_identity = @splat(13) };
    }
    fn legacyBinding(b: Binding) @TypeOf(.{ .id = b.id, .plan_digest = b.plan_digest, .owner = b.owner, .fence = b.fence, .authority = b.authority, .before_version = b.before_version, .after_version = b.after_version, .columns = b.columns, .before_schema = b.before_schema, .after_schema = b.after_schema, .before_public = b.before_public, .after_public = b.after_public, .after_catalog = b.after_catalog, .physical_catalog = b.physical_catalog, .inputs = b.inputs, .projection_identity = b.projection_identity }) {
        return .{ .id = b.id, .plan_digest = b.plan_digest, .owner = b.owner, .fence = b.fence, .authority = b.authority, .before_version = b.before_version, .after_version = b.after_version, .columns = b.columns, .before_schema = b.before_schema, .after_schema = b.after_schema, .before_public = b.before_public, .after_public = b.after_public, .after_catalog = b.after_catalog, .physical_catalog = b.physical_catalog, .inputs = b.inputs, .projection_identity = b.projection_identity };
    }
    fn ready(b: Binding) Header {
        return .{ .binding = b, .phase = .ready, .admitted = .{ .native = .{ .namespace = b.fence.namespace, .sequence = 1 } }, .frozen = .{ .native = .{ .namespace = b.fence.namespace, .sequence = 2 } }, .updated = .{ .native = .{ .namespace = b.fence.namespace, .sequence = 3 } }, .revision = 3, .records = 2, .rows_scanned = 2, .cursor_digest = hash("cursor"), .chain = @splat(14), .last_effect = @splat(15) };
    }
};

test "foreign key validation owner roles retain existing NOT NULL bytes and receipt identities" {
    const a = std.testing.allocator;
    const b = ValidationContractFixture.binding();
    try b.validate();
    const legacy = try std.json.Stringify.valueAlloc(a, ValidationContractFixture.legacyBinding(b), .{});
    defer a.free(legacy);
    const current = try std.json.Stringify.valueAlloc(a, b, .{});
    defer a.free(current);
    try std.testing.expectEqualSlices(u8, legacy, current);
    const header = ValidationContractFixture.ready(b);
    const old_header = try std.json.Stringify.valueAlloc(a, .{ .binding = ValidationContractFixture.legacyBinding(b), .phase = header.phase, .admitted = header.admitted, .frozen = header.frozen, .updated = header.updated, .revision = header.revision, .records = header.records, .rows_scanned = header.rows_scanned, .cursor_digest = header.cursor_digest, .chain = header.chain, .last_effect = header.last_effect, .failure_column = header.failure_column, .installation = header.installation, .release = header.release }, .{});
    defer a.free(old_header);
    const encoded = try header.encode(a);
    defer a.free(encoded);
    try std.testing.expectEqualSlices(u8, old_header, encoded[12 .. encoded.len - 32]);
    try std.testing.expectEqualSlices(u8, &hash(old_header), &try header.digest());
    try std.testing.expectEqualDeep(header, try Header.decode(encoded));
    const page: PageCommand = .{ .binding = b, .expected = try header.digest(), .boundary = .{ .records_examined = 2, .rows_examined = 2, .violation_column = null, .after = "\x00\xffcursor", .more = true } };
    const frame = try page.encode(a);
    defer a.free(frame);
    try std.testing.expectEqual(@as(u8, 1), frame[4]);
    const old_descriptor = try std.json.Stringify.valueAlloc(a, .{ .binding = ValidationContractFixture.legacyBinding(b), .expected = page.expected, .records = page.boundary.records_examined, .rows = page.boundary.rows_examined, .violation = page.boundary.violation_column, .more = page.boundary.more }, .{});
    defer a.free(old_descriptor);
    try std.testing.expectEqualSlices(u8, old_descriptor, frame[16..][0..std.mem.readInt(u32, frame[8..12], .little)]);
    const decoded = try PageCommand.decode(frame);
    try std.testing.expectEqualSlices(u8, &try page.effect(), &try decoded.effect());
}

test "foreign key validation owner coverage binds source parent and dual roles without authority conversion" {
    const a = std.testing.allocator;
    var b = ValidationContractFixture.binding();
    b.purpose = .foreign_key_source;
    var header = ValidationContractFixture.ready(b);
    try std.testing.expectError(error.InvalidSchemaValidationJob, header.validate());
    header.reference_coverage = .{ .expected = .{ .count = 2, .sum = @splat(4) } };
    try header.validate();
    const source = try header.encode(a);
    defer a.free(source);
    try std.testing.expectEqualDeep(header, try Header.decode(source));
    header.binding.purpose = .not_null;
    try std.testing.expectError(error.InvalidSchemaValidationJob, header.validate());
    header.binding.purpose = .foreign_key_parent;
    try std.testing.expectError(error.InvalidSchemaValidationJob, header.validate());
    header.binding.after_version = header.binding.before_version;
    header.binding.after_schema = header.binding.before_schema;
    header.binding.after_public = header.binding.before_public;
    header.binding.after_catalog = header.binding.fence.catalog_digest;
    try std.testing.expectError(error.InvalidSchemaValidationJob, header.validate());
    header.reference_coverage = .{ .observed = .{ .count = 2, .sum = @splat(4) } };
    try header.validate();
    var parent = header.binding;
    parent.after_schema[0] ^= 1;
    try std.testing.expectError(error.InvalidSchemaValidationJob, parent.validate());
    header = ValidationContractFixture.ready(b);
    header.binding.purpose = .foreign_key_dual;
    header.reference_coverage = .{ .expected = .{ .count = 2, .sum = @splat(4) }, .observed = .{ .count = 2, .sum = @splat(4) } };
    try header.validate();
    header.reference_coverage.?.observed.count = 3;
    try std.testing.expectError(error.InvalidSchemaValidationJob, header.validate());
}

test "foreign key validation owner page proofs bind witness bytes and reject frame version laundering" {
    const a = std.testing.allocator;
    var b = ValidationContractFixture.binding();
    b.purpose = .foreign_key_source;
    const page: PageCommand = .{ .binding = b, .expected = @splat(3), .boundary = .{ .records_examined = 2, .rows_examined = 2, .violation_column = null, .after = "\x00\xffcursor", .more = true }, .reference_coverage = .{ .expected = .{ .count = 2, .sum = @splat(4) } } };
    const bytes = try page.encode(a);
    defer a.free(bytes);
    try std.testing.expectEqual(@as(u8, 2), bytes[4]);
    const decoded = try PageCommand.decode(bytes);
    try std.testing.expectEqualDeep(page.reference_coverage, decoded.reference_coverage);
    try std.testing.expectEqualSlices(u8, page.boundary.after, decoded.boundary.after);
    try std.testing.expectEqualSlices(u8, &try page.effect(), &try decoded.effect());
    var changed = page;
    changed.reference_coverage.?.expected.sum[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &try page.effect(), &try changed.effect()));
    changed.reference_coverage = null;
    try std.testing.expectError(error.InvalidSchemaValidationPageCommand, changed.validate());
    bytes[4] = 1;
    @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
    try std.testing.expectError(error.InvalidSchemaValidationPageCommand, PageCommand.decode(bytes));
}

test "foreign key validation transaction coverage cannot substitute witness or root authority" {
    const a = std.testing.allocator;
    var header = ValidationContractFixture.ready(ValidationContractFixture.binding());
    header.binding.purpose = .foreign_key_source;
    header.transaction_coverage = .{ .record = @splat(16), .physical_root = 17 };
    try header.validate();
    const bytes = try header.encode(a);
    defer a.free(bytes);
    try std.testing.expectEqualDeep(header, try Header.decode(bytes));
    var forged = header;
    forged.reference_coverage = .{};
    try std.testing.expectError(error.InvalidSchemaValidationJob, forged.validate());
    forged = header;
    forged.transaction_coverage.?.physical_root = 0;
    try std.testing.expectError(error.InvalidSchemaValidationJob, forged.validate());
    forged = header;
    forged.transaction_coverage.?.record = @splat(0);
    try std.testing.expectError(error.InvalidSchemaValidationJob, forged.validate());
    for ([_]Purpose{ .not_null, .foreign_key_parent, .foreign_key_dual }) |purpose| {
        forged = header;
        forged.binding.purpose = purpose;
        try std.testing.expectError(error.InvalidSchemaValidationJob, forged.validate());
    }
}

test "foreign key validation source accepts initial parent epoch without weakening identity" {
    var parents = [_]ForeignKeyParent{.{ .constraint_name = "fk", .declared_parent = "users", .parent_table = "users", .parent_table_id = 88, .parent_schema_version = 0, .parent_schema = @splat(1), .parent_public = @splat(2), .parent_catalog = @splat(3), .parent_unique = "pk", .parent_generation = @splat(4) }};
    const source: ForeignKeySource = .{ .table_name = "usage_records", .parents = &parents };
    try source.validate();
    const original = parents[0];
    parents[0].parent_table_id = 0;
    try std.testing.expectError(error.InvalidSchemaValidationJob, source.validate());
    parents[0] = original;
    parents[0].parent_generation = @splat(0);
    try std.testing.expectError(error.InvalidSchemaValidationJob, source.validate());
    inline for (.{ "parent_schema", "parent_public", "parent_catalog" }) |field| {
        parents[0] = original;
        @field(parents[0], field) = @splat(0);
        try std.testing.expectError(error.InvalidSchemaValidationJob, source.validate());
    }
}
