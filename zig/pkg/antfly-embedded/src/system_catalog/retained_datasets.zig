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

//! Compiled retained-resource metadata. Logical catalog names live in the
//! enclosing resource; physical source bindings and portable dataset identity
//! do not change when either logical name is renamed.
const std = @import("std");
const datasets = @import("../retained_dataset_types.zig");
const incarnation = @import("../metadata/incarnation.zig");
const A = std.mem.Allocator;
pub const Id = datasets.Id;
pub const max_bytes = 32 * 1024;
pub const Phase = enum { activating, active, closing, closed, retiring };

pub const Source = struct {
    metadata_incarnation: [32]u8,
    table_id: u64,
    storage_name: []const u8,
    schema: Id,
    /// Zero denotes a schemaless document source.
    schema_version: u32,

    pub fn jsonStringify(self: Source, writer: anytype) !void {
        try datasets.writeFields(self, .{}, writer);
    }
    pub fn fromTable(a: A, table: anytype, cluster: incarnation.MetadataClusterIncarnation) !Source {
        var schema: Id = undefined;
        std.crypto.hash.sha2.Sha256.hash(table.schema_json, &schema, .{});
        const Header = struct { version: u32 = 0 };
        const version = if (table.schema_json.len == 0) 0 else result: {
            var header = try std.json.parseFromSlice(Header, a, table.schema_json, .{ .ignore_unknown_fields = true });
            defer header.deinit();
            break :result header.value.version;
        };
        const value: Source = .{ .metadata_incarnation = cluster, .table_id = table.table_id, .storage_name = table.name, .schema = schema, .schema_version = version };
        try value.validate();
        return value;
    }
    pub fn identity(self: Source) Id {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/retained-native-source/v1");
        hash.update(&self.metadata_incarnation);
        var table: [8]u8 = undefined;
        std.mem.writeInt(u64, &table, self.table_id, .big);
        hash.update(&table);
        return hash.finalResult();
    }
    pub fn validate(self: Source) !void {
        if (self.table_id == 0 or self.storage_name.len == 0 or self.storage_name.len > 1024 or
            std.mem.indexOfScalar(u8, self.storage_name, 0) != null or !std.unicode.utf8ValidateSlice(self.storage_name) or
            !incarnation.isValid(self.metadata_incarnation) or std.mem.allEqual(u8, &self.schema, 0))
            return error.InvalidRetainedDatasetResource;
    }
};

/// Transactionally maintained catalog indexes project only these two scalars.
/// The descriptor remains the authority and validates this projection on load.
pub const Identity = struct {
    dataset: Id,
    source_table_id: u64,
    pub fn jsonStringify(self: Identity, writer: anytype) !void {
        try datasets.writeFields(self, .{}, writer);
    }
};

pub const Descriptor = struct {
    format: u8 = 1,
    revision: u64 = 1,
    definition: datasets.Definition,
    /// Optional only after independently owned coverage or explicit retirement
    /// has released the source dependency. Historical lineage stays in the
    /// portable definition after the physical table has disappeared.
    source: ?Source,
    phase: Phase = .activating,
    /// Logical capabilities, never credentials or caller-supplied URIs.
    catalog_location: Id,
    catalog_storage_identity: Id,
    /// Exact verified native/archive publication witness. A digest's presence
    /// is structural validation, not authority to detach a source: admission
    /// must verify the referenced receipts under its metadata fence.
    closed_receipt: ?Id = null,

    pub fn jsonStringify(self: Descriptor, writer: anytype) !void {
        try datasets.writeFields(self, .{}, writer);
    }
    pub fn identity(self: Descriptor) Identity {
        return .{ .dataset = self.definition.id, .source_table_id = if (self.source) |source| source.table_id else 0 };
    }
    pub fn validate(self: Descriptor) !void {
        try self.definition.validate();
        if (self.format != 1 or self.revision == 0 or self.revision > datasets.max_public_counter or
            std.mem.allEqual(u8, &self.catalog_location, 0) or std.mem.allEqual(u8, &self.catalog_storage_identity, 0)) return error.InvalidRetainedDatasetResource;
        if (self.source) |source| {
            const summary = self.definition.summary orelse return error.InvalidRetainedDatasetResource;
            try source.validate();
            if (summary.projection) |projection| if (!std.mem.eql(u8, &projection.source_schema, &source.schema)) return error.InvalidRetainedDatasetResource;
            if (!std.mem.eql(u8, &summary.source, &source.identity()) or self.phase == .closed or self.closed_receipt != null) return error.InvalidRetainedDatasetResource;
        } else if (self.definition.kind == .summary and ((self.phase != .closed and self.phase != .retiring) or self.closed_receipt == null)) return error.InvalidRetainedDatasetResource;
        if (self.phase == .closed and self.closed_receipt == null) return error.InvalidRetainedDatasetResource;
        if (self.closed_receipt != null and self.phase != .closed and self.phase != .retiring) return error.InvalidRetainedDatasetResource;
        if (self.closed_receipt) |receipt| if (std.mem.allEqual(u8, &receipt, 0)) return error.InvalidRetainedDatasetResource;
    }

    pub fn validateCreation(self: Descriptor) !void {
        try self.validate();
        if (self.phase != .activating or self.closed_receipt != null or self.revision != 1 or self.definition.generation != 1) return error.InvalidRetainedDatasetResource;
    }

    /// This validates lifecycle structure only. Receipt verification and source
    /// authorization belong to the caller's fenced metadata admission.
    pub fn validateNext(self: Descriptor, a: A, next: Descriptor) !void {
        try self.validate();
        try next.validate();
        if (self.revision == datasets.max_public_counter or next.revision != self.revision + 1 or
            !std.mem.eql(u8, &self.definition.id, &next.definition.id) or
            !std.mem.eql(u8, &self.catalog_location, &next.catalog_location) or
            !std.mem.eql(u8, &self.catalog_storage_identity, &next.catalog_storage_identity)) return error.RetainedDatasetResourceChanged;
        if (self.closed_receipt != null and !std.meta.eql(self.closed_receipt, next.closed_receipt)) return error.RetainedDatasetResourceChanged;
        if (next.definition.generation != self.definition.generation) return error.RetainedDatasetGenerationTransitionRequired;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        if (!std.mem.eql(u8, &try self.definition.fingerprint(arena.allocator()), &try next.definition.fingerprint(arena.allocator()))) return error.RetainedDatasetResourceChanged;
        if (self.source) |source| {
            if (next.source) |other| {
                if (!std.mem.eql(u8, &source.identity(), &other.identity()) or
                    !std.mem.eql(u8, source.storage_name, other.storage_name) or
                    !std.mem.eql(u8, &source.schema, &other.schema) or source.schema_version != other.schema_version) return error.RetainedDatasetResourceChanged;
            } else if (next.closed_receipt == null) return error.InvalidRetainedDatasetResource;
        } else if (next.source != null) return error.RetainedDatasetResourceChanged;
        const allowed = switch (self.phase) {
            .activating => next.phase == .active or next.phase == .retiring,
            .active => next.phase == .closing or next.phase == .retiring,
            .closing => next.phase == .closed or next.phase == .retiring,
            .closed => next.phase == .retiring,
            .retiring => next.phase == .retiring,
        };
        if (!allowed) return error.InvalidRetainedDatasetResource;
    }
};

pub fn parse(a: A, json: []const u8) !std.json.Parsed(Descriptor) {
    if (json.len == 0 or json.len > max_bytes) return error.InvalidRetainedDatasetResource;
    var value = try std.json.parseFromSlice(Descriptor, a, json, .{ .allocate = .alloc_always });
    errdefer value.deinit();
    try value.value.validate();
    return value;
}

fn fixture() Descriptor {
    const source: Source = .{ .metadata_incarnation = "11111111111111111111111111111111".*, .table_id = 7, .storage_name = "physical-observations", .schema = @splat(9), .schema_version = 1 };
    var schema: Id = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &schema, .{});
    return .{ .definition = .{ .id = @splat(1), .generation = 1, .kind = .summary, .schema = schema, .schema_json = "{}", .authorization = @splat(3), .retention_ns = 1000, .local_residence_ns = 100, .summary = .{ .source = source.identity(), .timestamp = "ts", .interval_ns = 10, .mutable_for_ns = 100, .measures = &.{.{ .name = "n", .law = .count }} } }, .source = source, .catalog_location = @splat(4), .catalog_storage_identity = @splat(5) };
}

test "retained dataset resource lifecycle preserves source identity and requires verified detach staging" {
    const a = std.testing.allocator;
    const creating = fixture();
    try creating.validateCreation();
    var active = creating;
    active.revision += 1;
    active.phase = .active;
    try creating.validateNext(a, active);
    var rebound = active;
    rebound.revision += 1;
    rebound.source.?.storage_name = "another-physical-owner";
    try std.testing.expectError(error.RetainedDatasetResourceChanged, active.validateNext(a, rebound));
    var closed = active;
    closed.revision += 1;
    closed.phase = .closed;
    closed.source = null;
    closed.closed_receipt = @splat(7);
    try std.testing.expectError(error.InvalidRetainedDatasetResource, active.validateNext(a, closed));
    var closing = active;
    closing.revision += 1;
    closing.phase = .closing;
    try active.validateNext(a, closing);
    closed.revision = closing.revision + 1;
    try closing.validateNext(a, closed);
    var retiring = closed;
    retiring.revision += 1;
    retiring.phase = .retiring;
    try closed.validateNext(a, retiring);
    retiring.closed_receipt = @splat(8);
    try std.testing.expectError(error.RetainedDatasetResourceChanged, closed.validateNext(a, retiring));
    var evolved = active;
    evolved.revision += 1;
    evolved.definition.generation += 1;
    try std.testing.expectError(error.RetainedDatasetGenerationTransitionRequired, active.validateNext(a, evolved));
    var corrupt = creating;
    corrupt.source.?.metadata_incarnation = incarnation.zero;
    try std.testing.expectError(error.InvalidRetainedDatasetResource, corrupt.validateCreation());
}

test "retained dataset resource metadata round trips identities and releases admission allocations" {
    const Harness = struct {
        fn run(a: A) !void {
            const initial = fixture();
            const json = try std.json.Stringify.valueAlloc(a, initial, .{});
            defer a.free(json);
            var parsed = try parse(a, json);
            defer parsed.deinit();
            try std.testing.expectEqualDeep(initial.definition.id, parsed.value.definition.id);
            try std.testing.expectEqualDeep(initial.source.?.metadata_incarnation, parsed.value.source.?.metadata_incarnation);
            var active = parsed.value;
            active.phase = .active;
            active.revision += 1;
            try parsed.value.validateNext(a, active);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "retained dataset compiled projections bind source schema without changing legacy definition bytes" {
    const a = std.testing.allocator;
    const legacy = fixture();
    const bytes = try std.json.Stringify.valueAlloc(a, legacy.definition.summary.?, .{});
    defer a.free(bytes);
    var old = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer old.deinit();
    try std.testing.expect(!old.value.object.contains("projection"));
    var compiled = legacy;
    compiled.definition.summary.?.projection = .{ .source_schema = legacy.source.?.schema, .timestamp = .{ .column = "ts" }, .measures = &.{null} };
    try compiled.validateCreation();
    const old_id = try legacy.definition.fingerprint(a);
    const new_id = try compiled.definition.fingerprint(a);
    try std.testing.expect(!std.mem.eql(u8, &old_id, &new_id));
    const encoded = try std.json.Stringify.valueAlloc(a, compiled, .{});
    defer a.free(encoded);
    var decoded = try parse(a, encoded);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(compiled, decoded.value);
    compiled.definition.summary.?.projection.?.source_schema = @splat(19);
    try std.testing.expectError(error.InvalidRetainedDatasetResource, compiled.validate());
}
