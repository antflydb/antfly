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

//! Owned sequence defaults are preparation effects, never scalar bytecode.
//! Names are diagnostic; immutable catalog/attribute identities grant binding.
const std = @import("std");
const sequence = @import("sequences.zig");
const domain = @import("domain.zig");
const A = std.mem.Allocator;

pub const Generation = @import("antfly_schema_openapi").RelationalColumnSequenceGeneration;

/// Compiler intent, consumed only inside the containing table publication.
/// Ingress cannot supply an allocated sequence or attribute identity.
pub const Creation = struct {
    column: []const u8,
    generation: Generation,
    options: sequence.Options,

    pub fn validate(self: Creation) !void {
        try domain.validateTableName(self.column);
        if (self.options.owned_by != .unchanged or self.options.restart != null or self.options.integer == null)
            return error.InvalidSequenceDefinition;
        _ = try sequence.Definition.prepare(null, self.options);
    }
};

/// The schema owns one compact, immutable reference. Neither a column ordinal
/// nor a caller-provided name can authorize an allocation from another table.
pub const Binding = struct {
    generation: Generation,
    incarnation: [16]u8,
    sequence_id: u64,
    namespace_id: u64,
    owner: sequence.OwnedBy,
    target: domain.Target,

    pub fn validate(self: Binding) !void {
        if (self.sequence_id == 0 or self.namespace_id == 0 or std.mem.allEqual(u8, &self.incarnation, 0))
            return error.InvalidSequenceDefinition;
        try self.owner.validate();
        try self.target.validate();
    }

    /// Public identities are decimal strings, retaining every native u64 bit.
    /// The catalog incarnation is a fixed hexadecimal value, not a SQL OID.
    pub fn parse(backing: A, value: std.json.Value) !Owned {
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const a = arena.allocator();
        const Wire = struct {
            generation: Generation,
            incarnation: []const u8,
            sequence_id: []const u8,
            namespace_id: []const u8,
            table_id: []const u8,
            column_id: []const u8,
            database: []const u8,
            namespace: []const u8,
            name: []const u8,
        };
        if (value != .object or value.object.count() != 9) return error.InvalidSequenceDefinition;
        for (value.object.values()) |cell| {
            if (cell != .string or cell.string.len > 1024) return error.InvalidSequenceDefinition;
        }
        const wire = try std.json.parseFromValueLeaky(Wire, a, value, .{ .allocate = .alloc_always });
        if (wire.incarnation.len != 32) return error.InvalidSequenceDefinition;
        var incarnation: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&incarnation, wire.incarnation) catch return error.InvalidSequenceDefinition;
        const result: Binding = .{
            .generation = wire.generation,
            .incarnation = incarnation,
            .sequence_id = try id(wire.sequence_id),
            .namespace_id = try id(wire.namespace_id),
            .owner = .{ .table_id = try id(wire.table_id), .column_id = try id(wire.column_id) },
            .target = .{ .database = wire.database, .namespace = wire.namespace, .table = wire.name },
        };
        try result.validate();
        return .{ .arena = arena, .value = result };
    }

    fn id(text: []const u8) !u64 {
        const value = std.fmt.parseInt(u64, text, 10) catch return error.InvalidSequenceDefinition;
        if (value == 0 or text.len == 0 or (text.len > 1 and text[0] == '0')) return error.InvalidSequenceDefinition;
        for (text) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidSequenceDefinition;
        return value;
    }

    pub fn jsonStringify(self: Binding, writer: anytype) !void {
        var ids: [4][20]u8 = undefined;
        try writer.write(.{
            .generation = self.generation,
            .incarnation = &std.fmt.bytesToHex(self.incarnation, .lower),
            .sequence_id = std.fmt.bufPrint(&ids[0], "{d}", .{self.sequence_id}) catch unreachable,
            .namespace_id = std.fmt.bufPrint(&ids[1], "{d}", .{self.namespace_id}) catch unreachable,
            .table_id = std.fmt.bufPrint(&ids[2], "{d}", .{self.owner.table_id}) catch unreachable,
            .column_id = std.fmt.bufPrint(&ids[3], "{d}", .{self.owner.column_id}) catch unreachable,
            .database = self.target.database,
            .namespace = self.target.namespace,
            .name = self.target.table,
        });
    }

    /// Copies only the diagnostic strings into the caller's owning region.
    pub fn cloneLeaky(self: Binding, a: A) !*const Binding {
        const binding = try a.create(Binding);
        binding.* = self;
        binding.target.database = try a.dupe(u8, self.target.database);
        binding.target.namespace = try a.dupe(u8, self.target.namespace);
        binding.target.table = try a.dupe(u8, self.target.table);
        return binding;
    }

    /// Renaming a sequence does not change the logical default expression.
    pub fn fingerprint(self: Binding) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.column-sequence-default.v1\x00");
        hash.update(&self.incarnation);
        hash.update(@tagName(self.generation));
        inline for (.{ self.sequence_id, self.namespace_id, self.owner.table_id, self.owner.column_id }) |number| {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, number, .big);
            hash.update(&bytes);
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }
};

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    value: Binding,

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Actual immutable row-preparation scope, supplied by a pinned table view.
/// It is independent of the schema's untrusted/descriptive sequence reference.
pub const Scope = struct {
    table_id: u64,
    schema_version: u32,
    storage_name: []const u8,
};

pub const Provider = struct {
    ptr: *anyopaque,
    /// Resolve and reauthorize the current immutable identity/owner before
    /// consuming a cache value or admitting a durable counter reservation.
    allocate: *const fn (*anyopaque, A, Scope, []const u8, Binding) anyerror!i64,
};
