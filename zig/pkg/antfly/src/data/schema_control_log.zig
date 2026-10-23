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

//! Compact committed-log routing for private schema controls. The immutable
//! ASVR binding owns the schema/job identity; never repeat full schema/index
//! JSON on every validation page. Neither decoding nor a root generation is
//! metadata permission. The actual applying entry supplies group/term/index.
const std = @import("std");
const contract = @import("antfly_local_sources").storage_db_relational_schema_owner_contract;
const A = std.mem.Allocator;
pub const protocol_version = @import("../common/data_raft_protocol.zig").batch_schema_owner_control_protocol_version;
pub const magic = "ASRL\x01\x00\x00\x00";
const max_table_bytes = 1024;
const max_header_bytes = 8192;
pub const max_wire_bytes = 48 + max_header_bytes + contract.Command.max_raft_wire_bytes;

/// Source-local raw storage generation, not the +1 attestation epoch.
/// A committed entry does not select a destination root: replay independently
/// pins the receiver's visible generation and validates its persisted binding.
const Header = struct { protocol: u16, table: []const u8, root_generation: u64 };

fn hash(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn validateHeader(header: Header) !void {
    if (header.protocol != protocol_version) return error.UnsupportedRaftBatchProtocolVersion;
    if (header.table.len == 0 or header.table.len > max_table_bytes or !std.unicode.utf8ValidateSlice(header.table) or
        std.mem.indexOfScalar(u8, header.table, 0) != null) return error.InvalidReplicatedBatch;
}
/// Recognize the reserved family even for a future or truncated version. A
/// decoder must not reinterpret an unknown schema control as a legacy no-op.
pub fn looksLikeEnvelope(bytes: []const u8) bool {
    return std.mem.startsWith(u8, bytes, "ASRL");
}

pub fn encode(a: A, table: []const u8, root_generation: u64, command_bytes: []const u8) ![]u8 {
    const header: Header = .{ .protocol = protocol_version, .table = table, .root_generation = root_generation };
    try validateHeader(header);
    _ = try contract.Command.decodeRaft(command_bytes);
    var buffer: [max_header_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try std.json.Stringify.value(header, .{}, &writer);
    const metadata = writer.buffered();
    const bytes = try a.alloc(u8, 48 + metadata.len + command_bytes.len);
    @memcpy(bytes[0..8], magic);
    std.mem.writeInt(u32, bytes[8..12], @intCast(metadata.len), .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(command_bytes.len), .little);
    @memcpy(bytes[16..][0..metadata.len], metadata);
    @memcpy(bytes[16 + metadata.len ..][0..command_bytes.len], command_bytes);
    @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
    return bytes;
}

pub const Decoded = struct {
    header: std.json.Parsed(Header),
    /// Borrowed from the immutable committed entry, valid until it is released.
    command_bytes: []const u8,
    command: contract.Command,

    pub fn deinit(self: *@This()) void {
        self.header.deinit();
        self.* = undefined;
    }
    pub fn table(self: *const @This()) []const u8 {
        return self.header.value.table;
    }
    pub fn rootGeneration(self: *const @This()) u64 {
        return self.header.value.root_generation;
    }
};

pub fn decode(a: A, bytes: []const u8) !Decoded {
    if (bytes.len < 48 or bytes.len > max_wire_bytes or !looksLikeEnvelope(bytes)) return error.InvalidReplicatedBatch;
    if (!std.mem.eql(u8, bytes[0..8], magic)) return error.UnsupportedRaftBatchProtocolVersion;
    if (!std.mem.eql(u8, &hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..])) return error.InvalidReplicatedBatch;
    const header_len: usize = std.mem.readInt(u32, bytes[8..12], .little);
    const command_len: usize = std.mem.readInt(u32, bytes[12..16], .little);
    if (header_len == 0 or header_len > max_header_bytes or command_len > contract.Command.max_raft_wire_bytes or
        header_len + command_len != bytes.len - 48) return error.InvalidReplicatedBatch;
    const metadata = bytes[16..][0..header_len];
    var parsed = std.json.parseFromSlice(Header, a, metadata, .{ .allocate = .alloc_always, .ignore_unknown_fields = false }) catch |err|
        return if (err == error.OutOfMemory) err else error.InvalidReplicatedBatch;
    errdefer parsed.deinit();
    try validateHeader(parsed.value);
    var canonical: [max_header_bytes]u8 = undefined;
    var writer = std.Io.Writer.fixed(&canonical);
    try std.json.Stringify.value(parsed.value, .{}, &writer);
    if (!std.mem.eql(u8, writer.buffered(), metadata)) return error.InvalidReplicatedBatch;
    const command_bytes = bytes[16 + header_len ..][0..command_len];
    const command = try contract.Command.decodeRaft(command_bytes);
    return .{ .header = parsed, .command_bytes = command_bytes, .command = command };
}

/// Codec fixtures only. No actual scan, metadata permission, or commit cut is
/// asserted by these synthetic inputs.
pub const testing = if (@import("builtin").is_test) struct {
    pub fn command() contract.Command {
        return .{ .action = .freeze, .binding = .{
            .id = @splat(1),
            .plan_digest = @splat(2),
            .owner = @splat(3),
            .fence = .{ .transition_id = 4, .attempt = 1, .admission_epoch = 1, .owner_group_id = 72, .peer_group_id = 72, .namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .catalog_digest = @splat(5), .role = .schema_validation },
            .authority = .raft,
            .before_version = 1,
            .after_version = 2,
            .columns = 1,
            .before_schema = @splat(6),
            .after_schema = @splat(7),
            .before_public = @splat(8),
            .after_public = @splat(9),
            .after_catalog = @splat(10),
            .physical_catalog = @splat(11),
            .inputs = @splat(12),
            .projection_identity = @splat(13),
        } };
    }
} else struct {};

pub const consumer_tests = consumerTests();
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const owner = @import("antfly_source_root");
    if (@hasDecl(owner, "implementation_tests_only") and owner.implementation_tests_only) return struct {};
    const Suite = struct {
        test "raft schema control log owns bounded routing and borrows immutable command bytes" {
            const a = std.testing.allocator;
            const inner = try testing.command().encodeRaft(a);
            defer a.free(inner);
            const bytes = try encode(a, "röws", std.math.maxInt(u64), inner);
            var bytes_owned = true;
            defer if (bytes_owned) a.free(bytes);
            var parsed = try decode(a, bytes);
            defer parsed.deinit();
            try std.testing.expectEqualDeep(testing.command(), parsed.command);
            try std.testing.expectEqualSlices(u8, inner, parsed.command_bytes);
            try std.testing.expect(@intFromPtr(parsed.command_bytes.ptr) >= @intFromPtr(bytes.ptr));
            try std.testing.expect(@intFromPtr(parsed.command_bytes.ptr) + parsed.command_bytes.len <= @intFromPtr(bytes.ptr) + bytes.len);
            a.free(bytes);
            bytes_owned = false;
            // Routing is owned even though callers must retain the entry for payload.
            try std.testing.expectEqualStrings("röws", parsed.table());
            try std.testing.expectEqual(std.math.maxInt(u64), parsed.rootGeneration());
        }

        test "raft schema control log rejects alternate representations future versions and corrupt lengths" {
            const a = std.testing.allocator;
            const inner = try testing.command().encodeRaft(a);
            defer a.free(inner);
            const bytes = try encode(a, "rows", 11, inner);
            defer a.free(bytes);
            try std.testing.expect(looksLikeEnvelope(bytes));
            try std.testing.expect(looksLikeEnvelope(bytes[0..4]));
            try std.testing.expectError(error.InvalidReplicatedBatch, decode(a, bytes[0..4]));
            bytes[4] = 2;
            try std.testing.expectError(error.UnsupportedRaftBatchProtocolVersion, decode(a, bytes));
            bytes[4] = 1;
            bytes[bytes.len - 1] ^= 1;
            try std.testing.expectError(error.InvalidReplicatedBatch, decode(a, bytes));
            bytes[bytes.len - 1] ^= 1;
            const metadata_len = std.mem.readInt(u32, bytes[8..12], .little);
            const alternate = try a.alloc(u8, bytes.len + 1);
            defer a.free(alternate);
            @memcpy(alternate[0..16], bytes[0..16]);
            alternate[16] = ' ';
            @memcpy(alternate[17..], bytes[16..]);
            std.mem.writeInt(u32, alternate[8..12], metadata_len + 1, .little);
            @memcpy(alternate[alternate.len - 32 ..], &hash(alternate[0 .. alternate.len - 32]));
            try std.testing.expectError(error.InvalidReplicatedBatch, decode(a, alternate));
            std.mem.writeInt(u32, bytes[12..16], std.math.maxInt(u32), .little);
            @memcpy(bytes[bytes.len - 32 ..], &hash(bytes[0 .. bytes.len - 32]));
            try std.testing.expectError(error.InvalidReplicatedBatch, decode(a, bytes));
            try std.testing.expectError(error.InvalidReplicatedBatch, encode(a, "bad\x00name", 11, inner));
        }

        test "raft schema control log unwinds allocation failures without copying schema descriptors" {
            const a = std.testing.allocator;
            const inner = try testing.command().encodeRaft(a);
            defer a.free(inner);
            const bytes = try encode(a, "rows", 11, inner);
            defer a.free(bytes);
            const Check = struct {
                fn run(backing: A, input: []const u8) !void {
                    var parsed = try decode(backing, input);
                    defer parsed.deinit();
                    const again = try encode(backing, parsed.table(), parsed.rootGeneration(), parsed.command_bytes);
                    defer backing.free(again);
                    try std.testing.expectEqualSlices(u8, input, again);
                }
            };
            try std.testing.checkAllAllocationFailures(a, Check.run, .{bytes});
        }
    };
    return Suite;
}

comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
