// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! A local receipt binds authenticated metadata installation to one physical
//! root and its reconciled catalog. This is not itself remote authentication.
const std = @import("std");
const abi = @import("kernel_owner_abi").completion_pool;
const catalog = @import("../common/completion_catalog_digest.zig");
const root_identity = @import("db/root_identity.zig");
const pool = @import("lsm_backend/completion_pool.zig");
const Allocator = std.mem.Allocator;
pub const filename = "completion-installation.guard";
pub const encoded_len = 120;

pub const Receipt = struct {
    incarnation: u128,
    binding_digest: [32]u8,
    physical_digest: [32]u8,
};

fn hashNumber(hash: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}
pub fn bindingDigest(binding: abi.InstallBinding) ![32]u8 {
    const id = binding.identity;
    if (id.version != abi.pool_abi_version or id.protocol != 1 or id.profile != 1 or id.capacity == 0 or id.capacity > 4 or
        id.group_id == 0 or id.node_id == 0 or id.generation == 0 or binding.table_id == 0 or binding.range_id == 0 or
        std.mem.allEqual(u8, &id.incarnation, 0) or std.mem.allEqual(u8, &id.policy_digest, 0)) return error.CompletionProfileChanged;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-local-completion-installation-v1");
    for ([_]u64{ id.version, id.protocol, id.profile, id.capacity, id.group_id, id.node_id, id.generation, binding.table_id, binding.range_id, binding.split_attempt_epoch }) |value| hashNumber(&hash, value);
    hash.update(&id.incarnation);
    hash.update(&id.policy_digest);
    hash.update(&binding.schema_catalog_digest);
    hash.update(&binding.expected_definition);
    return hash.finalResult();
}
pub fn validateCatalog(alloc: Allocator, binding: abi.InstallBinding, schema: []const u8, read_schema: []const u8, indexes: []const u8) !void {
    _ = try bindingDigest(binding);
    const actual = try catalog.digest(alloc, schema, read_schema, indexes);
    if (!std.mem.eql(u8, &actual, &binding.schema_catalog_digest)) return error.CompletionProfileChanged;
}
pub fn encode(receipt: Receipt) [encoded_len]u8 {
    var out: [encoded_len]u8 = undefined;
    @memcpy(out[0..8], "AFCINST1");
    std.mem.writeInt(u128, out[8..24], receipt.incarnation, .little);
    @memcpy(out[24..56], &receipt.binding_digest);
    @memcpy(out[56..88], &receipt.physical_digest);
    std.crypto.hash.sha2.Sha256.hash(out[0..88], out[88..120], .{});
    return out;
}
pub fn decode(bytes: []const u8) !Receipt {
    if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AFCINST1")) return error.CompletionProfileChanged;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[0..88], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, bytes[88..120])) return error.CompletionProfileChanged;
    return .{ .incarnation = std.mem.readInt(u128, bytes[8..24], .little), .binding_digest = bytes[24..56].*, .physical_digest = bytes[56..88].* };
}
pub fn load(alloc: Allocator, io: std.Io, path: []const u8) !?Receipt {
    const receipt_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ path, filename });
    defer alloc.free(receipt_path);
    const bytes = std.Io.Dir.cwd().readFileAlloc(io, receipt_path, alloc, .limited(encoded_len + 1)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(bytes);
    return try decode(bytes);
}

/// Must run before native replay. Missing receipt means a fresh installation,
/// not permission to reopen accepted guards without authority. The native open
/// independently refuses accepted debt when this returns null.
pub fn preflight(alloc: Allocator, io: std.Io, path: []const u8, binding: abi.InstallBinding, schema: []const u8, read_schema: []const u8, indexes: []const u8) !?pool.Config {
    try validateCatalog(alloc, binding, schema, read_schema, indexes);
    const receipt = (try load(alloc, io, path)) orelse return null;
    const root = try root_identity.load(alloc, io, path);
    const expected = try bindingDigest(binding);
    if (root.incarnation != receipt.incarnation or !std.mem.eql(u8, &expected, &receipt.binding_digest)) return error.CompletionProfileChanged;
    return .{ .identity = binding.identity, .schema_catalog_digest = binding.schema_catalog_digest };
}

test "workload admission physical completion installation receipt binds root generation and canonical catalog" {
    const alloc = std.testing.allocator;
    var binding: abi.InstallBinding = .{ .identity = .{ .capacity = 4, .group_id = 5, .node_id = 6, .generation = 7, .incarnation = @splat(8), .policy_digest = @splat(9) }, .table_id = 10, .range_id = 11 };
    binding.schema_catalog_digest = try catalog.digest(alloc, "{\"b\":2,\"a\":1}", "", "{}");
    try validateCatalog(alloc, binding, "{\"a\":1,\"b\":2}", "", "{}");
    try std.testing.expectError(error.CompletionProfileChanged, validateCatalog(alloc, binding, "{\"a\":2}", "", "{}"));
    const expected = try bindingDigest(binding);
    var wire = encode(.{ .incarnation = 13, .binding_digest = expected, .physical_digest = @splat(14) });
    try std.testing.expectEqual(@as(u128, 13), (try decode(&wire)).incarnation);
    binding.identity.generation += 1;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try bindingDigest(binding)));
    wire[56] ^= 1;
    try std.testing.expectError(error.CompletionProfileChanged, decode(&wire));
}
