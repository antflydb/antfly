// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Trusted local restart configuration. Only the authenticated installer writes
//! this file, before publishing backing. It restores obligations, never fresh
//! admission authority. Root/receipt binding is checked again by native open.
const std = @import("std");
const abi = @import("kernel_owner_abi").completion_pool;
const settings_mod = @import("table_storage.zig");
const catalog = @import("completion_catalog_digest.zig");
const paths = @import("fs_paths.zig");
const Allocator = std.mem.Allocator;
pub const filename = "completion-installation.capsule";
pub const receipt_filename = "completion-installation.guard";
pub const max_encoded_bytes = 1024 * 1024;
const workspace_bytes = 4 * max_encoded_bytes;
const header_len = 48;
const magic = "AFCCAPS1";

pub const Value = struct {
    version: u32 = 1,
    binding: abi.InstallBinding,
    table_name: []const u8,
    shard_id: u64,
    root_generation: u64,
    /// Authenticated first-open range hint for offline restoration. The
    /// durable range remains authoritative once the owner has been created.
    initial_range: ?@import("../storage/byte_range.zig").ByteRange = null,
    settings: settings_mod.Settings,
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,
    canonical_root_digest: [32]u8,
    root_identity_digest: [32]u8,
    receipt_digest: [32]u8,
};
pub const Owned = struct {
    alloc: Allocator,
    workspace: []u8,
    value: Value,
    pub fn deinit(self: *Owned) void {
        self.alloc.free(self.workspace);
        self.* = undefined;
    }
};
fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn validate(alloc: Allocator, value: Value) !void {
    const id = value.binding.identity;
    if (value.version != 1 or id.version != abi.pool_abi_version or id.protocol != 1 or id.profile != 1 or
        id.capacity == 0 or id.capacity > 4 or id.group_id == 0 or id.node_id == 0 or id.generation == 0 or
        value.binding.table_id == 0 or value.binding.range_id == 0 or value.shard_id == 0 or
        value.table_name.len == 0 or value.table_name.len > 1024 or
        std.mem.indexOfScalar(u8, value.table_name, 0) != null or
        value.schema_json.len > 128 * 1024 or value.read_schema_json.len > 128 * 1024 or value.indexes_json.len > 128 * 1024 or
        value.schema_json.len + value.read_schema_json.len + value.indexes_json.len > 128 * 1024 or
        std.mem.allEqual(u8, &id.incarnation, 0) or std.mem.allEqual(u8, &value.canonical_root_digest, 0) or
        std.mem.allEqual(u8, &value.root_identity_digest, 0) or std.mem.allEqual(u8, &value.receipt_digest, 0))
        return error.CompletionProfileChanged;
    const policy = value.settings.transaction_recovery orelse return error.CompletionProfileChanged;
    try policy.validate();
    if (!policy.requiresDurableCompletion()) return error.CompletionProfileChanged;
    // Policy digest uses the same fixed integer encoding as metadata activation.
    const actual_policy = @import("../metadata/completion_activation.zig").policyDigest(policy);
    if (!std.mem.eql(u8, &actual_policy, &id.policy_digest)) return error.CompletionProfileChanged;
    const actual_catalog = try catalog.digest(alloc, value.schema_json, value.read_schema_json, value.indexes_json);
    if (!std.mem.eql(u8, &actual_catalog, &value.binding.schema_catalog_digest)) return error.CompletionProfileChanged;
    if (value.initial_range) |range| {
        if (range.start.len > 1024 * 1024 or range.end.len > 1024 * 1024 or
            (range.end.len != 0 and range.start.len != 0 and std.mem.order(u8, range.start, range.end) != .lt))
            return error.CompletionProfileChanged;
    }
}
pub fn encode(alloc: Allocator, value: Value) ![]u8 {
    try validate(alloc, value);
    const json = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(json);
    if (json.len > max_encoded_bytes - header_len) return error.CompletionProfileChanged;
    const result = try alloc.alloc(u8, header_len + json.len);
    @memcpy(result[0..8], magic);
    std.mem.writeInt(u64, result[8..16], @intCast(json.len), .little);
    @memcpy(result[header_len..], json);
    var checksum = std.crypto.hash.sha2.Sha256.init(.{});
    checksum.update(result[0..16]);
    checksum.update(json);
    @memcpy(result[16..48], &checksum.finalResult());
    return result;
}
pub fn decode(alloc: Allocator, bytes: []const u8) !Owned {
    // Reject size, version framing and corruption before any allocation.
    if (bytes.len < header_len or bytes.len > max_encoded_bytes or !std.mem.eql(u8, bytes[0..8], magic) or
        std.mem.readInt(u64, bytes[8..16], .little) != bytes.len - header_len) return error.CompletionProfileChanged;
    var checksum = std.crypto.hash.sha2.Sha256.init(.{});
    checksum.update(bytes[0..16]);
    checksum.update(bytes[header_len..]);
    if (!std.mem.eql(u8, &checksum.finalResult(), bytes[16..48])) return error.CompletionProfileChanged;
    const workspace = try alloc.alloc(u8, workspace_bytes);
    errdefer alloc.free(workspace);
    var fixed = std.heap.FixedBufferAllocator.init(workspace);
    const value = try std.json.parseFromSliceLeaky(Value, fixed.allocator(), bytes[header_len..], .{ .allocate = .alloc_always });
    try validate(fixed.allocator(), value);
    return .{ .alloc = alloc, .workspace = workspace, .value = value };
}
fn fileDigest(alloc: Allocator, io: std.Io, root: []const u8, name: []const u8) ![32]u8 {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, name });
    defer alloc.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(4096));
    defer alloc.free(bytes);
    if (bytes.len == 0) return error.CompletionProfileChanged;
    return digest(bytes);
}
fn pathDigest(alloc: Allocator, io: std.Io, root: []const u8) ![32]u8 {
    const canonical = if (std.fs.path.isAbsolute(root))
        try std.Io.Dir.realPathFileAbsoluteAlloc(io, root, alloc)
    else
        try std.Io.Dir.cwd().realPathFileAlloc(io, root, alloc);
    defer alloc.free(canonical);
    return digest(canonical);
}
pub fn bindRoot(alloc: Allocator, io: std.Io, root: []const u8, value: *Value) !void {
    value.canonical_root_digest = try pathDigest(alloc, io, root);
    value.root_identity_digest = try fileDigest(alloc, io, root, "root_identity.checkpoint");
    value.receipt_digest = try fileDigest(alloc, io, root, receipt_filename);
}
pub fn validateRoot(alloc: Allocator, io: std.Io, root: []const u8, value: Value) !void {
    var actual = value;
    try bindRoot(alloc, io, root, &actual);
    if (!std.mem.eql(u8, &actual.canonical_root_digest, &value.canonical_root_digest) or
        !std.mem.eql(u8, &actual.root_identity_digest, &value.root_identity_digest) or
        !std.mem.eql(u8, &actual.receipt_digest, &value.receipt_digest)) return error.CompletionProfileChanged;
}
pub fn load(alloc: Allocator, io: std.Io, root: []const u8) !?Owned {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, filename });
    defer alloc.free(path);
    const bytes = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_encoded_bytes)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(bytes);
    var owned = try decode(alloc, bytes);
    errdefer owned.deinit();
    try validateRoot(alloc, io, root, owned.value);
    return owned;
}
/// Caller holds the installation registry barrier until this succeeds. Failure
/// (including uncertain directory sync) must leave the group fenced.
pub fn publish(alloc: Allocator, io: std.Io, root: []const u8, value: Value) !void {
    return publishObserved(alloc, io, root, value, null);
}

const PublishStage = enum { file_synced, renamed };
fn publishObserved(alloc: Allocator, io: std.Io, root: []const u8, value: Value, observer: ?*const fn (PublishStage) anyerror!void) !void {
    try validateRoot(alloc, io, root, value);
    const wire = try encode(alloc, value);
    defer alloc.free(wire);
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, filename });
    defer alloc.free(path);
    if (try load(alloc, io, root)) |existing_value| {
        var existing = existing_value;
        defer existing.deinit();
        const prior = try encode(alloc, existing.value);
        defer alloc.free(prior);
        if (!std.mem.eql(u8, prior, wire)) return error.CompletionProfileChanged;
        // Retry an uncertain prior rename/directory sync before exposing backing.
        try paths.syncFileAndParentPortable(io, path);
        return;
    }
    var nonce: u128 = undefined;
    try io.randomSecure(std.mem.asBytes(&nonce));
    const temporary = try std.fmt.allocPrint(alloc, "{s}.tmp-{x}", .{ path, nonce });
    defer alloc.free(temporary);
    defer std.Io.Dir.cwd().deleteFile(io, temporary) catch {};
    {
        var file = try paths.createFilePortable(io, temporary, .{ .exclusive = true });
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(wire);
        try writer.end();
        try file.sync(io);
    }
    if (observer) |observe| try observe(.file_synced);
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temporary, std.Io.Dir.cwd(), path, io);
    if (observer) |observe| try observe(.renamed);
    try paths.syncDirPortable(io, root);
}

fn fixture(alloc: Allocator) !Value {
    const settings: settings_mod.Settings = .{ .transaction_recovery = .{
        .protocol_version = 1,
        .max_count = 4,
        .max_bytes = 65536,
        .max_transaction_bytes = 16384,
        .completion_protocol_version = 1,
        .profile_version = 1,
    } };
    return .{
        .binding = .{
            .identity = .{ .group_id = 2, .node_id = 7, .capacity = 4, .generation = 1, .incarnation = @splat(15), .policy_digest = @import("../metadata/completion_activation.zig").policyDigest(settings.transaction_recovery.?) },
            .table_id = 1,
            .range_id = 3,
            .schema_catalog_digest = try catalog.digest(alloc, "", "", "{}"),
        },
        .table_name = "docs",
        .shard_id = 2,
        .root_generation = 0,
        .settings = settings,
        .schema_json = "",
        .read_schema_json = "",
        .indexes_json = "{}",
        .canonical_root_digest = @splat(1),
        .root_identity_digest = @splat(2),
        .receipt_digest = @splat(3),
    };
}

test "workload admission completion capsule validates framing before allocation and preserves configuration" {
    const alloc = std.testing.allocator;
    var value = try fixture(alloc);
    value.initial_range = .{ .start = "a", .end = "m" };
    const wire = try encode(alloc, value);
    defer alloc.free(wire);
    var decoded = try decode(alloc, wire);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(value, decoded.value);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, decode(failing.allocator(), wire));
    wire[wire.len - 1] ^= 1;
    try std.testing.expectError(error.CompletionProfileChanged, decode(failing.allocator(), wire));
    try std.testing.expectError(error.CompletionProfileChanged, decode(failing.allocator(), wire[0..47]));
    var unsupported = value;
    unsupported.version = 2;
    try std.testing.expectError(error.CompletionProfileChanged, encode(alloc, unsupported));
    unsupported = value;
    unsupported.binding.identity.policy_digest[0] ^= 1;
    try std.testing.expectError(error.CompletionProfileChanged, encode(alloc, unsupported));
    unsupported = value;
    unsupported.initial_range = .{ .start = "z", .end = "a" };
    try std.testing.expectError(error.CompletionProfileChanged, encode(alloc, unsupported));
}

fn writeFixtureFile(io: std.Io, root: []const u8, name: []const u8, bytes: []const u8) !void {
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ root, name });
    defer std.testing.allocator.free(path);
    var file = try paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);
    var buffer: [256]u8 = undefined;
    var writer = file.writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.end();
    try file.sync(io);
}

test "workload admission completion capsule publication rejects changed roots and preserves immutable installation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var thread = std.Io.Threaded.init(alloc, .{});
    defer thread.deinit();
    const io = thread.io();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    try std.testing.expect((try load(alloc, io, root)) == null);
    try writeFixtureFile(io, root, "root_identity.checkpoint", "root-A");
    try writeFixtureFile(io, root, receipt_filename, "native-receipt-A");
    var value = try fixture(alloc);
    try bindRoot(alloc, io, root, &value);
    try publish(alloc, io, root, value);
    try publish(alloc, io, root, value);
    var reopened = (try load(alloc, io, root)).?;
    defer reopened.deinit();
    try std.testing.expectEqualDeep(value, reopened.value);
    var changed = value;
    changed.binding.identity.generation += 1;
    try std.testing.expectError(error.CompletionProfileChanged, publish(alloc, io, root, changed));
    changed = value;
    changed.canonical_root_digest[0] ^= 1;
    try std.testing.expectError(error.CompletionProfileChanged, validateRoot(alloc, io, root, changed));
    try writeFixtureFile(io, root, "root_identity.checkpoint", "root-B");
    try std.testing.expectError(error.CompletionProfileChanged, load(alloc, io, root));
    try writeFixtureFile(io, root, "root_identity.checkpoint", "root-A");
    try writeFixtureFile(io, root, receipt_filename, "native-receipt-B");
    try std.testing.expectError(error.CompletionProfileChanged, load(alloc, io, root));
}

test "workload admission completion capsule publication failures retain a retryable exact identity" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var thread = std.Io.Threaded.init(alloc, .{});
    defer thread.deinit();
    const io = thread.io();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    try writeFixtureFile(io, root, "root_identity.checkpoint", "root-A");
    try writeFixtureFile(io, root, receipt_filename, "native-receipt-A");
    var value = try fixture(alloc);
    try bindRoot(alloc, io, root, &value);
    const Crash = struct {
        fn beforeRename(stage: PublishStage) !void {
            if (stage == .file_synced) return error.InjectedCapsulePublicationFailure;
        }
        fn afterRename(stage: PublishStage) !void {
            if (stage == .renamed) return error.InjectedCapsulePublicationFailure;
        }
    };
    try std.testing.expectError(error.InjectedCapsulePublicationFailure, publishObserved(alloc, io, root, value, Crash.beforeRename));
    try std.testing.expect((try load(alloc, io, root)) == null);
    try std.testing.expectError(error.InjectedCapsulePublicationFailure, publishObserved(alloc, io, root, value, Crash.afterRename));
    var uncertain = (try load(alloc, io, root)).?;
    defer uncertain.deinit();
    try std.testing.expectEqualDeep(value, uncertain.value);
    // A visible result after uncertain directory sync is retried exactly and
    // synced again. Neither failure can publish a different generation.
    try publish(alloc, io, root, value);
    value.binding.identity.generation += 1;
    try std.testing.expectError(error.CompletionProfileChanged, publish(alloc, io, root, value));
}
