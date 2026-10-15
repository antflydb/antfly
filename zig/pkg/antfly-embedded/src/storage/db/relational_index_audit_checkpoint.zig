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

//! Physical-root-local audit checkpoints, never canonical native job state.
//! The caller retains the physical root lifecycle lease. A namespace file lock
//! serializes local scanners without holding the primary apply lock. Signed
//! progress can resume only under this root's key and exact job/target binding;
//! it is not a portable publication proof or permission to adopt a generation.
const std = @import("std");
const claims = @import("relational_index_private_claims.zig");
const generation = @import("relational_index_validation.zig");
const jobs = @import("relational_index_owner_job.zig");
const signing = @import("root_signing_identity.zig");
const root_identity = @import("root_identity.zig");
const Context = @import("../../api/operation.zig").RequestContext;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const Allocator = std.mem.Allocator;
const Digest = [32]u8;
const magic = "AFRIA\x02\x00\x00";
const max_header = 8192;
const max_checkpoint = 16 + max_header + generation.max_wire_bytes + 64;

pub const State = struct {
    inventory: claims.InventoryValidation.Audit,
    generation: ?generation.Audit = null,
    pub fn deinit(self: *@This()) void {
        if (self.generation) |*value| value.deinit();
        self.* = undefined;
    }
    fn validate(self: State, root: signing.State, physical_incarnation: u128, binding: jobs.Binding, expected: claims.TargetExpectation) !void {
        try expected.requireBinding(binding);
        if (root.root_incarnation == 0 or physical_incarnation == 0 or self.inventory.root_incarnation != physical_incarnation or
            !equal(self.inventory.expectation_digest, try expected.digest()) or !nonzero(self.inventory.canonical_digest) or !nonzero(self.inventory.page_digest)) return error.PrivateIndexAuditScopeChanged;
        try self.inventory.fields.validate(expected);
        if (self.generation) |value| {
            if (self.inventory.fields.phase != .ready or value.root_incarnation != physical_incarnation or
                !nonzero(value.canonical_digest) or !equal(value.inventory_audit_digest, self.inventory.page_digest) or
                !equal(try value.progress.summary.expected.digest(), try expected.digest()) or
                !equal(value.progress.summary.inventory_digest, self.inventory.canonical_digest)) return error.PrivateIndexAuditScopeChanged;
            try value.progress.summary.validate();
        }
    }
};
pub const Summary = struct {
    root_incarnation: u128,
    physical_root_incarnation: u128,
    expectation_digest: Digest,
    inventory: claims.InventoryValidation.Fields,
    generation: ?generation.Fields,
    generation_canonical_digest: ?Digest,
    source_members: ?u64,
    canonical_position: ?@import("receipt_position.zig").Native,
    checkpoint_digest: Digest,
    pub fn validateFor(self: @This(), expected: claims.TargetExpectation, root_incarnation: u128) !void {
        try expected.validate();
        try self.inventory.validate(expected);
        if (root_incarnation == 0 or self.root_incarnation != root_incarnation or self.physical_root_incarnation == 0 or !equal(self.expectation_digest, try expected.digest()) or !nonzero(self.checkpoint_digest)) return error.InvalidRelationalIndexValidation;
        if (self.generation) |fields| {
            if (self.inventory.phase != .ready or !nonzero(self.generation_canonical_digest orelse return error.InvalidRelationalIndexValidation)) return error.InvalidRelationalIndexValidation;
            try fields.validate(self.source_members orelse return error.InvalidRelationalIndexValidation);
            try (self.canonical_position orelse return error.InvalidRelationalIndexValidation).validate();
        } else if (self.generation_canonical_digest != null or self.source_members != null or self.canonical_position != null) return error.InvalidRelationalIndexValidation;
    }
};
const Scope = struct { canonical_digest: Digest, inventory_audit_digest: Digest };
const Header = struct { signing_root_incarnation: u128, binding_digest: Digest, inventory: claims.InventoryValidation.Audit, generation_scope: ?Scope };
fn equal(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
fn nonzero(value: Digest) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn hash(bytes: []const u8) Digest {
    var digest: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    return digest;
}
fn message(bytes: []const u8) Digest {
    var digest = std.crypto.hash.Blake3.init(.{});
    digest.update("antfly/private-index-root-local-checkpoint/v2");
    digest.update(bytes);
    var result: Digest = undefined;
    digest.final(&result);
    return result;
}
fn permissions() std.Io.File.Permissions {
    if (comptime @import("builtin").os.tag == .windows) return .default_file;
    return @fromBackingInt(0o600);
}
fn encode(alloc: Allocator, root: signing.State, physical_incarnation: u128, binding: jobs.Binding, expected: claims.TargetExpectation, state: State) ![]u8 {
    try state.validate(root, physical_incarnation, binding, expected);
    const header: Header = .{ .signing_root_incarnation = root.root_incarnation, .binding_digest = hash(&try binding.encode()), .inventory = state.inventory, .generation_scope = if (state.generation) |value| .{ .canonical_digest = value.canonical_digest, .inventory_audit_digest = value.inventory_audit_digest } else null };
    const json = try std.json.Stringify.valueAlloc(alloc, header, .{});
    defer alloc.free(json);
    if (json.len > max_header) return error.InvalidRelationalIndexValidation;
    const progress = if (state.generation) |value| try value.progress.encodeAlloc(alloc) else null;
    defer if (progress) |bytes| alloc.free(bytes);
    const binary: []const u8 = progress orelse "";
    if (binary.len > generation.max_wire_bytes) return error.InvalidRelationalIndexValidation;
    const bytes = try alloc.alloc(u8, 16 + json.len + binary.len + 64);
    errdefer alloc.free(bytes);
    @memcpy(bytes[0..8], magic);
    std.mem.writeInt(u32, bytes[8..12], @intCast(json.len), .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(binary.len), .little);
    @memcpy(bytes[16..][0..json.len], json);
    @memcpy(bytes[16 + json.len ..][0..binary.len], binary);
    @memcpy(bytes[bytes.len - 64 ..], &try root.sign(&message(bytes[0 .. bytes.len - 64])));
    return bytes;
}
fn decode(alloc: Allocator, root: signing.State, physical_incarnation: u128, binding: jobs.Binding, expected: claims.TargetExpectation, bytes: []const u8) !State {
    if (bytes.len < 80 or bytes.len > max_checkpoint or !std.mem.eql(u8, bytes[0..8], magic)) return error.InvalidRelationalIndexValidation;
    const json_len = std.mem.readInt(u32, bytes[8..12], .little);
    const binary_len = std.mem.readInt(u32, bytes[12..16], .little);
    if (json_len > max_header or binary_len > generation.max_wire_bytes or @as(u64, json_len) + binary_len + 80 != bytes.len) return error.InvalidRelationalIndexValidation;
    // Authenticate before parsing or allocating a potentially large cursor.
    signing.verify(root.public_key, &message(bytes[0 .. bytes.len - 64]), bytes[bytes.len - 64 ..][0..64].*) catch return error.InvalidRelationalIndexValidation;
    var parsed = try std.json.parseFromSlice(Header, alloc, bytes[16..][0..json_len], .{ .ignore_unknown_fields = false, .max_value_len = 128 });
    defer parsed.deinit();
    const canonical = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(canonical);
    if (!std.mem.eql(u8, canonical, bytes[16..][0..json_len]) or parsed.value.signing_root_incarnation != root.root_incarnation or
        (parsed.value.generation_scope != null) != (binary_len != 0)) return error.PrivateIndexAuditScopeChanged;
    if (!equal(parsed.value.binding_digest, hash(&try binding.encode()))) return error.PrivateIndexAuditOwnerChanged;
    if (parsed.value.inventory.root_incarnation != physical_incarnation) return error.PrivateIndexAuditPhysicalRootChanged;
    var result: State = .{ .inventory = parsed.value.inventory };
    errdefer result.deinit();
    if (parsed.value.generation_scope) |scope| {
        result.generation = .{ .root_incarnation = physical_incarnation, .canonical_digest = scope.canonical_digest, .inventory_audit_digest = scope.inventory_audit_digest, .progress = try generation.Loaded.decode(alloc, bytes[16 + json_len ..][0..binary_len]) };
    }
    try result.validate(root, physical_incarnation, binding, expected);
    return result;
}

pub const Guard = struct {
    alloc: Allocator,
    io: std.Io,
    root_dir: []const u8,
    root: signing.State,
    physical_incarnation: u128,
    binding: jobs.Binding,
    expected: claims.TargetExpectation,
    path: []u8,
    directory: std.Io.Dir,
    lock: std.Io.File,
    context: Context,
    checkpoint_digest: ?Digest = null,
    pub fn deinit(self: *@This()) void {
        self.lock.unlock(self.io);
        self.lock.close(self.io);
        self.directory.close(self.io);
        self.alloc.free(self.path);
        self.* = undefined;
    }
    fn requireRoot(self: *const @This()) !void {
        try self.context.ensureActive();
        const current = try root_identity.load(self.alloc, self.io, self.root_dir);
        if (current.incarnation != self.root.root_incarnation) return error.PrivateIndexAuditScopeChanged;
        const key = try signing.load(self.alloc, self.io, self.root_dir);
        if (!equal(key.public_key, self.root.public_key)) return error.PrivateIndexAuditScopeChanged;
    }
    pub fn load(self: *@This()) !?State {
        self.checkpoint_digest = null;
        try self.requireRoot();
        const file = self.directory.openFile(self.io, std.fs.path.basename(self.path), .{ .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer file.close(self.io);
        const stat = try file.stat(self.io);
        if (stat.kind != .file or stat.size > max_checkpoint) return error.InvalidRelationalIndexValidation;
        if (comptime @import("builtin").os.tag != .windows) if ((stat.permissions.toMode() & 0o077) != 0) return error.InvalidRelationalIndexValidation;
        var buffer: [4096]u8 = undefined;
        var reader = file.reader(self.io, &buffer);
        const bytes = try reader.interface.allocRemaining(self.alloc, .limited(max_checkpoint + 1));
        defer self.alloc.free(bytes);
        const result = decode(self.alloc, self.root, self.physical_incarnation, self.binding, self.expected, bytes) catch |err| switch (err) {
            // The canonical native job has one slot per namespace. A newly
            // authorized owner starts from scratch, never from the prior job's
            // authenticated prefix. Its first save atomically replaces this
            // same slot, bounding checkpoint files after repeated job churn.
            error.PrivateIndexAuditOwnerChanged, error.PrivateIndexAuditPhysicalRootChanged => return null,
            else => return err,
        };
        self.checkpoint_digest = hash(bytes);
        return result;
    }
    pub fn save(self: *@This(), state: State) !void {
        try self.requireRoot();
        const bytes = try encode(self.alloc, self.root, self.physical_incarnation, self.binding, self.expected, state);
        defer self.alloc.free(bytes);
        const digest = hash(bytes);
        if (self.checkpoint_digest) |previous| if (equal(previous, digest)) return;
        const temporary = try std.fmt.allocPrint(self.alloc, "{s}.tmp", .{std.fs.path.basename(self.path)});
        defer self.alloc.free(temporary);
        // Only this exact job's private temporary file, under its held lock.
        // A crash before rename leaves the old checkpoint authoritative.
        self.directory.deleteFile(self.io, temporary) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        errdefer self.directory.deleteFile(self.io, temporary) catch {};
        {
            var file = try self.directory.createFile(self.io, temporary, .{ .exclusive = true, .permissions = permissions() });
            defer file.close(self.io);
            var buffer: [4096]u8 = undefined;
            var writer = file.writer(self.io, &buffer);
            try writer.interface.writeAll(bytes);
            try writer.end();
            try file.sync(self.io);
        }
        try self.requireRoot();
        try std.Io.Dir.rename(self.directory, temporary, self.directory, std.fs.path.basename(self.path), self.io);
        // Sync the exact opened directory, not a newly resolved pathname.
        const directory_file: std.Io.File = .{ .handle = self.directory.handle, .flags = .{ .nonblocking = false } };
        try directory_file.sync(self.io);
        // Also close the mkdir race: a peer may have observed the new audit
        // directory before its creator synced the containing root. Once this
        // job has a durable checkpoint, later turns need only sync its parent.
        if (self.checkpoint_digest == null) try fs_paths.syncDirPortable(self.io, self.root_dir);
        self.checkpoint_digest = digest;
    }
    pub fn summary(self: *const @This(), state: State) !Summary {
        try state.validate(self.root, self.physical_incarnation, self.binding, self.expected);
        return .{ .root_incarnation = self.root.root_incarnation, .physical_root_incarnation = self.physical_incarnation, .expectation_digest = state.inventory.expectation_digest, .inventory = state.inventory.fields, .generation = if (state.generation) |value| value.progress.summary.fields else null, .generation_canonical_digest = if (state.generation) |value| value.canonical_digest else null, .source_members = if (state.generation) |value| value.progress.summary.source_members else null, .canonical_position = if (state.generation) |value| value.progress.summary.position else null, .checkpoint_digest = self.checkpoint_digest orelse return error.PrivateIndexTargetValidationRequired };
    }
};

/// root_dir is the provisioned signing root, not a snapshot/staging input.
/// physical_incarnation comes from the pinned live DB, never a remote request.
/// Both root lifecycle leases outlive this guard and every scan it protects.
pub fn open(alloc: Allocator, io: std.Io, root_dir: []const u8, root_incarnation: u128, physical_incarnation: u128, binding: jobs.Binding, expected: claims.TargetExpectation, context: Context) !Guard {
    try context.ensureActive();
    try expected.requireBinding(binding);
    const identity = try root_identity.load(alloc, io, root_dir);
    if (root_incarnation == 0 or physical_incarnation == 0 or identity.incarnation != root_incarnation) return error.PrivateIndexAuditScopeChanged;
    // Provisioning owns key creation. An audit must not mint a new identity
    // or wait on an unrelated root-key creation lock.
    const root = try signing.load(alloc, io, root_dir);
    if (root.root_incarnation != root_incarnation) return error.PrivateIndexAuditScopeChanged;
    const directory = try std.fmt.allocPrint(alloc, "{s}/.relational-index-audits", .{root_dir});
    defer alloc.free(directory);
    if (std.Io.Dir.cwd().createDir(io, directory, .default_dir)) |_| {
        try fs_paths.syncDirPortable(io, root_dir);
    } else |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    }
    // Existing directories need no parent sync on every bounded audit turn.
    // Linux otherwise opens an O_PATH descriptor, which cannot be fsynced.
    const opened_directory = try std.Io.Dir.cwd().openDir(io, directory, .{ .follow_symlinks = false, .iterate = true });
    errdefer opened_directory.close(io);
    // Match the single canonical native-job slot per namespace, rather than
    // accumulating files per attempt. The signed header still binds the full
    // owner identity. Different namespaces may share the logical job ID.
    const id = std.fmt.bytesToHex(hash(&@import("online_source_contract.zig").namespaceBytes(binding.fence.namespace)), .lower);
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}.checkpoint", .{ directory, id });
    errdefer alloc.free(path);
    const lock_path = try std.fmt.allocPrint(alloc, "{s}.lock", .{std.fs.path.basename(path)});
    defer alloc.free(lock_path);
    if (opened_directory.createFile(io, lock_path, .{ .exclusive = true, .permissions = permissions() })) |seed| {
        seed.close(io);
    } else |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    }
    const file = try opened_directory.openFile(io, lock_path, .{ .mode = .read_write, .follow_symlinks = false });
    errdefer file.close(io);
    if ((try file.stat(io)).kind != .file) return error.InvalidRelationalIndexValidation;
    while (!try file.tryLock(io, .exclusive)) {
        try context.ensureActive();
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    errdefer file.unlock(io);
    const result: Guard = .{ .alloc = alloc, .io = io, .root_dir = root_dir, .root = root, .physical_incarnation = physical_incarnation, .binding = binding, .expected = expected, .path = path, .directory = opened_directory, .lock = file, .context = context };
    try result.requireRoot();
    const temporary = try std.fmt.allocPrint(alloc, "{s}.tmp", .{std.fs.path.basename(path)});
    defer alloc.free(temporary);
    opened_directory.deleteFile(io, temporary) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    return result;
}
