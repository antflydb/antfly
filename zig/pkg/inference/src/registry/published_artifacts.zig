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

//! Filesystem authority for managed model construction and readers. A receipt
//! authenticates its listed files, never arbitrary siblings in the directory.
const std = @import("std");
const managed = @import("managed_receipt.zig");
pub const Expected = struct { publication: ?[32]u8 };
/// Process-owned successful checksum proofs. Keys own no pathname memory;
/// publication, canonical member, declared digest and the complete file stamp
/// must all match. No on-disk memo can authenticate a receipt member.
pub const VerificationCache = struct {
    pub const capacity = 4096;
    const Entry = struct { stamp: managed.ArtifactStamp, used: u64 };
    /// Manager-owned caches outlive the allocators of borrowed acquisitions.
    owner_allocator: ?std.mem.Allocator = null,
    mutex: std.atomic.Mutex = .unlocked,
    builders: [16]std.Io.Mutex = @splat(.init),
    entries: std.AutoHashMapUnmanaged([32]u8, Entry) = .empty,
    clock: u64 = 0,

    fn lock(self: *VerificationCache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn keyFor(publication: [32]u8, artifact: *const managed.ValidatedArtifact) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&publication);
        hash.update(artifact.canonical_path);
        hash.update(&.{0});
        hash.update(std.mem.asBytes(&artifact.size));
        for (artifact.sha256.?) |byte| hash.update(&.{std.ascii.toLower(byte)});
        return hash.finalResult();
    }
    fn hit(self: *VerificationCache, key: [32]u8, stamp: managed.ArtifactStamp) bool {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.entries.getPtr(key) orelse return false;
        if (!std.meta.eql(entry.stamp, stamp)) return false;
        self.clock +|= 1;
        entry.used = self.clock;
        return true;
    }
    fn remember(self: *VerificationCache, a: std.mem.Allocator, key: [32]u8, stamp: managed.ArtifactStamp) !void {
        self.lock();
        defer self.mutex.unlock();
        self.clock +|= 1;
        if (!self.entries.contains(key) and self.entries.count() == capacity) {
            var oldest: ?[32]u8 = null;
            var age: u64 = std.math.maxInt(u64);
            var it = self.entries.iterator();
            while (it.next()) |entry| if (entry.value_ptr.used <= age) {
                oldest = entry.key_ptr.*;
                age = entry.value_ptr.used;
            };
            _ = self.entries.remove(oldest.?);
        }
        try self.entries.put(self.owner_allocator orelse a, key, .{ .stamp = stamp, .used = self.clock });
    }
    fn verify(self: *VerificationCache, a: std.mem.Allocator, io: std.Io, publication: [32]u8, artifact: *const managed.ValidatedArtifact, initial: managed.ArtifactStamp) !managed.ArtifactStamp {
        const key = keyFor(publication, artifact);
        if (self.hit(key, initial)) return initial;
        const builder = &self.builders[key[0] % self.builders.len];
        try builder.lock(io);
        defer builder.unlock(io);
        // Another acquisition may have verified or replaced the file while
        // this request waited. Never reuse the pre-wait filesystem stamp.
        const stamp = managed.ArtifactStamp.fromStat(try std.Io.Dir.cwd().statFile(io, artifact.canonical_path, .{ .follow_symlinks = false }));
        if (stamp.kind != .file or stamp.size != artifact.size) return error.InvalidManagedDownload;
        if (self.hit(key, stamp)) return stamp;
        const verified = try managed.verifyValidatedArtifactSha256AndStamp(io, artifact, artifact.sha256.?);
        self.remember(a, key, verified) catch {}; // Cache pressure cannot deny a valid construction.
        return verified;
    }
    pub fn deinit(self: *VerificationCache, a: std.mem.Allocator) void {
        self.entries.deinit(self.owner_allocator orelse a);
    }
};

pub const PublishedArtifacts = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    root: []const u8,
    requested_root: []u8,
    storage_root: []u8,
    physical_root: [:0]u8,
    physical_direct: ?[]u8 = null,
    // A standalone GGUF may be constructed with a directory metadata snapshot.
    // Keep that dependency scope separate from the direct file's own binding.
    directory_scope: ?[:0]u8 = null,
    directory_scope_inode: ?@FieldType(std.Io.File.Stat, "inode") = null,
    direct_path: ?[]u8 = null,
    direct_index: ?usize = null,
    verification_cache: ?*VerificationCache = null,
    receipt: ?managed.ValidatedReceipt,
    publication: ?[32]u8,
    verified_stamps: []?managed.ArtifactStamp = &.{},
    canonical_index: std.StringHashMapUnmanaged(usize) = .empty,

    pub fn init(a: std.mem.Allocator, io: std.Io, root: []const u8, expected: ?Expected) !PublishedArtifacts {
        const direct = std.mem.endsWith(u8, root, ".gguf");
        const storage_root = try managed.publicationRootAlloc(a, io, root);
        errdefer a.free(storage_root);
        const requested_root = try absolutePath(a, io, storage_root);
        errdefer a.free(requested_root);
        const direct_path = if (direct) try absolutePath(a, io, root) else null;
        errdefer if (direct_path) |path| a.free(path);
        const physical_root = try std.Io.Dir.cwd().realPathFileAlloc(io, storage_root, a);
        errdefer a.free(physical_root);
        const physical_direct = if (direct) try managed.resolveRegularFilePath(a, io, root) else null;
        errdefer if (physical_direct) |path| a.free(path);
        const standalone = direct and std.mem.eql(u8, storage_root, root);
        var receipt = if (standalone) null else try managed.loadValidated(a, io, storage_root);
        errdefer if (receipt) |*value| value.deinit();
        const publication = if (receipt) |value| value.publication_generation else null;
        if (expected) |value| if (!std.meta.eql(value.publication, publication)) return error.ModelArtifactsChanging;
        var index = std.StringHashMapUnmanaged(usize).empty;
        errdefer index.deinit(a);
        if (receipt) |value| for (value.artifacts, 0..) |artifact, i| try index.put(a, artifact.canonical_path, i);
        const direct_index: ?usize = if (direct and receipt != null) blk: {
            const requested = try managed.resolveRequestedFilePath(a, io, root);
            defer a.free(requested);
            const canonical = try managed.resolveRegularFilePath(a, io, root);
            defer a.free(canonical);
            const selected = if (managed.pathIsWithinRoot(requested_root, requested)) requested else canonical;
            if (!managed.pathIsWithinRoot(requested_root, selected)) return error.ModelArtifactNotPublished;
            const offset = requested_root.len + @intFromBool(!std.fs.path.isSep(requested_root[requested_root.len - 1]));
            const member = receipt.?.path_index.get(selected[offset..]) orelse return error.ModelArtifactNotPublished;
            if (!std.mem.eql(u8, canonical, receipt.?.artifacts[member].canonical_path)) return error.ModelArtifactNotPublished;
            break :blk member;
        } else null;
        const verified = try a.alloc(?managed.ArtifactStamp, if (receipt) |value| value.artifacts.len else 0);
        @memset(verified, null);
        return .{ .allocator = a, .io = io, .root = root, .requested_root = requested_root, .storage_root = storage_root, .physical_root = physical_root, .physical_direct = physical_direct, .direct_path = direct_path, .direct_index = direct_index, .receipt = receipt, .publication = publication, .canonical_index = index, .verified_stamps = verified };
    }
    pub fn deinit(self: *PublishedArtifacts) void {
        self.allocator.free(self.requested_root);
        self.allocator.free(self.storage_root);
        self.allocator.free(self.physical_root);
        if (self.physical_direct) |path| self.allocator.free(path);
        if (self.directory_scope) |path| self.allocator.free(path);
        if (self.direct_path) |path| self.allocator.free(path);
        self.allocator.free(self.verified_stamps);
        self.canonical_index.deinit(self.allocator);
        if (self.receipt) |*receipt| receipt.deinit();
    }
    fn constructionRoot(self: *const PublishedArtifacts) []const u8 {
        return self.directory_scope orelse self.physical_root;
    }

    /// A supplied directory snapshot grants a standalone direct file its local
    /// sidecar scope. It cannot change receipt authority or admit a foreign file.
    pub fn bindDirectoryScope(self: *PublishedArtifacts, root: []const u8) !void {
        if (self.direct_path == null) return;
        const canonical = try std.Io.Dir.cwd().realPathFileAlloc(self.io, root, self.allocator);
        errdefer self.allocator.free(canonical);
        if (!std.mem.eql(u8, canonical, root)) return error.ModelArtifactsChanging;
        const stat = try std.Io.Dir.cwd().statFile(self.io, canonical, .{});
        if (stat.kind != .directory) return error.NotDir;
        if (!managed.pathIsWithinRoot(canonical, self.physical_direct.?)) return error.ModelArtifactOutsideRoot;
        if (self.receipt != null) {
            if (!std.mem.eql(u8, self.physical_root, canonical) and !managed.pathIsWithinRoot(self.physical_root, canonical)) return error.ModelArtifactOutsideRoot;
            self.allocator.free(canonical);
            return;
        }
        if (self.directory_scope) |scope| {
            if (!std.mem.eql(u8, scope, canonical) or self.directory_scope_inode.? != stat.inode) return error.ModelArtifactsChanging;
            self.allocator.free(canonical);
        } else {
            self.directory_scope = canonical;
            self.directory_scope_inode = stat.inode;
        }
    }

    /// Default sidecars are optional only when absent from the captured catalog.
    /// Explicit selections must use constructionPath and cannot silently vanish.
    pub fn optionalConstructionPath(self: *const PublishedArtifacts, path: []const u8) !?[]u8 {
        if (self.receipt != null) {
            _ = self.indexForPath(path) catch |err| switch (err) {
                error.InvalidManagedDownload => return null,
                else => return err,
            };
        }
        return self.constructionPath(path) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => null,
            else => return err,
        };
    }

    pub fn resolve(self: *const PublishedArtifacts, relative: []const u8) !?[]u8 {
        if (self.receipt) |*receipt| {
            const artifact = receipt.find(relative) orelse return null;
            return try self.allocator.dupe(u8, artifact.canonical_path);
        }
        return managed.resolveContainedArtifactPath(self.allocator, self.io, self.directory_scope orelse self.storage_root, relative) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => null,
            else => return err,
        };
    }
    /// Component discovery retains the selected filename and dependency base.
    /// The cold constructor will pin the leaf target separately. Resolve first
    /// to enforce receipt membership and containment before exposing this name.
    pub fn resolveModel(self: *const PublishedArtifacts, relative: []const u8) !?[]u8 {
        const canonical = (try self.resolve(relative)) orelse return null;
        defer self.allocator.free(canonical);
        return try std.fs.path.join(self.allocator, &.{ self.constructionRoot(), relative });
    }
    /// Verify bytes actually read/mapped, avoiding a hash-then-reopen race.
    pub fn verifyBytes(self: *const PublishedArtifacts, relative: []const u8, bytes: []const u8) !void {
        if (self.receipt) |*receipt| {
            try managed.verifyArtifactBytes(receipt.find(relative) orelse return error.InvalidManagedDownload, bytes);
        }
    }
    pub fn read(self: *const PublishedArtifacts, relative: []const u8, max_bytes: usize) ![]u8 {
        if (self.receipt) |*receipt| {
            const artifact = receipt.find(relative) orelse return error.FileNotFound;
            return self.readSelected(artifact.canonical_path, artifact, max_bytes);
        }
        const path = (try self.resolve(relative)) orelse return error.FileNotFound;
        defer self.allocator.free(path);
        return self.readPath(path, max_bytes);
    }
    fn absolutePath(a: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
        if (std.fs.path.isAbsolute(path)) return std.fs.path.resolve(a, &.{path});
        const cwd = try std.Io.Dir.cwd().realPathFileAlloc(io, ".", a);
        defer a.free(cwd);
        return std.fs.path.resolve(a, &.{ cwd, path });
    }
    /// Select by the name captured by this acquisition, never by a new target
    /// discovered while opening it. Both resolved and logical receipt paths
    /// retain their original checksum when a leaf/ancestor is retargeted.
    fn indexForPath(self: *const PublishedArtifacts, path: []const u8) !usize {
        const absolute = try absolutePath(self.allocator, self.io, path);
        defer self.allocator.free(absolute);
        if (self.canonical_index.get(absolute)) |index| return index;
        if (self.direct_path) |direct| if (std.mem.eql(u8, absolute, direct)) return self.direct_index orelse error.InvalidManagedDownload;
        for ([_][]const u8{ self.requested_root, self.physical_root }) |root| {
            if (managed.pathIsWithinRoot(root, absolute)) {
                const offset = root.len + @intFromBool(!std.fs.path.isSep(root[root.len - 1]));
                if (self.receipt.?.path_index.get(absolute[offset..])) |index| return index;
            }
        }
        return error.InvalidManagedDownload;
    }
    /// Read an already selected sidecar and authenticate its actual bytes
    /// against the original member's contract, even if the pathname changes.
    pub fn readPath(self: *const PublishedArtifacts, path: []const u8, max_bytes: usize) ![]u8 {
        if (self.receipt) |*receipt| {
            const artifact = &receipt.artifacts[try self.indexForPath(path)];
            return self.readSelected(path, artifact, max_bytes);
        }
        const canonical = try managed.resolveRegularFilePath(self.allocator, self.io, path);
        defer self.allocator.free(canonical);
        const root = try std.Io.Dir.cwd().realPathFileAlloc(self.io, self.storage_root, self.allocator);
        defer self.allocator.free(root);
        const standalone = self.direct_path != null and std.mem.eql(u8, self.root, self.storage_root);
        if (!managed.pathIsWithinRoot(root, canonical) and !(standalone and std.mem.eql(u8, root, canonical))) return error.ModelArtifactOutsideRoot;
        return self.readSelected(canonical, null, max_bytes);
    }
    fn readSelected(self: *const PublishedArtifacts, path: []const u8, artifact: ?*const managed.ValidatedArtifact, max_bytes: usize) ![]u8 {
        if (artifact) |member| {
            const canonical = try managed.resolveRegularFilePath(self.allocator, self.io, path);
            defer self.allocator.free(canonical);
            if (!std.mem.eql(u8, canonical, member.canonical_path)) return error.ModelArtifactsChanging;
        }
        const limit = if (artifact) |member| @min(max_bytes, std.math.cast(usize, member.size) orelse return error.FileTooLarge) else max_bytes;
        // Io.Limit rejects a stream when the limit is reached, so leave one
        // byte of headroom to observe EOF for an exactly sized artifact.
        const bytes = std.Io.Dir.cwd().readFileAlloc(self.io, path, self.allocator, .limited(limit +| 1)) catch |err| switch (err) {
            error.StreamTooLong => return if (artifact != null and artifact.?.size <= max_bytes) error.InvalidManagedDownload else error.FileTooLarge,
            else => return err,
        };
        errdefer self.allocator.free(bytes);
        if (artifact) |member| try managed.verifyArtifactBytes(member, bytes);
        return bytes;
    }
    /// Verification belongs to one reader acquisition, not path resolution.
    /// Reuse it across graph collection, backend retries and shared external
    /// tensors only while the opened file's complete identity is unchanged.
    fn verifyIndex(self: *PublishedArtifacts, index: usize) !void {
        const artifact = &self.receipt.?.artifacts[index];
        const stamp = managed.ArtifactStamp.fromStat(try std.Io.Dir.cwd().statFile(self.io, artifact.canonical_path, .{ .follow_symlinks = false }));
        if (stamp.kind != .file or stamp.size != artifact.size) return error.InvalidManagedDownload;
        if (self.verified_stamps[index]) |prior| if (std.meta.eql(prior, stamp)) return;
        self.verified_stamps[index] = if (artifact.sha256) |expected|
            if (self.verification_cache) |cache|
                try cache.verify(self.allocator, self.io, self.publication.?, artifact, stamp)
            else
                try managed.verifyValidatedArtifactSha256AndStamp(self.io, artifact, expected)
        else
            stamp;
    }
    pub fn verifyFile(self: *PublishedArtifacts, relative: []const u8) !void {
        if (self.receipt) |*receipt| {
            const index = receipt.path_index.get(relative) orelse return error.InvalidManagedDownload;
            // Every canonical alias now shares one normalized digest and proof.
            try self.verifyIndex(self.canonical_index.get(receipt.artifacts[index].canonical_path).?);
        }
    }
    /// ONNX external tensors are consumed by a component loader, not by
    /// read(). They must belong to the same receipt as the graph itself.
    pub fn verifyPath(self: *PublishedArtifacts, path: []const u8) !void {
        if (self.receipt != null) {
            const index = try self.indexForPath(path);
            const canonical = try managed.resolveRegularFilePath(self.allocator, self.io, path);
            defer self.allocator.free(canonical);
            if (!std.mem.eql(u8, canonical, self.receipt.?.artifacts[index].canonical_path)) return error.ModelArtifactsChanging;
            try self.verifyIndex(self.canonical_index.get(canonical).?);
        }
    }
    /// Authenticate the complete graph dependency set before a pathname-based
    /// importer can read it. Callers retain a shared publication lease through
    /// construction and call verify() before publishing the resulting session.
    pub fn verifyOnnx(self: *PublishedArtifacts, path: []const u8) !void {
        return self.verifyOnnxWithBase(path, null);
    }
    pub fn verifyOnnxWithBase(self: *PublishedArtifacts, path: []const u8, external_base_dir: ?[]const u8) !void {
        if (self.receipt == null) return;
        try self.verifyPath(path);
        var graph = try @import("../backends/imported_onnx_session.zig").inspectArtifactSetWithBase(self.allocator, path, external_base_dir);
        defer graph.deinit();
        for (graph.external_paths) |external| try self.verifyPath(external);
    }
    /// Rebase metadata into the captured namespace without opening optional
    /// artifacts or granting execution permission. A backend must still call
    /// constructionPath/verifyPath for each dependency it actually consumes.
    pub fn namespacePath(self: *const PublishedArtifacts, path: []const u8) ![]u8 {
        const absolute = try absolutePath(self.allocator, self.io, path);
        errdefer self.allocator.free(absolute);
        if (std.mem.eql(u8, absolute, self.requested_root)) {
            const captured = try self.allocator.dupe(u8, self.physical_root);
            self.allocator.free(absolute);
            return captured;
        }
        if (managed.pathIsWithinRoot(self.requested_root, absolute)) {
            const offset = self.requested_root.len + @intFromBool(!std.fs.path.isSep(self.requested_root[self.requested_root.len - 1]));
            const captured = try std.fs.path.join(self.allocator, &.{ self.physical_root, absolute[offset..] });
            self.allocator.free(absolute);
            return captured;
        }
        return absolute;
    }

    /// Capture the physical construction target once. Backends must never
    /// reopen a mutable caller alias after authenticating a different target.
    pub fn constructionPath(self: *const PublishedArtifacts, path: []const u8) ![]u8 {
        const absolute = try absolutePath(self.allocator, self.io, path);
        defer self.allocator.free(absolute);
        if (self.direct_path) |direct| if (std.mem.eql(u8, absolute, direct))
            return self.allocator.dupe(u8, self.physical_direct.?);
        if (std.mem.eql(u8, absolute, self.physical_root)) return self.allocator.dupe(u8, self.physical_root);
        if (std.mem.eql(u8, absolute, self.requested_root)) return self.allocator.dupe(u8, self.physical_root);
        if (self.receipt) |receipt| {
            if (self.indexForPath(path)) |index| {
                return self.allocator.dupe(u8, receipt.artifacts[index].canonical_path);
            } else |err| if (err != error.InvalidManagedDownload) return err;
        }
        const captured_path = if (managed.pathIsWithinRoot(self.requested_root, absolute)) blk: {
            const offset = self.requested_root.len + @intFromBool(!std.fs.path.isSep(self.requested_root[self.requested_root.len - 1]));
            break :blk try std.fs.path.join(self.allocator, &.{ self.physical_root, absolute[offset..] });
        } else try self.allocator.dupe(u8, absolute);
        defer self.allocator.free(captured_path);
        const canonical = try std.Io.Dir.cwd().realPathFileAlloc(self.io, captured_path, self.allocator);
        defer self.allocator.free(canonical);
        const scope = self.constructionRoot();
        if (!std.mem.eql(u8, scope, canonical) and !managed.pathIsWithinRoot(scope, canonical)) return error.ModelArtifactOutsideRoot;
        // Directory importers verify their selected dependencies separately.
        // A file must always be selected through the captured receipt above.
        if (self.receipt != null and (try std.Io.Dir.cwd().statFile(self.io, canonical, .{})).kind != .directory) return error.InvalidManagedDownload;
        return self.allocator.dupe(u8, canonical);
    }

    fn verifyBinding(self: *const PublishedArtifacts, path: []const u8, expected: []const u8) !void {
        const current = try std.Io.Dir.cwd().realPathFileAlloc(self.io, path, self.allocator);
        defer self.allocator.free(current);
        if (!std.mem.eql(u8, current, expected)) return error.ModelArtifactsChanging;
    }
    pub fn verify(self: *const PublishedArtifacts) !void {
        try self.verifyBinding(self.storage_root, self.physical_root);
        if (self.directory_scope) |root| {
            try self.verifyBinding(root, root);
            const stat = try std.Io.Dir.cwd().statFile(self.io, root, .{});
            if (stat.kind != .directory or stat.inode != self.directory_scope_inode.?) return error.ModelArtifactsChanging;
        }
        if (self.direct_path) |path| try self.verifyBinding(path, self.physical_direct.?);
        if (!std.meta.eql(self.publication, try managed.publicationGeneration(self.allocator, self.io, self.root))) return error.ModelArtifactsChanging;
        // Native directory importers also discover members by relative names.
        // Keep every alias of a verified member bound to its captured target.
        if (self.receipt) |receipt| for (receipt.artifacts) |artifact| {
            const index = self.canonical_index.get(artifact.canonical_path).?;
            if (self.verified_stamps[index] == null) continue;
            const path = try std.fs.path.join(self.allocator, &.{ self.physical_root, artifact.path });
            defer self.allocator.free(path);
            try self.verifyBinding(path, artifact.canonical_path);
        };
        // A path verified earlier must still name those bytes when construction
        // finishes. Do not reuse a digest across replacement or in-place writes.
        for (self.verified_stamps, 0..) |maybe_stamp, index| if (maybe_stamp) |expected| {
            const current = managed.ArtifactStamp.fromStat(try std.Io.Dir.cwd().statFile(self.io, self.receipt.?.artifacts[index].canonical_path, .{ .follow_symlinks = false }));
            if (!std.meta.eql(expected, current)) return error.ModelArtifactsChanging;
        };
    }
};

test "model selection published artifacts reject unlisted and escaping files" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.onnx", .data = "weights" });
    try tmp.dir.writeFile(io, .{ .sub_path = "outside.onnx", .data = "outside" });
    const outside = try tmp.dir.realPathFileAlloc(io, "outside.onnx", a);
    defer a.free(outside);
    try tmp.dir.symLink(io, outside, "model/unlisted.onnx", .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("weights", &digest, .{});
    const json = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .artifacts = .{.{ .path = "model.onnx", .size = @as(u64, 7), .sha256 = std.fmt.bytesToHex(digest, .lower) }} }, .{});
    defer a.free(json);
    try tmp.dir.writeFile(io, .{ .sub_path = "model/.antfly-download-complete.json", .data = json });
    const root = try tmp.dir.realPathFileAlloc(io, "model", a);
    defer a.free(root);
    var published = try PublishedArtifacts.init(a, io, root, null);
    defer published.deinit();
    try std.testing.expectError(error.FileNotFound, published.read("unlisted.onnx", 128));
    try std.testing.expectError(error.InvalidManagedDownload, published.verifyPath(outside));
    const bytes = try published.read("model.onnx", 128);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("weights", bytes);
    try std.testing.expectError(error.FileTooLarge, published.read("model.onnx", 2));
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.onnx", .data = "changed" });
    try std.testing.expectError(error.ChecksumMismatch, published.read("model.onnx", 128));
}

test "model selection reader acquisition hashes each unchanged artifact once" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Probe = struct {
        var opens: usize = 0;
        var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (std.mem.endsWith(u8, path, "model.onnx")) opens += 1;
            return original(raw, dir, path, options);
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "weights" });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("weights", &digest, .{});
    const json = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .artifacts = .{.{ .path = "model.onnx", .size = @as(u64, 7), .sha256 = std.fmt.bytesToHex(digest, .lower) }} }, .{});
    defer a.free(json);
    try tmp.dir.writeFile(io, .{ .sub_path = managed.complete_filename, .data = json });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var table = io.vtable.*;
    Probe.original = table.dirOpenFile;
    table.dirOpenFile = Probe.open;
    const counted_io = std.Io{ .userdata = io.userdata, .vtable = &table };
    var published = try PublishedArtifacts.init(a, counted_io, root, null);
    defer published.deinit();
    Probe.opens = 0;
    try published.verifyFile("model.onnx");
    try published.verifyFile("model.onnx");
    const path = (try published.resolve("model.onnx")).?;
    defer a.free(path);
    try published.verifyPath(path); // Shared ONNX external tensors use the same proof.
    try published.verify();
    try std.testing.expectEqual(@as(usize, 1), Probe.opens);
    try io.sleep(std.Io.Duration.fromMilliseconds(2), .awake);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "changed" });
    try std.testing.expectError(error.ModelArtifactsChanging, published.verify());
    try std.testing.expectError(error.ChecksumMismatch, published.verifyFile("model.onnx"));
    try std.testing.expectEqual(@as(usize, 2), Probe.opens);
}

test "model selection canonical aliases retain the strongest checksum in either receipt order" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |both_known| {
        for ([_]bool{ false, true }) |reverse| {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "data" });
            try tmp.dir.symLink(io, "model.onnx", "alias.onnx", .{});
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash("data", &digest, .{});
            const hex = std.fmt.bytesToHex(digest, .lower);
            const upper_hex = std.fmt.bytesToHex(digest, .upper);
            const Artifact = struct { path: []const u8, size: u64 = 4, sha256: ?[]const u8 };
            const strong = Artifact{ .path = "model.onnx", .sha256 = &hex };
            const weak = Artifact{ .path = "alias.onnx", .sha256 = if (both_known) &upper_hex else null };
            const files = if (reverse) [_]Artifact{ weak, strong } else [_]Artifact{ strong, weak };
            const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .artifacts = files }, .{});
            defer a.free(receipt);
            try tmp.dir.writeFile(io, .{ .sub_path = managed.complete_filename, .data = receipt });
            const root = try tmp.dir.realPathFileAlloc(io, ".", a);
            defer a.free(root);
            var published = try PublishedArtifacts.init(a, io, root, null);
            defer published.deinit();
            try std.testing.expect(std.ascii.eqlIgnoreCase(&hex, published.receipt.?.find("model.onnx").?.sha256.?));
            try std.testing.expect(std.ascii.eqlIgnoreCase(&hex, published.receipt.?.find("alias.onnx").?.sha256.?));
            const data = try published.read("alias.onnx", 4);
            a.free(data);
            try std.testing.expectError(error.FileTooLarge, published.read("alias.onnx", 3));
            try published.verifyFile("model.onnx");
            try published.verifyFile("alias.onnx");
            try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "tada" });
            try std.testing.expectError(error.ChecksumMismatch, published.read("alias.onnx", 8));
            try std.testing.expectError(error.ChecksumMismatch, published.read("model.onnx", 8));
        }
    }
}

test "model selection reads retain artifact identity after leaf and ancestor retargets" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |ancestor| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.createDirPath(io, "first");
        try tmp.dir.createDirPath(io, "second");
        try tmp.dir.writeFile(io, .{ .sub_path = "first/model.onnx", .data = "aaaa" });
        try tmp.dir.writeFile(io, .{ .sub_path = "second/model.onnx", .data = "bbbb" });
        var first: [32]u8 = undefined;
        var second: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash("aaaa", &first, .{});
        std.crypto.hash.sha2.Sha256.hash("bbbb", &second, .{});
        const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .artifacts = .{
            .{ .path = "first/model.onnx", .size = @as(u64, 4), .sha256 = std.fmt.bytesToHex(first, .lower) },
            .{ .path = "second/model.onnx", .size = @as(u64, 4), .sha256 = std.fmt.bytesToHex(second, .lower) },
        } }, .{});
        defer a.free(receipt);
        try tmp.dir.writeFile(io, .{ .sub_path = managed.complete_filename, .data = receipt });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        var artifacts = try PublishedArtifacts.init(a, io, root, null);
        defer artifacts.deinit();
        const selected_path = (try artifacts.resolve("first/model.onnx")).?;
        defer a.free(selected_path);
        const original = try artifacts.readPath(selected_path, 4);
        defer a.free(original);
        try std.testing.expectEqualStrings("aaaa", original);
        if (ancestor) {
            try tmp.dir.rename("first", tmp.dir, "saved", io);
            try tmp.dir.symLink(io, "second", "first", .{ .is_directory = true });
        } else {
            try tmp.dir.deleteFile(io, "first/model.onnx");
            try tmp.dir.symLink(io, "../second/model.onnx", "first/model.onnx", .{});
        }
        try std.testing.expectError(error.ModelArtifactsChanging, artifacts.read("first/model.onnx", 4));
        try std.testing.expectError(error.ModelArtifactsChanging, artifacts.readPath(selected_path, 4));
        try std.testing.expectError(error.ModelArtifactsChanging, artifacts.verifyPath(selected_path));
        const other = try artifacts.read("second/model.onnx", 4);
        defer a.free(other);
        try std.testing.expectEqualStrings("bbbb", other);
    }
}

test "model selection logical paths keep receipt identity through a directory alias" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "installed");
    try tmp.dir.writeFile(io, .{ .sub_path = "installed/model.onnx", .data = "data" });
    try tmp.dir.writeFile(io, .{ .sub_path = "installed/.antfly-download-complete.json", .data =
        \\{"version":2,"artifacts":[{"path":"model.onnx","size":4}]}
    });
    try tmp.dir.symLink(io, "installed", "alias", .{ .is_directory = true });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const alias = try std.fs.path.join(a, &.{ root, "alias" });
    defer a.free(alias);
    const logical = try std.fs.path.join(a, &.{ alias, "model.onnx" });
    defer a.free(logical);
    var artifacts = try PublishedArtifacts.init(a, io, alias, null);
    defer artifacts.deinit();
    const bytes = try artifacts.readPath(logical, 4);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("data", bytes);
    try artifacts.verifyPath(logical);
    try artifacts.verify();
    // Checksum-free receipts still fence size/kind and subsequent replacement.
    try tmp.dir.writeFile(io, .{ .sub_path = "installed/model.onnx", .data = "longer" });
    try std.testing.expectError(error.ModelArtifactsChanging, artifacts.verify());
    try std.testing.expectError(error.InvalidManagedDownload, artifacts.verifyPath(logical));
}

test "model selection direct GGUF authority rejects unlisted aliases and foreign target receipts" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{ "owner", "foreign" }) |dir| {
        try tmp.dir.createDirPath(io, dir);
        var opened = try tmp.dir.openDir(io, dir, .{});
        defer opened.close(io);
        try opened.writeFile(io, .{ .sub_path = "model.gguf", .data = "data" });
        try opened.writeFile(io, .{ .sub_path = managed.complete_filename, .data =
            \\{"version":2,"artifacts":[{"path":"model.gguf","size":4}]}
        });
    }
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const legitimate = try std.fs.path.join(a, &.{ root, "owner/model.gguf" });
    defer a.free(legitimate);
    var direct = try PublishedArtifacts.init(a, io, legitimate, null);
    defer direct.deinit();
    try direct.verifyPath(legitimate);
    try direct.verify();
    try tmp.dir.symLink(io, "../foreign/model.gguf", "owner/escape.gguf", .{});
    const escape = try std.fs.path.join(a, &.{ root, "owner/escape.gguf" });
    defer a.free(escape);
    try std.testing.expectError(error.ModelArtifactNotPublished, PublishedArtifacts.init(a, io, escape, null));
    try tmp.dir.symLink(io, "model.gguf", "owner/unlisted.gguf", .{});
    const unlisted = try std.fs.path.join(a, &.{ root, "owner/unlisted.gguf" });
    defer a.free(unlisted);
    try std.testing.expectError(error.ModelArtifactNotPublished, PublishedArtifacts.init(a, io, unlisted, null));
    // An alias outside any installation inherits the published target's owner.
    try tmp.dir.symLink(io, "owner/model.gguf", "alias.gguf", .{});
    const alias = try std.fs.path.join(a, &.{ root, "alias.gguf" });
    defer a.free(alias);
    var inherited = try PublishedArtifacts.init(a, io, alias, null);
    defer inherited.deinit();
    try inherited.verifyPath(alias);
    const bytes = try inherited.readPath(alias, 4);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("data", bytes);
    try inherited.verify();
    // A busy requested owner must reject aliases instead of borrowing a stable
    // foreign receipt. This also binds publicationGeneration to that owner.
    try tmp.dir.writeFile(io, .{ .sub_path = "owner/.antfly-download-in-progress", .data = "" });
    try std.testing.expectError(error.IncompleteManagedDownload, PublishedArtifacts.init(a, io, escape, null));
    try std.testing.expectError(error.IncompleteManagedDownload, managed.publicationGeneration(a, io, escape));
}

test "model selection shared checksum proofs reject changed digests publications and targets" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("aaaa", &digest, .{});
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .artifacts = .{.{ .path = "model.onnx", .size = @as(u64, 4), .sha256 = std.fmt.bytesToHex(digest, .lower) }} }, .{});
    defer a.free(receipt);
    for ([_][]const u8{ "first", "second" }) |dir| {
        try tmp.dir.createDirPath(io, dir);
        var opened = try tmp.dir.openDir(io, dir, .{});
        defer opened.close(io);
        try opened.writeFile(io, .{ .sub_path = "model.onnx", .data = if (std.mem.eql(u8, dir, "first")) "aaaa" else "bbbb" });
        try opened.writeFile(io, .{ .sub_path = managed.complete_filename, .data = receipt });
    }
    try tmp.dir.symLink(io, "first", "current", .{ .is_directory = true });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const current = try std.fs.path.join(a, &.{ root, "current" });
    defer a.free(current);
    var cache = VerificationCache{};
    defer cache.deinit(a);
    {
        var source = try PublishedArtifacts.init(a, io, current, null);
        defer source.deinit();
        source.verification_cache = &cache;
        try source.verifyFile("model.onnx");
        try source.verify();
    }
    try tmp.dir.deleteFile(io, "current");
    try tmp.dir.symLink(io, "second", "current", .{ .is_directory = true });
    {
        var source = try PublishedArtifacts.init(a, io, current, null);
        defer source.deinit();
        source.verification_cache = &cache;
        try std.testing.expectError(error.ChecksumMismatch, source.verifyFile("model.onnx"));
    }
    try tmp.dir.deleteFile(io, "current");
    try tmp.dir.symLink(io, "first", "current", .{ .is_directory = true });
    // The original file/stamp still exists, but a receipt with a different
    // declared digest must not reuse its successful proof.
    const changed = try a.dupe(u8, receipt);
    defer a.free(changed);
    const marker = "\"sha256\":\"";
    const offset = std.mem.indexOf(u8, changed, marker).? + marker.len;
    @memset(changed[offset..][0..64], '0');
    try tmp.dir.writeFile(io, .{ .sub_path = "first/.antfly-download-complete.json", .data = changed });
    var invalid = try PublishedArtifacts.init(a, io, current, null);
    defer invalid.deinit();
    invalid.verification_cache = &cache;
    try std.testing.expectError(error.ChecksumMismatch, invalid.verifyFile("model.onnx"));
}

test "model selection shared proof cache bounds residency and preserves recent entries" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "data" });
    const stamp = managed.ArtifactStamp.fromStat(try tmp.dir.statFile(io, "model.onnx", .{}));
    var cache = VerificationCache{};
    defer cache.deinit(a);
    const hot: [32]u8 = @splat(0);
    try cache.remember(a, hot, stamp);
    for (0..VerificationCache.capacity + 32) |i| {
        var key: [32]u8 = @splat(1);
        std.mem.writeInt(u64, key[0..8], @intCast(i), .little);
        try cache.remember(a, key, stamp);
        try std.testing.expect(cache.hit(hot, stamp));
        try std.testing.expect(cache.entries.count() <= VerificationCache.capacity);
    }
    try std.testing.expectEqual(@as(usize, VerificationCache.capacity), cache.entries.count());
    var changed = stamp;
    changed.size += 1;
    try std.testing.expect(!cache.hit(hot, changed));
}

test "model selection concurrent acquisitions coalesce one checkpoint checksum" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "data" });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("data", &digest, .{});
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .artifacts = .{.{ .path = "model.onnx", .size = @as(u64, 4), .sha256 = std.fmt.bytesToHex(digest, .lower) }} }, .{});
    defer a.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = managed.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const Probe = struct {
        var opens: std.atomic.Value(usize) = .init(0);
        var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (std.mem.endsWith(u8, path, "/model.onnx")) {
                _ = opens.fetchAdd(1, .monotonic);
                std.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
            }
            return original(raw, dir, path, options);
        }
    };
    var table = io.vtable.*;
    Probe.original = table.dirOpenFile;
    table.dirOpenFile = Probe.open;
    const counted = std.Io{ .userdata = io.userdata, .vtable = &table };
    var cache = VerificationCache{};
    defer cache.deinit(a);
    var start: std.Io.Event = .unset;
    const Worker = struct {
        path: []const u8,
        io: std.Io,
        cache: *VerificationCache,
        start: *std.Io.Event,
        failure: ?anyerror = null,
        fn verify(self: *@This()) !void {
            var artifacts = try PublishedArtifacts.init(std.testing.allocator, self.io, self.path, null);
            defer artifacts.deinit();
            artifacts.verification_cache = self.cache;
            try self.start.wait(self.io);
            try artifacts.verifyFile("model.onnx");
            try artifacts.verify();
        }
        fn run(self: *@This()) void {
            self.verify() catch |err| {
                self.failure = err;
            };
        }
    };
    var workers: [4]Worker = undefined;
    var tasks: [workers.len]std.Io.Future(void) = undefined;
    var launched: usize = 0;
    defer {
        start.set(io);
        for (tasks[0..launched]) |*task| task.await(io);
    }
    for (&workers, 0..) |*worker, i| {
        worker.* = .{ .path = root, .io = counted, .cache = &cache, .start = &start };
        tasks[i] = try io.concurrent(Worker.run, .{worker});
        launched += 1;
    }
    start.set(io);
    for (&tasks) |*task| task.await(io);
    for (workers) |worker| try std.testing.expect(worker.failure == null);
    try std.testing.expectEqual(@as(usize, 1), Probe.opens.load(.acquire));
}

test "model selection canceled checksum wait releases no borrowed ownership" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "data" });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("data", &digest, .{});
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .artifacts = .{.{ .path = "model.onnx", .size = @as(u64, 4), .sha256 = std.fmt.bytesToHex(digest, .lower) }} }, .{});
    defer a.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = managed.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const Canceled = struct {
        fn wait(_: ?*anyopaque, _: *const u32, _: u32, _: std.Io.Timeout) std.Io.Cancelable!void {
            return error.Canceled;
        }
    };
    var table = io.vtable.*;
    table.futexWait = Canceled.wait;
    const canceled = std.Io{ .userdata = io.userdata, .vtable = &table };
    var cache = VerificationCache{};
    defer cache.deinit(a);
    var artifacts = try PublishedArtifacts.init(a, canceled, root, null);
    defer artifacts.deinit();
    artifacts.verification_cache = &cache;
    const key = VerificationCache.keyFor(artifacts.publication.?, &artifacts.receipt.?.artifacts[0]);
    const builder = &cache.builders[key[0] % cache.builders.len];
    try builder.lock(io);
    var held = true;
    defer if (held) builder.unlock(io);
    try std.testing.expectError(error.Canceled, artifacts.verifyFile("model.onnx"));
    try std.testing.expectEqual(@as(usize, 0), cache.entries.count());
    builder.unlock(io);
    held = false;
    artifacts.io = io;
    try artifacts.verifyFile("model.onnx");
    try artifacts.verify();
}

test "model selection proof cache allocation failure does not deny authenticated bytes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "data" });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("data", &digest, .{});
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .artifacts = .{.{ .path = "model.onnx", .size = @as(u64, 4), .sha256 = std.fmt.bytesToHex(digest, .lower) }} }, .{});
    defer a.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = managed.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var failing = std.testing.FailingAllocator.init(a, .{});
    var cache = VerificationCache{};
    defer cache.deinit(failing.allocator());
    var artifacts = try PublishedArtifacts.init(failing.allocator(), io, root, null);
    defer artifacts.deinit();
    artifacts.verification_cache = &cache;
    failing.fail_index = failing.alloc_index;
    try artifacts.verifyFile("model.onnx");
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.count());
    failing.fail_index = std.math.maxInt(usize);
    try artifacts.verify();
}

test "model selection construction keeps captured root and directory targets" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        for ([_][]const u8{ "first", "second" }) |name| {
            const nested = try std.fs.path.join(a, &.{ name, "encoder" });
            defer a.free(nested);
            try tmp.dir.createDirPath(io, nested);
        }
        try tmp.dir.writeFile(io, .{ .sub_path = "first/encoder/model.gguf", .data = "aaaa" });
        if (managed_model) try tmp.dir.writeFile(io, .{ .sub_path = "first/.antfly-download-complete.json", .data = "{\"version\":2,\"artifacts\":[{\"path\":\"encoder/model.gguf\",\"size\":4}]}" });
        try tmp.dir.symLink(io, "first", "current", .{ .is_directory = true });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const alias = try std.fs.path.join(a, &.{ root, "current" });
        defer a.free(alias);
        const component = try std.fs.path.join(a, &.{ alias, "encoder" });
        defer a.free(component);
        var artifacts = try PublishedArtifacts.init(a, io, alias, null);
        defer artifacts.deinit();
        try artifacts.verify();
        try tmp.dir.deleteFile(io, "current");
        try tmp.dir.symLink(io, "second", "current", .{ .is_directory = true });
        const captured = try artifacts.constructionPath(component);
        defer a.free(captured);
        const first = try tmp.dir.realPathFileAlloc(io, "first/encoder", a);
        defer a.free(first);
        try std.testing.expectEqualStrings(first, captured);
        try std.testing.expectError(error.ModelArtifactsChanging, artifacts.verify());
    }
}

test "model selection final verification detects member alias retargets" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "first.gguf", .data = "aaaa" });
    try tmp.dir.writeFile(io, .{ .sub_path = "second.gguf", .data = "bbbb" });
    try tmp.dir.symLink(io, "first.gguf", "model.gguf", .{});
    try tmp.dir.writeFile(io, .{ .sub_path = managed.complete_filename, .data = "{\"version\":2,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":4}]}" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var artifacts = try PublishedArtifacts.init(a, io, root, null);
    defer artifacts.deinit();
    try artifacts.verifyFile("model.gguf");
    try artifacts.verify();
    try tmp.dir.deleteFile(io, "model.gguf");
    try tmp.dir.symLink(io, "second.gguf", "model.gguf", .{});
    try std.testing.expectError(error.ModelArtifactsChanging, artifacts.verify());
}

test "model selection namespace rebasing does not inspect unused alternatives" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "first");
    try tmp.dir.createDirPath(io, "second");
    try tmp.dir.symLink(io, "first", "current", .{ .is_directory = true });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const alias = try std.fs.path.join(a, &.{ root, "current" });
    defer a.free(alias);
    var artifacts = try PublishedArtifacts.init(a, io, alias, null);
    defer artifacts.deinit();
    try tmp.dir.deleteFile(io, "current");
    try tmp.dir.symLink(io, "second", "current", .{ .is_directory = true });
    const optional = try std.fs.path.join(a, &.{ alias, "absent/missing.onnx" });
    defer a.free(optional);
    const rebased = try artifacts.namespacePath(optional);
    defer a.free(rebased);
    const expected = try std.fs.path.join(a, &.{ root, "first/absent/missing.onnx" });
    defer a.free(expected);
    try std.testing.expectEqualStrings(expected, rebased);
    // Rebasing grants no execution permission and does not follow the new alias.
    try std.testing.expectError(error.FileNotFound, artifacts.constructionPath(rebased));
    const outside = try std.fs.path.join(a, &.{ root, "second" });
    defer a.free(outside);
    const unchanged = try artifacts.namespacePath(outside);
    defer a.free(unchanged);
    try std.testing.expectEqualStrings(outside, unchanged);
    try std.testing.expectError(error.ModelArtifactOutsideRoot, artifacts.constructionPath(unchanged));
    try std.testing.expectError(error.ModelArtifactsChanging, artifacts.verify());
}

fn directDirectoryScopeAllocationScenario(a: std.mem.Allocator, file: []const u8, root: []const u8) !void {
    var artifacts = try PublishedArtifacts.init(a, std.testing.io, file, null);
    defer artifacts.deinit();
    try artifacts.bindDirectoryScope(root);
    try artifacts.bindDirectoryScope(root);
    const path = try std.fs.path.join(a, &.{ root, "config.json" });
    defer a.free(path);
    const config = try artifacts.constructionPath(path);
    defer a.free(config);
    try artifacts.verify();
}

test "model selection direct directory scopes contain sidecars and unwind allocation failures" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.createDirPath(io, "foreign");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.gguf", .data = "weights" });
    try tmp.dir.writeFile(io, .{ .sub_path = "model/config.json", .data = "{}" });
    try tmp.dir.writeFile(io, .{ .sub_path = "foreign/config.json", .data = "{}" });
    const root = try tmp.dir.realPathFileAlloc(io, "model", a);
    defer a.free(root);
    const file = try tmp.dir.realPathFileAlloc(io, "model/model.gguf", a);
    defer a.free(file);
    const foreign = try tmp.dir.realPathFileAlloc(io, "foreign", a);
    defer a.free(foreign);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, directDirectoryScopeAllocationScenario, .{ file, root });
    var artifacts = try PublishedArtifacts.init(a, io, file, null);
    defer artifacts.deinit();
    try std.testing.expectError(error.ModelArtifactOutsideRoot, artifacts.bindDirectoryScope(foreign));
    try artifacts.bindDirectoryScope(root);
    const outside = try std.fs.path.join(a, &.{ foreign, "config.json" });
    defer a.free(outside);
    try std.testing.expectError(error.ModelArtifactOutsideRoot, artifacts.constructionPath(outside));
    try tmp.dir.rename("model", tmp.dir, "previous", io);
    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.gguf", .data = "weights" });
    try std.testing.expectError(error.ModelArtifactsChanging, artifacts.verify());
}
