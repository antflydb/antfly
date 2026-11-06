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
pub const PublishedArtifacts = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    root: []const u8,
    requested_root: []u8,
    receipt: ?managed.ValidatedReceipt,
    publication: ?[32]u8,
    verified_stamps: []?managed.ArtifactStamp = &.{},
    canonical_index: std.StringHashMapUnmanaged(usize) = .empty,

    pub fn init(a: std.mem.Allocator, io: std.Io, root: []const u8, expected: ?Expected) !PublishedArtifacts {
        const requested_root = try absolutePath(a, io, root);
        errdefer a.free(requested_root);
        var receipt = try managed.loadValidated(a, io, root);
        errdefer if (receipt) |*value| value.deinit();
        const publication = if (receipt) |value| value.publication_generation else null;
        if (expected) |value| if (!std.meta.eql(value.publication, publication)) return error.ModelArtifactsChanging;
        var index = std.StringHashMapUnmanaged(usize).empty;
        errdefer index.deinit(a);
        if (receipt) |value| for (value.artifacts, 0..) |artifact, i| try index.put(a, artifact.canonical_path, i);
        const verified = try a.alloc(?managed.ArtifactStamp, if (receipt) |value| value.artifacts.len else 0);
        @memset(verified, null);
        return .{ .allocator = a, .io = io, .root = root, .requested_root = requested_root, .receipt = receipt, .publication = publication, .canonical_index = index, .verified_stamps = verified };
    }
    pub fn deinit(self: *PublishedArtifacts) void {
        self.allocator.free(self.requested_root);
        self.allocator.free(self.verified_stamps);
        self.canonical_index.deinit(self.allocator);
        if (self.receipt) |*receipt| receipt.deinit();
    }
    pub fn resolve(self: *const PublishedArtifacts, relative: []const u8) !?[]u8 {
        if (self.receipt) |*receipt| {
            const artifact = receipt.find(relative) orelse return null;
            return try self.allocator.dupe(u8, artifact.canonical_path);
        }
        return managed.resolveContainedArtifactPath(self.allocator, self.io, self.root, relative) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => null,
            else => return err,
        };
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
        if (managed.pathIsWithinRoot(self.requested_root, absolute)) {
            const offset = self.requested_root.len + @intFromBool(!std.fs.path.isSep(self.requested_root[self.requested_root.len - 1]));
            if (self.receipt.?.path_index.get(absolute[offset..])) |index| return index;
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
        const root = try std.Io.Dir.cwd().realPathFileAlloc(self.io, self.root, self.allocator);
        defer self.allocator.free(root);
        if (!managed.pathIsWithinRoot(root, canonical)) return error.ModelArtifactOutsideRoot;
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
        if (self.receipt == null) return;
        try self.verifyPath(path);
        var graph = try @import("../backends/imported_onnx_session.zig").inspectArtifactSet(self.allocator, path);
        defer graph.deinit();
        for (graph.external_paths) |external| try self.verifyPath(external);
    }
    pub fn verify(self: *const PublishedArtifacts) !void {
        if (!std.meta.eql(self.publication, try managed.publicationGeneration(self.allocator, self.io, self.root))) return error.ModelArtifactsChanging;
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
