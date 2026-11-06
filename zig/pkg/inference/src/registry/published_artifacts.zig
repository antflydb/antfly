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
    receipt: ?managed.ValidatedReceipt,
    publication: ?[32]u8,
    verified_stamps: []?managed.ArtifactStamp = &.{},
    canonical_index: std.StringHashMapUnmanaged(usize) = .empty,

    pub fn init(a: std.mem.Allocator, io: std.Io, root: []const u8, expected: ?Expected) !PublishedArtifacts {
        var receipt = try managed.loadValidated(a, io, root);
        errdefer if (receipt) |*value| value.deinit();
        const publication = if (receipt) |value| value.publication_generation else null;
        if (expected) |value| if (!std.meta.eql(value.publication, publication)) return error.ModelArtifactsChanging;
        var index = std.StringHashMapUnmanaged(usize).empty;
        errdefer index.deinit(a);
        if (receipt) |value| for (value.artifacts, 0..) |artifact, i| try index.put(a, artifact.canonical_path, i);
        const verified = try a.alloc(?managed.ArtifactStamp, if (receipt) |value| value.artifacts.len else 0);
        @memset(verified, null);
        return .{ .allocator = a, .io = io, .root = root, .receipt = receipt, .publication = publication, .canonical_index = index, .verified_stamps = verified };
    }
    pub fn deinit(self: *PublishedArtifacts) void {
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
            const artifact = receipt.find(relative) orelse return error.InvalidManagedDownload;
            if (artifact.size != bytes.len) return error.InvalidManagedDownload;
            if (artifact.sha256) |expected| {
                if (expected.len != 64) return error.ChecksumMismatch;
                var decoded: [32]u8 = undefined;
                _ = std.fmt.hexToBytes(&decoded, expected) catch return error.ChecksumMismatch;
                var actual: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(bytes, &actual, .{});
                if (!std.mem.eql(u8, &decoded, &actual)) return error.ChecksumMismatch;
            }
        }
    }
    pub fn read(self: *const PublishedArtifacts, relative: []const u8, max_bytes: usize) ![]u8 {
        const path = (try self.resolve(relative)) orelse return error.FileNotFound;
        defer self.allocator.free(path);
        const bytes = std.Io.Dir.cwd().readFileAlloc(self.io, path, self.allocator, .limited(max_bytes)) catch |err| switch (err) {
            error.StreamTooLong => return error.FileTooLarge,
            else => return err,
        };
        errdefer self.allocator.free(bytes);
        try self.verifyBytes(relative, bytes);
        return bytes;
    }
    /// Verification belongs to one reader acquisition, not path resolution.
    /// Reuse it across graph collection, backend retries and shared external
    /// tensors only while the opened file's complete identity is unchanged.
    fn verifyIndex(self: *PublishedArtifacts, index: usize) !void {
        const artifact = &self.receipt.?.artifacts[index];
        const expected = artifact.sha256 orelse return;
        const stamp = managed.ArtifactStamp.fromStat(try std.Io.Dir.cwd().statFile(self.io, artifact.canonical_path, .{ .follow_symlinks = false }));
        if (self.verified_stamps[index]) |prior| if (std.meta.eql(prior, stamp)) return;
        self.verified_stamps[index] = try managed.verifyValidatedArtifactSha256AndStamp(self.io, artifact, expected);
    }
    pub fn verifyFile(self: *PublishedArtifacts, relative: []const u8) !void {
        if (self.receipt) |*receipt| {
            const index = receipt.path_index.get(relative) orelse return error.InvalidManagedDownload;
            try self.verifyIndex(index);
        }
    }
    /// ONNX external tensors are consumed by a component loader, not by
    /// read(). They must belong to the same receipt as the graph itself.
    pub fn verifyPath(self: *PublishedArtifacts, path: []const u8) !void {
        if (self.receipt != null) {
            const canonical = try managed.resolveRegularFilePath(self.allocator, self.io, path);
            defer self.allocator.free(canonical);
            const index = self.canonical_index.get(canonical) orelse return error.InvalidManagedDownload;
            try self.verifyIndex(index);
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
