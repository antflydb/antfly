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

//! One constant-size freshness token for a model store. Publishers serialize
//! only directory publication/recovery, never network download. Readers reject
//! an active/crashed publication instead of caching a partial alias inventory.
const std = @import("std");
const download = @import("download.zig");
pub const filename = ".antfly-catalog-generation";
pub const Generation = struct {
    kind: enum { legacy, publication, snapshot } = .legacy,
    digest: [32]u8 = @splat(0),
};
pub fn read(a: std.mem.Allocator, io: std.Io, root: []const u8) !Generation {
    for ([_][]const u8{ filename, "COMPLETE" }, 0..) |name, index| {
        const path = try std.fs.path.join(a, &.{ root, name });
        defer a.free(path);
        const bytes = std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(4096)) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => continue,
            else => return err,
        };
        defer a.free(bytes);
        if (index == 0) {
            if (bytes.len != 71 or !std.mem.startsWith(u8, bytes, "stable:")) return error.ModelArtifactsChanging;
            for (bytes[7..]) |c| if (!std.ascii.isHex(c)) return error.ModelArtifactsChanging;
        } else if (std.mem.indexOf(u8, bytes, "CATALOG.json") == null or std.mem.indexOf(u8, bytes, "SHA256SUMS") == null) return error.IncompleteManagedDownload;
        // COMPLETE is the immutable GCS snapshot publication boundary, already
        // checked by the snapshot checksum gate. Mutable legacy roots without
        // either contract retain the conservative directory-watch fallback.
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return .{ .kind = if (index == 0) .publication else .snapshot, .digest = digest };
    }
    return .{};
}

pub fn require(a: std.mem.Allocator, io: std.Io, root: []const u8, expected: Generation) !void {
    if (!std.meta.eql(expected, try read(a, io, root))) return error.ModelArtifactsChanging;
}

const Journal = struct { version: u8 = 1, destination: []const u8 };

// The journal names a destination relative to the store, never an arbitrary
// filesystem path. Prove its existing parent stays in the canonical store too.
fn journalDestination(a: std.mem.Allocator, io: std.Io, root: []const u8, relative: []const u8) ![]u8 {
    const receipt = @import("managed_receipt.zig");
    if (!receipt.artifactPathIsSafe(relative)) return error.ModelArtifactsChanging;
    const canonical_root = try std.Io.Dir.cwd().realPathFileAlloc(io, root, a);
    defer a.free(canonical_root);
    const destination = try std.fs.path.join(a, &.{ canonical_root, relative });
    defer a.free(destination);
    const canonical = try receipt.resolveRequestedFilePath(a, io, destination);
    errdefer a.free(canonical);
    if (!std.mem.startsWith(u8, canonical, canonical_root) or canonical.len <= canonical_root.len or
        (!std.fs.path.isSep(canonical_root[canonical_root.len - 1]) and !std.fs.path.isSep(canonical[canonical_root.len]))) return error.ModelArtifactsChanging;
    return canonical;
}

fn recoverInterrupted(a: std.mem.Allocator, io: std.Io, root: []const u8, path: []const u8) !void {
    const bytes = std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(4096)) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer a.free(bytes);
    if (bytes.len == 71 and std.mem.startsWith(u8, bytes, "stable:")) {
        for (bytes[7..]) |c| if (!std.ascii.isHex(c)) return error.ModelArtifactsChanging;
        return;
    }
    var parsed = std.json.parseFromSlice(Journal, a, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ModelArtifactsChanging,
    };
    defer parsed.deinit();
    if (parsed.value.version != 1) return error.ModelArtifactsChanging;
    const destination = try journalDestination(a, io, root, parsed.value.destination);
    defer a.free(destination);
    try download.recoverManagedPublication(a, io, destination);
}

pub const Publication = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []u8,
    lock: std.Io.File,
    finished: bool = false,

    pub fn begin(a: std.mem.Allocator, io: std.Io, root: []const u8, relative_destination: []const u8) !Publication {
        try @import("publication_lease.zig").Lease.assertMutable(a, io, root);
        const lock_path = try std.fs.path.join(a, &.{ root, ".antfly-catalog.lock" });
        defer a.free(lock_path);
        const lock = try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false, .lock = .exclusive });
        errdefer lock.close(io);
        const path = try std.fs.path.join(a, &.{ root, filename });
        errdefer a.free(path);
        // Never overwrite evidence of an earlier interrupted publication.
        // Recover its named directory while the namespace lock is exclusive.
        try recoverInterrupted(a, io, root, path);
        const destination = try journalDestination(a, io, root, relative_destination);
        defer a.free(destination);
        const journal = try std.json.Stringify.valueAlloc(a, Journal{ .destination = relative_destination }, .{});
        defer a.free(journal);
        if (journal.len > 4096) return error.ModelArtifactsChanging;
        try download.writeFileAtomically(a, io, path, journal);
        return .{ .allocator = a, .io = io, .path = path, .lock = lock };
    }
    pub fn finish(self: *Publication) !void {
        if (self.finished) return;
        var random: [32]u8 = undefined;
        self.io.random(&random);
        const bytes = "stable:" ++ std.fmt.bytesToHex(random, .lower);
        try download.writeFileAtomically(self.allocator, self.io, self.path, bytes);
        self.finished = true;
    }
    pub fn deinit(self: *Publication) void {
        const previous = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(previous);
        // Only an explicit successful finish authenticates the namespace.
        // A failed rollback may leave it partially restored; never erase that
        // evidence in a destructor. The next locked pull recovers it.
        self.lock.close(self.io);
        self.allocator.free(self.path);
    }
};

test "model selection catalog publication fences interrupted and concurrent mutation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    try std.testing.expectEqual(.legacy, (try read(a, io, root)).kind);
    var first = try Publication.begin(a, io, root, "fixture");
    try std.testing.expectError(error.ModelArtifactsChanging, read(a, io, root));
    try first.finish();
    const prior = try read(a, io, root);
    first.deinit();
    try std.testing.expectEqual(.publication, prior.kind);
    // Simulate a killed publisher. Taking its OS lock and finishing the next
    // publication restores a stable generation rather than serving stale data.
    var aborted = try Publication.begin(a, io, root, "fixture");
    aborted.deinit();
    try std.testing.expectError(error.ModelArtifactsChanging, read(a, io, root));
    var recovery = try Publication.begin(a, io, root, "fixture");
    try recovery.finish();
    recovery.deinit();
    const next = try read(a, io, root);
    try std.testing.expect(!std.meta.eql(prior, next));
}

test "model selection publication journal rejects escaping and malformed recovery" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "store");
    const root = try tmp.dir.realPathFileAlloc(io, "store", a);
    defer a.free(root);
    try std.testing.expectError(error.ModelArtifactsChanging, Publication.begin(a, io, root, "../outside"));
    for ([_][]const u8{
        "{\"version\":1,\"destination\":\"../outside\"}",
        "publishing\n",
        "{\"version\":2,\"destination\":\"fixture\"}",
    }) |journal| {
        try tmp.dir.writeFile(io, .{ .sub_path = "store/" ++ filename, .data = journal });
        try std.testing.expectError(error.ModelArtifactsChanging, Publication.begin(a, io, root, "fixture"));
        try std.testing.expectError(error.ModelArtifactsChanging, read(a, io, root));
    }
}

test "model selection failed journal recovery retains the namespace fence" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "store/acme/.model.antfly-download-backup");
    try tmp.dir.writeFile(io, .{ .sub_path = "store/acme/.model.antfly-download-backup/.antfly-download-complete.json", .data = "{}" });
    const root = try tmp.dir.realPathFileAlloc(io, "store", a);
    defer a.free(root);
    var interrupted = try Publication.begin(a, io, root, "acme/model");
    interrupted.deinit();
    try std.testing.expectError(error.InvalidManagedDownload, Publication.begin(a, io, root, "unrelated"));
    try std.testing.expectError(error.ModelArtifactsChanging, read(a, io, root));
    try tmp.dir.access(io, "store/acme/.model.antfly-download-backup/.antfly-download-complete.json", .{});
    try tmp.dir.createDirPath(io, "outside");
    try tmp.dir.symLink(io, "../outside", "store/escape", .{});
    // Repairing a journal must never follow an escaping parent symlink.
    try tmp.dir.writeFile(io, .{ .sub_path = "store/" ++ filename, .data = "{\"version\":1,\"destination\":\"escape/model\"}" });
    try std.testing.expectError(error.ModelArtifactsChanging, Publication.begin(a, io, root, "unrelated"));
}

test "model selection immutable snapshots reject publication before creating a journal" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "COMPLETE", .data = "CATALOG.json SHA256SUMS" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    try std.testing.expectError(error.ReadOnlyFileSystem, Publication.begin(a, io, root, "fixture"));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access(io, filename, .{}));
    const destination = try std.fs.path.join(a, &.{ root, "fixture" });
    defer a.free(destination);
    try std.testing.expectError(error.ReadOnlyFileSystem, download.ManagedModelTransaction.begin(a, io, destination));
    try std.testing.expectEqual(.snapshot, (try read(a, io, root)).kind);
}
