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
const publication_lease = @import("publication_lease.zig");
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

fn recoverInterrupted(a: std.mem.Allocator, io: std.Io, root: []const u8, path: []const u8) !bool {
    const bytes = std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(4096)) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer a.free(bytes);
    if (bytes.len == 71 and std.mem.startsWith(u8, bytes, "stable:")) {
        for (bytes[7..]) |c| if (!std.ascii.isHex(c)) return error.ModelArtifactsChanging;
        return false;
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
    return true;
}

fn writeStableGeneration(a: std.mem.Allocator, io: std.Io, path: []const u8) !void {
    var random: [32]u8 = undefined;
    io.random(&random);
    const bytes = "stable:" ++ std.fmt.bytesToHex(random, .lower);
    try download.writeFileAtomically(a, io, path, bytes);
}

pub const Publication = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []u8,
    lock: std.Io.File,
    write_lease: publication_lease.WriteLease,
    journal: []u8,
    started: bool = false,
    finished: bool = false,

    /// Serialize publishers and wait for constructors while readers retain
    /// the stable catalog. No busy journal is exposed during preparation.
    pub fn prepare(a: std.mem.Allocator, io: std.Io, root: []const u8, relative_destination: []const u8) !Publication {
        try publication_lease.Lease.assertMutable(a, io, root);
        const lock_path = try std.fs.path.join(a, &.{ root, ".antfly-catalog.lock" });
        defer a.free(lock_path);
        const lock = try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false, .lock = .exclusive });
        errdefer lock.close(io);
        const path = try std.fs.path.join(a, &.{ root, filename });
        errdefer a.free(path);
        // Recovery owns the old interrupted destination first. Release its
        // busy marker before waiting on a different installation's readers.
        const recovered = try recoverInterrupted(a, io, root, path);
        if (recovered or (try read(a, io, root)).kind == .legacy) try writeStableGeneration(a, io, path);
        const destination = try journalDestination(a, io, root, relative_destination);
        defer a.free(destination);
        var lease = try publication_lease.WriteLease.acquire(a, io, destination);
        errdefer lease.deinit(io);
        const journal = try std.json.Stringify.valueAlloc(a, Journal{ .destination = relative_destination }, .{});
        errdefer a.free(journal);
        if (journal.len > 4096) return error.ModelArtifactsChanging;
        return .{ .allocator = a, .io = io, .path = path, .lock = lock, .write_lease = lease, .journal = journal };
    }

    /// Compatibility boundary for callers that immediately mutate the store.
    pub fn begin(a: std.mem.Allocator, io: std.Io, root: []const u8, relative_destination: []const u8) !Publication {
        var publication = try prepare(a, io, root, relative_destination);
        errdefer publication.deinit();
        try publication.start();
        return publication;
    }

    pub fn start(self: *Publication) !void {
        if (self.started) return;
        if (self.finished) return error.ModelArtifactsChanging;
        try download.writeFileAtomically(self.allocator, self.io, self.path, self.journal);
        self.started = true;
    }

    fn startMutation(raw: *anyopaque) !void {
        const self: *Publication = @ptrCast(@alignCast(raw));
        try self.start();
    }
    fn mutationJournal(self: *Publication) publication_lease.MutationJournal {
        return .{ .ptr = self, .begin_fn = startMutation };
    }

    pub fn recoverDestination(self: *Publication) !void {
        if (self.finished) return error.ModelArtifactsChanging;
        try download.recoverManagedPublicationWithLease(self.allocator, self.io, self.write_lease.destination, &self.write_lease, self.mutationJournal());
    }

    pub fn commit(self: *Publication, transaction: *download.ManagedModelTransaction) !void {
        if (self.finished) return error.ModelArtifactsChanging;
        // Complete validation and unchanged-content checks under the write
        // lease, then journal immediately before the first namespace mutation.
        try transaction.commitWithLease(self.io, &self.write_lease, self.mutationJournal());
    }

    pub fn finish(self: *Publication) !void {
        if (self.finished) return;
        if (self.started) try writeStableGeneration(self.allocator, self.io, self.path);
        self.finished = true;
    }
    pub fn deinit(self: *Publication) void {
        const previous = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(previous);
        // Never authenticate a potentially failed rename/rollback implicitly.
        // Errors during preparation leave the existing catalog readable.
        self.write_lease.deinit(self.io);
        self.lock.close(self.io);
        self.allocator.free(self.journal);
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

test "model selection publication preparation keeps the catalog readable while construction holds a lease" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "owner/model");
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var initial = try Publication.begin(a, io, root, "owner/model");
    try initial.finish();
    initial.deinit();
    const prior = try read(a, io, root);
    const model = try tmp.dir.realPathFileAlloc(io, "owner/model", a);
    defer a.free(model);
    var held = try publication_lease.Lease.shared(a, io, model);
    defer held.deinit(io);
    const Probe = struct {
        var original: @FieldType(std.Io.VTable, "fileTryLock") = undefined;
        var root_path: []const u8 = undefined;
        var expected: Generation = undefined;
        var readable = false;
        fn lock(raw: ?*anyopaque, file: std.Io.File, mode: std.Io.File.Lock) std.Io.File.LockError!void {
            if (try original(raw, file, mode)) return;
            const current = read(std.testing.allocator, std.testing.io, root_path) catch return error.Unexpected;
            readable = std.meta.eql(expected, current);
            return error.Canceled;
        }
    };
    var table = io.vtable.*;
    Probe.original = table.fileTryLock;
    Probe.root_path = root;
    Probe.expected = prior;
    Probe.readable = false;
    table.fileLock = Probe.lock;
    const contended_io = std.Io{ .userdata = io.userdata, .vtable = &table };
    try std.testing.expectError(error.Canceled, Publication.prepare(a, contended_io, root, "owner/model"));
    try std.testing.expect(Probe.readable);
    try std.testing.expectEqual(prior, try read(a, io, root));
    held.deinit(io);
    var prepared = try Publication.prepare(a, io, root, "owner/model");
    defer prepared.deinit();
    try std.testing.expectEqual(prior, try read(a, io, root));
    try prepared.finish();
    try std.testing.expectEqual(prior, try read(a, io, root));
}

test "model selection commit validates content before exposing its mutation journal" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "owner/model");
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const model = try tmp.dir.realPathFileAlloc(io, "owner/model", a);
    defer a.free(model);
    var transaction = try download.ManagedModelTransaction.begin(a, io, model);
    defer transaction.deinit(io);
    var initial = try Publication.begin(a, io, root, "owner/model");
    try initial.finish();
    initial.deinit();
    const prior = try read(a, io, root);
    var publication = try Publication.prepare(a, io, root, "owner/model");
    defer publication.deinit();
    try std.testing.expectError(error.IncompleteManagedDownload, publication.commit(&transaction));
    try std.testing.expect(!publication.started);
    try std.testing.expectEqual(prior, try read(a, io, root));
}

test "model selection unchanged content comparison leaves the catalog available" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "owner/model");
    try tmp.dir.writeFile(io, .{ .sub_path = "owner/model/model.gguf", .data = "payload" });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("payload", &digest, .{});
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 1), .artifacts = .{.{ .path = "model.gguf", .size = @as(u64, 7), .sha256 = std.fmt.bytesToHex(digest, .lower) }} }, .{});
    defer a.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = "owner/model/.antfly-download-complete.json", .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const model = try tmp.dir.realPathFileAlloc(io, "owner/model", a);
    defer a.free(model);
    var transaction = try download.ManagedModelTransaction.begin(a, io, model);
    defer transaction.deinit(io);
    const weight = try std.fs.path.join(a, &.{ transaction.staging, "model.gguf" });
    defer a.free(weight);
    // A new inode with identical bytes forces the content comparison path.
    try download.writeFileAtomically(a, io, weight, "payload");
    const plan = try std.fs.path.join(a, &.{ transaction.staging, ".antfly-download-plan.json" });
    defer a.free(plan);
    try download.writeFileAtomically(a, io, plan, receipt);
    try download.completeManagedDownload(a, io, transaction.staging);
    var initial = try Publication.begin(a, io, root, "owner/model");
    try initial.finish();
    initial.deinit();
    const prior = try read(a, io, root);
    const Probe = struct {
        var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        var root_path: []const u8 = undefined;
        var expected: Generation = undefined;
        var checks: usize = 0;
        var readable = true;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (std.mem.endsWith(u8, path, "model.gguf")) {
                checks += 1;
                const current = read(std.testing.allocator, std.testing.io, root_path) catch return error.Unexpected;
                readable = readable and std.meta.eql(expected, current);
            }
            return original(raw, dir, path, options);
        }
    };
    var table = io.vtable.*;
    Probe.original = table.dirOpenFile;
    Probe.root_path = root;
    Probe.expected = prior;
    Probe.checks = 0;
    Probe.readable = true;
    table.dirOpenFile = Probe.open;
    const observed_io = std.Io{ .userdata = io.userdata, .vtable = &table };
    var publication = try Publication.prepare(a, observed_io, root, "owner/model");
    defer publication.deinit();
    try publication.commit(&transaction);
    try publication.finish();
    try std.testing.expect(Probe.checks > 0 and Probe.readable);
    try std.testing.expect(!publication.started);
    try std.testing.expectEqual(prior, try read(a, io, root));
}
