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

//! Catalog-owned, resumable reachability proof for an exact metadata document.
//! Retirement HEAD changes reuse the proof; metadata changes require a proof
//! covering every new root. Immutable inventories survive cold worker restarts.
const std = @import("std");
const storage = @import("objectstore");
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const rows = @import("row_commit.zig");
const inventory = @import("manifest_inventory.zig");
const retired = @import("retirement_index.zig");
const A = std.mem.Allocator;
const Digest = retired.Digest;
const Snapshot = struct { id: i64, uri: []const u8 };
const Roots = struct { snapshots: []const Snapshot, statistics: []const []const u8 };
const Progress = struct {
    version: u8 = 1,
    metadata_hash: [64]u8,
    roots: Roots,
    snapshot: usize = 0,
    manifest: usize = 0,
    file: usize = 0,
    statistic: usize = 0,
    list_marked: bool = false,
    members: ?Digest = null,
    manifests: ?Digest = null,
};
const Proof = struct { version: u8 = 1, metadata_hash: [64]u8, members: ?Digest };
pub const Result = struct { ready: bool, members: ?Digest = null, steps: usize = 0 };

pub const Builder = struct {
    files: rows.Files,
    prefix: []const u8,
    /// Used only to avoid replacing a newer build checkpoint with a stale one.
    /// Proofs themselves are immutable and bound to the complete metadata hash.
    head_key: []const u8,
    index: retired.Index,
    inventories: inventory.Store,

    pub fn init(a: A, files: rows.Files, prefix: []const u8, head_key: []const u8) Builder {
        return .{ .files = files, .prefix = prefix, .head_key = head_key, .index = retired.Index.init(a, files.client, files.bucket, prefix, files.context), .inventories = .{ .files = files, .client = files.client, .bucket = files.bucket, .prefix = prefix } };
    }
    pub fn deinit(self: *Builder) void {
        self.index.deinit();
    }
    fn key(self: Builder, a: A, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/{s}", .{ self.prefix, name });
    }
    fn get(self: *Builder, a: A, key_: []const u8, limit: usize) !?storage.GetResult {
        try self.files.context.ensureActive();
        var client = self.files.client;
        client.allocator = a;
        return client.getObject(self.files.bucket, key_, .{ .skip_metadata_probe = true, .max_response_bytes = limit, .cancellation = types.contextCancellation(&self.files.context) }) catch |err| switch (err) {
            error.ObjectNotFound, error.FileNotFound => null,
            else => return err,
        };
    }
    fn immutable(self: *Builder, a: A, key_: []const u8, bytes: []const u8) !void {
        try self.files.context.ensureActive();
        var client = self.files.client;
        client.allocator = a;
        var stored = client.putObject(self.files.bucket, key_, bytes, .{ .if_none_match = true, .cancellation = types.contextCancellation(&self.files.context) }) catch |err| {
            var existing = (try self.get(a, key_, @max(bytes.len, 64 * 1024))) orelse return err;
            defer existing.deinit(a);
            if (!std.mem.eql(u8, existing.body, bytes)) return error.InvalidLakeReachability;
            return;
        };
        stored.deinit(a);
    }
    fn canWork(self: *Builder) !bool {
        try self.files.context.ensureActive();
        if (self.files.context.deadline_ns) |deadline| {
            return @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s < deadline;
        }
        return true;
    }
    fn roots(a: A, bytes: []const u8) !Roots {
        const parsed = try metadata.parse(a, bytes);
        var snapshots: std.ArrayList(Snapshot) = .empty;
        for ((try metadata.get(parsed, "snapshots")).array.items) |snapshot| try snapshots.append(a, .{ .id = try metadata.int(try metadata.get(snapshot, "snapshot-id")), .uri = try metadata.str(try metadata.get(snapshot, "manifest-list")) });
        var statistics: std.ArrayList([]const u8) = .empty;
        inline for (.{ "statistics", "partition-statistics" }) |kind| {
            if (parsed.object.get(kind)) |entries| {
                if (entries != .array) return error.InvalidLakeMetadata;
                for (entries.array.items) |entry| try statistics.append(a, try metadata.str(try metadata.get(entry, "statistics-path")));
            }
        }
        return .{ .snapshots = snapshots.items, .statistics = statistics.items };
    }
    fn retarget(a: A, progress: *Progress, next: Roots, hash: [64]u8) !void {
        // Retain partially proved work under append-only metadata advancement.
        // Root removal/replacement must start a fresh exact membership set.
        var ids: std.AutoHashMapUnmanaged(i64, []const u8) = .empty;
        for (next.snapshots) |snapshot| try ids.put(a, snapshot.id, snapshot.uri);
        var statistics: std.StringHashMapUnmanaged(void) = .empty;
        for (next.statistics) |uri| try statistics.put(a, uri, {});
        var compatible = true;
        for (progress.roots.snapshots) |snapshot| {
            const uri = ids.get(snapshot.id) orelse {
                compatible = false;
                break;
            };
            if (!std.mem.eql(u8, snapshot.uri, uri)) {
                compatible = false;
                break;
            }
        }
        for (progress.roots.statistics) |uri| if (!statistics.contains(uri)) {
            compatible = false;
            break;
        };
        if (!compatible) {
            progress.* = .{ .metadata_hash = hash, .roots = next };
            return;
        }
        var ordered: std.ArrayList(Snapshot) = .empty;
        try ordered.appendSlice(a, progress.roots.snapshots);
        for (progress.roots.snapshots) |snapshot| _ = ids.remove(snapshot.id);
        for (next.snapshots) |snapshot| if (ids.contains(snapshot.id)) try ordered.append(a, snapshot);
        var stats: std.ArrayList([]const u8) = .empty;
        try stats.appendSlice(a, progress.roots.statistics);
        for (progress.roots.statistics) |uri| _ = statistics.remove(uri);
        for (next.statistics) |uri| if (statistics.contains(uri)) try stats.append(a, uri);
        progress.roots = .{ .snapshots = ordered.items, .statistics = stats.items };
        progress.metadata_hash = hash;
    }
    /// One bounded durable step. No coordinator-supplied membership is trusted.
    pub fn step(self: *Builder, a: A, table: types.Table, max_steps: usize) !Result {
        if (max_steps > types.max_reference_steps) return error.InvalidLakeReachability;
        const hash = types.digestHex(table.metadata_json);
        const proof_key = try self.key(a, try std.fmt.allocPrint(a, "versions/{s}.json", .{hash}));
        if (try self.get(a, proof_key, 16384)) |value| {
            var object = value;
            defer object.deinit(a);
            const proof = try std.json.parseFromSliceLeaky(Proof, a, object.body, .{});
            if (proof.version != 1 or !std.mem.eql(u8, &proof.metadata_hash, &hash)) return error.InvalidLakeReachability;
            return .{ .ready = true, .members = proof.members };
        }
        if (max_steps == 0) return .{ .ready = false };
        const progress_key = try self.key(a, "build.json");
        var saved = try self.get(a, progress_key, 64 * 1024 * 1024);
        defer if (saved) |*object| object.deinit(a);
        var progress: Progress = if (saved) |object|
            try std.json.parseFromSliceLeaky(Progress, a, object.body, .{ .allocate = .alloc_always })
        else
            .{ .metadata_hash = hash, .roots = try roots(a, table.metadata_json) };
        if (progress.version != 1 or progress.snapshot > progress.roots.snapshots.len or progress.statistic > progress.roots.statistics.len) return error.InvalidLakeReachability;
        if (!std.mem.eql(u8, &progress.metadata_hash, &hash)) try retarget(a, &progress, try roots(a, table.metadata_json), hash);
        var work = max_steps;
        // Read-only shared-manifest edges do not consume the mutation budget,
        // but still have their own bound and deadline checks.
        var edges: usize = 4096;
        var sources: usize = 4;
        while (work != 0 and try self.canWork() and progress.statistic < progress.roots.statistics.len) {
            progress.members = try self.index.insert(progress.members, progress.roots.statistics[progress.statistic]);
            progress.statistic += 1;
            work -= 1;
        }
        while (work != 0 and try self.canWork() and progress.snapshot < progress.roots.snapshots.len) {
            const uri = progress.roots.snapshots[progress.snapshot].uri;
            const list = (try self.inventories.load(a, uri, true, &sources)) orelse break;
            if (!progress.list_marked) {
                progress.members = try self.index.insert(progress.members, uri);
                progress.list_marked = true;
                work -= 1;
            }
            var lists: inventory.Store.Reader = .{ .store = &self.inventories, .a = a, .inventory_ = list };
            while (work != 0 and edges != 0 and try self.canWork()) {
                const manifest = (try lists.next(progress.manifest)) orelse {
                    progress.snapshot += 1;
                    progress.manifest = 0;
                    progress.file = 0;
                    progress.list_marked = false;
                    work -= 1;
                    break;
                };
                if (try self.index.contains(progress.manifests, manifest)) {
                    progress.manifest += 1;
                    progress.file = 0;
                    edges -= 1;
                    continue;
                }
                const files = (try self.inventories.load(a, manifest, false, &sources)) orelse break;
                var reader: inventory.Store.Reader = .{ .store = &self.inventories, .a = a, .inventory_ = files };
                while (work != 0 and edges != 0 and try self.canWork()) {
                    const file = (try reader.next(progress.file)) orelse {
                        progress.manifests = try self.index.insert(progress.manifests, manifest);
                        progress.manifest += 1;
                        progress.file = 0;
                        work -= 1;
                        break;
                    };
                    progress.members = try self.index.insert(progress.members, file);
                    progress.file += 1;
                    work -= 1;
                }
                if (sources == 0 or edges == 0 or !try self.canWork()) break;
            }
            if (sources == 0 or edges == 0 or !try self.canWork()) break;
        }
        const complete = progress.snapshot == progress.roots.snapshots.len and progress.statistic == progress.roots.statistics.len;
        if (complete) {
            try self.immutable(a, proof_key, try std.json.Stringify.valueAlloc(a, Proof{ .metadata_hash = hash, .members = progress.members }, .{}));
        }
        // Do not regress a newer metadata build when a stale worker resumes.
        // This guard is an optimization; retirement's HEAD CAS is the fence.
        var head = (try self.get(a, self.head_key, 64 * 1024)) orelse return error.LakeTableNotFound;
        defer head.deinit(a);
        const current = try std.json.parseFromSliceLeaky(std.json.Value, a, head.body, .{});
        if (std.mem.eql(u8, try metadata.str(try metadata.get(current, "metadata_location")), table.metadata_location)) {
            const bytes = try std.json.Stringify.valueAlloc(a, progress, .{});
            if (bytes.len > 64 * 1024 * 1024) return error.LakeMetadataTooLarge;
            var client = self.files.client;
            client.allocator = a;
            var stored = client.compareAndPut(self.files.bucket, progress_key, bytes, if (saved) |object| object.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = types.contextCancellation(&self.files.context) }) catch |err| switch (err) {
                error.PreconditionFailed => return .{ .ready = complete, .members = progress.members, .steps = max_steps - work },
                else => return err,
            };
            stored.deinit(a);
        }
        // The caller charges these mutations against traversal's remaining
        // work. A complete exact-metadata proof is usable immediately, even
        // when the next maintenance turn will observe another catalog commit.
        return .{ .ready = complete, .members = progress.members, .steps = max_steps - work };
    }
    pub fn contains(self: *Builder, root: ?Digest, uri: []const u8) !bool {
        return self.index.contains(root, uri);
    }
};
