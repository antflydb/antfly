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

//! Bounded shared receipt snapshots for scoped, read-only model discovery.
const std = @import("std");
const managed = @import("../registry/managed_receipt.zig");
const PublishedArtifacts = @import("../registry/published_artifacts.zig").PublishedArtifacts;
const ExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

const max_cached_bytes_per_slot = 4 * 1024 * 1024;
const revalidate_after_ns = 60 * std.time.ns_per_s;

pub const Cache = struct {
    slots: [16]Slot = @splat(.{}),

    const Slot = struct {
        build: std.Io.Mutex = .init,
        mutex: std.atomic.Mutex = .unlocked,
        current: ?*Snapshot = null,

        fn lock(self: *Slot) void {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        }

        fn acquireCurrent(self: *Slot, io: std.Io, path: []const u8, publication: [32]u8, canonical: []const u8, stamp: managed.ArtifactStamp) ?*Snapshot {
            const now = std.Io.Timestamp.now(io, .awake);
            self.lock();
            defer self.mutex.unlock();
            const current = self.current orelse return null;
            const age = now.nanoseconds - current.checked_at.nanoseconds;
            if (age >= revalidate_after_ns or
                !std.mem.eql(u8, path, current.artifacts.root) or
                !std.mem.eql(u8, canonical, current.artifacts.physical_root) or
                !std.meta.eql(stamp, current.root_stamp) or
                !std.mem.eql(u8, &publication, &current.artifacts.installation.?.publication))
                return null;
            _ = current.refs.fetchAdd(1, .monotonic);
            return current;
        }

        fn publish(self: *Slot, snapshot: *Snapshot) void {
            _ = snapshot.refs.fetchAdd(1, .monotonic);
            self.lock();
            const old = self.current;
            self.current = snapshot;
            self.mutex.unlock();
            if (old) |previous| previous.release();
        }
    };

    fn reuse(slot: *Slot, a: std.mem.Allocator, io: std.Io, path: []const u8, publication: [32]u8, canonical: []const u8, stamp: managed.ArtifactStamp, control: ExecutionControl) !?Borrow {
        const current = slot.acquireCurrent(io, path, publication, canonical, stamp) orelse return null;
        var retained = false;
        defer if (!retained) current.release();
        managed.requireInstallation(a, io, path, current.artifacts.installation.?) catch |err| switch (err) {
            error.ModelArtifactsChanging, error.FileNotFound, error.NotDir => return null,
            else => return err,
        };
        try control.check();
        retained = true;
        return .{ .snapshot = current, .reused = true };
    }

    const Snapshot = struct {
        refs: std.atomic.Value(usize) = .init(1),
        artifacts: PublishedArtifacts,
        root_stamp: managed.ArtifactStamp,
        checked_at: std.Io.Timestamp,

        fn release(self: *Snapshot) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            const a = self.artifacts.allocator;
            const path = self.artifacts.root;
            self.artifacts.deinit();
            a.free(path);
            a.destroy(self);
        }

        fn cacheable(self: *const Snapshot) bool {
            const receipt = self.artifacts.receipt orelse return false;
            if (!receipt.all_checksums_declared) return false;
            // Include the parsed receipt, lookup tables, and resolved paths.
            var bytes = self.artifacts.installation.?.receipt_stamp.size *| 2;
            for (receipt.artifacts) |artifact| {
                bytes +|= 192 + artifact.canonical_path.len;
                if (bytes > max_cached_bytes_per_slot) return false;
            }
            return bytes <= max_cached_bytes_per_slot;
        }
    };

    pub const Borrow = struct {
        snapshot: *Snapshot,
        reused: bool,

        /// These views are only used by read-only catalog consumers. All
        /// temporary allocations and I/O belong to the borrowing request.
        pub fn view(self: Borrow, a: std.mem.Allocator, io: std.Io) PublishedArtifacts {
            var artifacts = self.snapshot.artifacts;
            artifacts.allocator = a;
            artifacts.io = io;
            artifacts.revalidate_catalog_members = self.reused;
            return artifacts;
        }

        pub fn release(self: Borrow) void {
            self.snapshot.release();
        }
    };

    pub fn deinit(self: *Cache) void {
        for (&self.slots) |*slot| if (slot.current) |current| {
            std.debug.assert(current.refs.load(.acquire) == 1);
            current.release();
            slot.current = null;
        };
    }

    /// The caller holds the shared publication lease for the entire borrow.
    /// Root and receipt changes invalidate immediately. The age limit catches
    /// out-of-band changes to unreferenced members inside the same publication.
    pub fn acquire(self: *Cache, owner: std.mem.Allocator, io: std.Io, path: []const u8, publication: [32]u8, control: ExecutionControl) !Borrow {
        const hash = std.hash.Wyhash.hash(0, path);
        const slot = &self.slots[hash % self.slots.len];
        try control.check();

        const root = try managed.publicationRootAlloc(owner, io, path);
        defer owner.free(root);
        const canonical = try std.Io.Dir.cwd().realPathFileAlloc(io, root, owner);
        defer owner.free(canonical);
        const stamp = managed.ArtifactStamp.fromStat(try std.Io.Dir.cwd().statFile(io, canonical, .{ .follow_symlinks = false }));
        if (try reuse(slot, owner, io, path, publication, canonical, stamp, control)) |borrow| return borrow;
        try managed.lockVerification(control, &slot.build, io);
        defer slot.build.unlock(io);
        if (try reuse(slot, owner, io, path, publication, canonical, stamp, control)) |borrow| return borrow;

        const owned_path = try owner.dupe(u8, path);
        errdefer owner.free(owned_path);
        var artifacts = try PublishedArtifacts.initWithControl(owner, io, owned_path, .{ .publication = publication }, control);
        errdefer artifacts.deinit();
        const snapshot = try owner.create(Snapshot);
        snapshot.* = .{ .artifacts = artifacts, .root_stamp = stamp, .checked_at = std.Io.Timestamp.now(io, .awake) };
        if (snapshot.cacheable()) slot.publish(snapshot);
        return .{ .snapshot = snapshot, .reused = false };
    }
};
