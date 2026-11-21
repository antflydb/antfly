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
const manifest_mod = @import("../models/manifest.zig");
const model_manager = @import("model_manager.zig");
const backends = @import("../backends/backends.zig");
const dependencies = @import("../registry/artifact_dependencies.zig");
const c_file = @import("../util/c_file.zig");
const ExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

const max_cached_bytes_per_slot = 4 * 1024 * 1024;
const max_cached_primary_bytes = 1024 * 1024;
const safetensors = @import("../models/safetensors.zig");
const revalidate_after_ns = 60 * std.time.ns_per_s;
const dependency_retry_ns = std.time.ns_per_s;

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
        return .{ .snapshot = current, .slot = slot };
    }

    const Inspection = struct {
        const Member = struct { path: []const u8, stamp: managed.ArtifactStamp };
        const Members = std.StringHashMapUnmanaged(managed.ArtifactStamp);
        const Primary = enum { native, onnx };
        const PrimaryCache = struct {
            const Stamps = struct {
                arena: std.heap.ArenaAllocator,
                members: []const Member,

                fn destroy(self: *Stamps, owner: std.mem.Allocator) void {
                    self.arena.deinit();
                    owner.destroy(self);
                }
            };
            build: std.Io.Mutex = .init,
            available: std.atomic.Value(?*Stamps) = .init(null),
            retry_at: ?std.Io.Timestamp = null,

            fn check(self: *PrimaryCache, owner: std.mem.Allocator, artifacts: *const PublishedArtifacts, manifest: *const manifest_mod.ModelManifest, primary: Primary, control: ExecutionControl) !bool {
                if (self.available.load(.acquire)) |cached| {
                    try validate(cached.members, artifacts, control);
                    return true;
                }
                try managed.lockVerification(control, &self.build, artifacts.io);
                defer self.build.unlock(artifacts.io);
                if (self.available.load(.acquire)) |cached| {
                    try validate(cached.members, artifacts, control);
                    return true;
                }
                if (self.retry_at) |retry| {
                    if (std.Io.Timestamp.now(artifacts.io, .awake).nanoseconds < retry.nanoseconds) return false;
                }

                const current = try owner.create(Stamps);
                current.arena = .init(owner);
                var retained = false;
                defer if (!retained) current.destroy(owner);
                const a = current.arena.allocator();
                var members: Members = .empty;
                collectPrimaryMembers(&members, a, artifacts, manifest, primary, control) catch |err| return self.reject(artifacts.io, err);
                current.members = try freeze(a, &members);
                validate(current.members, artifacts, control) catch |err| return self.reject(artifacts.io, err);
                if (current.arena.queryCapacity() <= max_cached_primary_bytes) {
                    retained = true;
                    self.available.store(current, .release);
                }
                return true;
            }

            fn reject(self: *PrimaryCache, io: std.Io, err: anyerror) !bool {
                switch (err) {
                    error.OutOfMemory, error.Timeout, error.Canceled, error.Cancelled => return err,
                    else => {},
                }
                self.retry_at = .{ .nanoseconds = std.Io.Timestamp.now(io, .awake).nanoseconds + dependency_retry_ns };
                return false;
            }
        };
        arena: std.heap.ArenaAllocator,
        members: []const Member,
        primary: [2]PrimaryCache,
        backend_order: []const backends.BackendType,
        execution_manifest: manifest_mod.ModelManifest,
        summary: model_manager.CompatibilitySummary,

        fn add(members: *std.StringHashMapUnmanaged(managed.ArtifactStamp), a: std.mem.Allocator, artifacts: *const PublishedArtifacts, path: []const u8) !void {
            if (members.contains(path)) return;
            const stamp = try artifacts.requireCatalogMember(path);
            try members.put(a, try a.dupe(u8, path), stamp);
        }

        fn create(owner: std.mem.Allocator, artifacts: *const PublishedArtifacts, path: []const u8, manifest: *const manifest_mod.ModelManifest, backend_order: []const backends.BackendType, control: ExecutionControl) !*Inspection {
            const result = try owner.create(Inspection);
            errdefer owner.destroy(result);
            result.arena = .init(owner);
            errdefer result.arena.deinit();
            const a = result.arena.allocator();
            const execution_manifest = try manifest_mod.loadFromPublishedArtifacts(a, path, artifacts, false);
            const checks = try collectCommon(a, artifacts, &execution_manifest, control);
            result.primary = @splat(.{});
            const order = try a.dupe(backends.BackendType, backend_order);
            const summary = model_manager.compatibilitySummaryForBackendsWithControl(artifacts.allocator, path, manifest, backend_order, control) catch |err| switch (err) {
                error.OutOfMemory, error.Timeout, error.Cancelled, error.Canceled => return err,
                else => model_manager.CompatibilitySummary{ .level = .unknown, .code = .artifact_unreadable, .message = "model compatibility could not be determined" },
            };
            try validate(checks, artifacts, control);
            result.members = checks;
            result.backend_order = order;
            result.execution_manifest = execution_manifest;
            result.summary = summary;
            return result;
        }

        fn freeze(a: std.mem.Allocator, members: *const Members) ![]const Member {
            const checks = try a.alloc(Member, members.count());
            var it = members.iterator();
            for (checks) |*member| {
                const entry = it.next().?;
                member.* = .{ .path = entry.key_ptr.*, .stamp = entry.value_ptr.* };
            }
            return checks;
        }

        fn collectCommon(a: std.mem.Allocator, artifacts: *const PublishedArtifacts, manifest: *const manifest_mod.ModelManifest, control: ExecutionControl) ![]const Member {
            var members: Members = .empty;
            inline for (@typeInfo(manifest_mod.ModelManifest).@"struct".field_names) |field| {
                if (comptime std.mem.endsWith(u8, field, "_path") and @TypeOf(@field(manifest.*, field)) == ?[]const u8 and
                    !std.mem.eql(u8, field, "onnx_path") and !std.mem.eql(u8, field, "onnx_logical_path") and
                    !std.mem.eql(u8, field, "gguf_path") and !std.mem.eql(u8, field, "safetensors_path") and
                    !std.mem.eql(u8, field, "safetensors_index_path"))
                {
                    if (@field(manifest.*, field)) |member| {
                        try control.check();
                        try add(&members, a, artifacts, member);
                    }
                }
            }
            const components = [_]?[]const u8{
                manifest.visual_model_path,      manifest.audio_model_path,      manifest.text_projection_path,
                manifest.visual_projection_path, manifest.audio_projection_path,
            };
            for (components) |component| {
                const graph = component orelse continue;
                if (std.mem.endsWith(u8, graph, ".onnx"))
                    try collectOnnx(&members, a, artifacts, graph, graph, control);
            }
            return freeze(a, &members);
        }

        fn collectPrimaryMembers(members: *Members, a: std.mem.Allocator, artifacts: *const PublishedArtifacts, manifest: *const manifest_mod.ModelManifest, primary: Primary, control: ExecutionControl) !void {
            try control.check();
            switch (primary) {
                .onnx => if (manifest.onnx_path) |graph| {
                    try collectOnnx(members, a, artifacts, graph, manifest.onnx_logical_path orelse graph, control);
                },
                .native => switch (manifest.nativeWeightArtifactKind() orelse return) {
                    .gguf => try add(members, a, artifacts, manifest.gguf_path.?),
                    .safetensors => try add(members, a, artifacts, manifest.safetensors_path.?),
                    .sharded_safetensors => {
                        const index = manifest.safetensors_index_path.?;
                        try add(members, a, artifacts, index);
                        const bytes = try artifacts.readPath(index, safetensors.max_header_size);
                        defer artifacts.allocator.free(bytes);
                        var shards = try safetensors.inspectIndexDependencies(artifacts.allocator, index, bytes, control);
                        defer shards.deinit();
                        for (shards.paths[1..]) |shard| {
                            try control.check();
                            try add(members, a, artifacts, shard);
                        }
                    },
                },
            }
        }

        fn collectOnnx(members: *Members, a: std.mem.Allocator, artifacts: *const PublishedArtifacts, graph: []const u8, logical: []const u8, control: ExecutionControl) !void {
            try control.check();
            try add(members, a, artifacts, logical);
            var mapped = try c_file.MmapRegion.init(artifacts.allocator, graph);
            defer mapped.deinit();
            var tensors = dependencies.onnxWithControl(artifacts.allocator, logical, mapped.data, control) catch |err| switch (err) {
                error.OutOfMemory, error.Timeout, error.Canceled, error.Cancelled => return err,
                else => return error.InvalidManagedDownload,
            };
            defer tensors.deinit();
            var it = tensors.items.keyIterator();
            while (it.next()) |tensor| {
                try control.check();
                try add(members, a, artifacts, tensor.*);
            }
        }

        fn validate(members: []const Member, artifacts: *const PublishedArtifacts, control: ExecutionControl) !void {
            for (members) |member| {
                try control.check();
                const stamp = try artifacts.requireCatalogMember(member.path);
                if (!std.meta.eql(member.stamp, stamp)) return error.ModelArtifactsChanging;
            }
        }

        fn destroy(self: *Inspection, owner: std.mem.Allocator) void {
            for (&self.primary) |*primary| {
                if (primary.available.load(.monotonic)) |cached| cached.destroy(owner);
            }
            self.arena.deinit();
            owner.destroy(self);
        }
    };

    const Snapshot = struct {
        refs: std.atomic.Value(usize) = .init(1),
        artifacts: PublishedArtifacts,
        inspection_build: std.Io.Mutex = .init,
        inspection: std.atomic.Value(?*Inspection) = .init(null),
        root_stamp: managed.ArtifactStamp,
        checked_at: std.Io.Timestamp,

        fn release(self: *Snapshot) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            const a = self.artifacts.allocator;
            const path = self.artifacts.root;
            if (self.inspection.load(.monotonic)) |inspection| inspection.destroy(a);
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
        slot: *Slot,
        request_inspection: ?*Inspection = null,

        /// These views are only used by read-only catalog consumers. All
        /// temporary allocations and I/O belong to the borrowing request.
        pub fn view(self: Borrow, a: std.mem.Allocator, io: std.Io) PublishedArtifacts {
            var artifacts = self.snapshot.artifacts;
            artifacts.allocator = a;
            artifacts.io = io;
            return artifacts;
        }

        pub fn compatibility(self: *Borrow, artifacts: *const PublishedArtifacts, path: []const u8, manifest: *const manifest_mod.ModelManifest, backend_order: []const backends.BackendType, control: ExecutionControl) !model_manager.CompatibilitySummary {
            const snapshot = self.snapshot;
            if (snapshot.inspection.load(.acquire)) |cached| {
                if (std.mem.eql(backends.BackendType, backend_order, cached.backend_order)) {
                    Inspection.validate(cached.members, artifacts, control) catch |err| {
                        if (err == error.ModelArtifactsChanging or err == error.InvalidManagedDownload) self.invalidate();
                        return err;
                    };
                    return cached.summary;
                }
            }
            try managed.lockVerification(control, &snapshot.inspection_build, artifacts.io);
            defer snapshot.inspection_build.unlock(artifacts.io);
            if (snapshot.inspection.load(.acquire)) |cached| {
                if (std.mem.eql(backends.BackendType, backend_order, cached.backend_order)) {
                    Inspection.validate(cached.members, artifacts, control) catch |err| {
                        if (err == error.ModelArtifactsChanging or err == error.InvalidManagedDownload) self.invalidate();
                        return err;
                    };
                    return cached.summary;
                }
            }
            const owner = snapshot.artifacts.allocator;
            const inspection = try Inspection.create(owner, artifacts, path, manifest, backend_order, control);
            if (snapshot.inspection.load(.monotonic) == null and inspection.arena.queryCapacity() <= max_cached_bytes_per_slot - 2 * max_cached_primary_bytes) {
                snapshot.inspection.store(inspection, .release);
                return inspection.summary;
            }
            self.request_inspection = inspection;
            return inspection.summary;
        }

        pub fn executionManifest(self: Borrow) ?*const manifest_mod.ModelManifest {
            const inspection = self.request_inspection orelse self.snapshot.inspection.load(.acquire) orelse return null;
            return &inspection.execution_manifest;
        }

        pub fn filterEmbeddingRevisions(self: Borrow, artifacts: *const PublishedArtifacts, manifest: *const manifest_mod.ModelManifest, revisions: *model_manager.ModelManager.EmbeddingRevisions, control: ExecutionControl) !model_manager.ModelManager.EmbeddingRevisions {
            var rejected: model_manager.ModelManager.EmbeddingRevisions = .{};
            const inspection = self.request_inspection orelse self.snapshot.inspection.load(.acquire) orelse return rejected;
            var verified: [2]?bool = @splat(null);
            var kept: usize = 0;
            for (revisions.backends[0..revisions.len], revisions.revisions[0..revisions.len]) |backend, revision| {
                const primary: Inspection.Primary = if (model_manager.embeddingBackendUsesOnnxPrimary(manifest.*, backend)) .onnx else .native;
                const i = @backingInt(primary);
                if (verified[i] == null) {
                    // Inspect the selected closure lazily. A cold native-only
                    // probe never needs to map or walk the optional ONNX graph.
                    const cache = &inspection.primary[i];
                    verified[i] = cache.check(self.snapshot.artifacts.allocator, artifacts, &inspection.execution_manifest, primary, control) catch |err| switch (err) {
                        error.OutOfMemory, error.Timeout, error.Canceled, error.Cancelled => return err,
                        else => invalid: {
                            self.invalidate();
                            break :invalid false;
                        },
                    };
                }
                if (verified[i].?) {
                    revisions.backends[kept] = backend;
                    revisions.revisions[kept] = revision;
                    kept += 1;
                } else {
                    rejected.backends[rejected.len] = backend;
                    rejected.revisions[rejected.len] = revision;
                    rejected.len += 1;
                }
            }
            revisions.len = kept;
            return rejected;
        }

        pub fn invalidate(self: Borrow) void {
            self.slot.lock();
            const removed = self.slot.current == self.snapshot;
            if (removed) self.slot.current = null;
            self.slot.mutex.unlock();
            if (removed) self.snapshot.release();
        }

        pub fn release(self: Borrow) void {
            if (self.request_inspection) |inspection| inspection.destroy(self.snapshot.artifacts.allocator);
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
        return .{ .snapshot = snapshot, .slot = slot };
    }
};
