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
const builtin = @import("builtin");
const managed = @import("../registry/managed_receipt.zig");
const PublishedArtifacts = @import("../registry/published_artifacts.zig").PublishedArtifacts;
const manifest_mod = @import("../models/manifest.zig");
const model_manager = @import("model_manager.zig");
const loaded_runtime = @import("../models/loaded_runtime.zig");
const backends = @import("../backends/backends.zig");
const dependencies = @import("../registry/artifact_dependencies.zig");
const c_file = @import("../util/c_file.zig");
const ExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

const max_cached_bytes_per_slot = 4 * 1024 * 1024;
const max_cached_primary_bytes: usize = 1024 * 1024;
const max_cached_adapter_bytes: usize = 64 * 1024;
const gemma_identity = @import("../models/embedding_gemma2_identity.zig");
const tokenizer_loader = @import("../models/tokenizer_loader.zig");
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const gemma_inspection_limit = 512 * 1024 * 1024;
var gemma_inspection_bytes: std.atomic.Value(usize) = .init(0);

fn validateGemmaSidecar(artifacts: *const PublishedArtifacts, check: gemma_identity.CheckedSidecar, encoded: u64, control: ExecutionControl) !void {
    const estimate = @import("../models/load_plan.zig").tokenizerAdmissionPlan(std.math.cast(usize, encoded) orelse return error.MemoryBudgetExceeded, .huggingface) catch return error.MemoryBudgetExceeded;
    try validateGemmaSidecarWithEstimate(artifacts, check, @min(gemma_inspection_limit, estimate.peak.host_weight_bytes), control);
}

fn validateGemmaSidecarWithEstimate(artifacts: *const PublishedArtifacts, check: gemma_identity.CheckedSidecar, charge: usize, control: ExecutionControl) !void {
    validateGemmaSidecarWithBudget(artifacts, check, charge, control) catch |err| switch (err) {
        error.GemmaInspectionScratchExceeded => {
            if (charge == gemma_inspection_limit) return error.MemoryBudgetExceeded;
            // The admission estimate is not a format constraint. Retry an
            // underestimated tokenizer with the full shared scratch budget.
            validateGemmaSidecarWithBudget(artifacts, check, gemma_inspection_limit, control) catch |retry| switch (retry) {
                error.GemmaInspectionScratchExceeded => return error.MemoryBudgetExceeded,
                else => return retry,
            };
        },
        else => return err,
    };
}

fn validateGemmaSidecarWithBudget(artifacts: *const PublishedArtifacts, check: gemma_identity.CheckedSidecar, charge: usize, control: ExecutionControl) !void {
    // Reserve before reading or parsing. Other installations may still use
    // the rest of the shared discovery scratch allowance concurrently.
    var current = gemma_inspection_bytes.load(.monotonic);
    while (true) {
        try control.check();
        if (charge > gemma_inspection_limit - current) return error.ResourceTemporarilyUnavailable;
        current = gemma_inspection_bytes.cmpxchgWeak(current, current + charge, .acq_rel, .monotonic) orelse break;
    }
    defer _ = gemma_inspection_bytes.fetchSub(charge, .release);
    var scratch = BoundedAllocator{ .backing = artifacts.allocator, .limit = charge };
    const a = scratch.allocator();
    var borrowed = artifacts.*;
    borrowed.allocator = a;
    const result = switch (check) {
        .tokenizer => result: {
            const tokenizer = tokenizer_loader.loadDurableGemmaTokenizer(a, &borrowed) catch |err| break :result err;
            tokenizer.deinitSelf();
            break :result {};
        },
        .processor => result: {
            const bytes = tokenizer_loader.readGemmaSidecar(&borrowed, .processor) catch |err| break :result err;
            defer a.free(bytes);
            break :result gemma_identity.validateProcessor(a, bytes);
        },
    };
    result catch |err| {
        if (err == error.OutOfMemory and scratch.denied) return error.GemmaInspectionScratchExceeded;
        return err;
    };
    try control.check();
}

const safetensors = @import("../models/safetensors.zig");
const revalidate_after_ns = 60 * std.time.ns_per_s;
const CompatibilitySummary = model_manager.CompatibilitySummary;
const no_compatible_backend: CompatibilitySummary = .{
    .level = .incompatible,
    .code = .unsupported_backend,
    .message = "no configured backend accepts an artifact in this model bundle",
};
const unavailable_dependency: CompatibilitySummary = .{
    .level = .incompatible,
    .code = .artifact_unreadable,
    .message = "a required model dependency is unavailable",
};
const dependency_retry_ns = std.time.ns_per_s;

pub var onnx_compatibility_checks_for_test: if (builtin.is_test) usize else void = if (builtin.is_test) 0 else {};

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
        const borrow = Borrow{ .snapshot = current, .slot = slot };
        current.validateAliasBindings(a, io, control) catch |err| switch (err) {
            error.ModelArtifactsChanging, error.FileNotFound, error.NotDir, error.InvalidModelArtifactKind => {
                borrow.invalidate();
                return null;
            },
            else => return err,
        };
        try control.check();
        retained = true;
        return borrow;
    }

    const Inspection = struct {
        const Member = struct { path: []const u8, stamp: managed.ArtifactStamp };
        const MemberCollector = struct {
            allocator: std.mem.Allocator,
            artifacts: *const PublishedArtifacts,
            members: std.StringHashMapUnmanaged(managed.ArtifactStamp) = .empty,
            failed_member: ?[]const u8 = null,
            failed_stamp: ?managed.ArtifactStamp = null,

            fn stamp(self: *MemberCollector, path: []const u8) !managed.ArtifactStamp {
                return self.artifacts.requireCatalogMember(path) catch |err| {
                    if (err == error.CatalogArtifactUnavailable or err == error.ChecksumMismatch)
                        self.failed_member = try self.allocator.dupe(u8, path);
                    return err;
                };
            }

            fn add(self: *MemberCollector, path: []const u8) !u64 {
                if (self.members.get(path)) |record| return record.size;
                const record = try self.stamp(path);
                try self.members.put(self.allocator, try self.allocator.dupe(u8, path), record);
                return record.size;
            }

            fn verify(self: *MemberCollector, members: []const Member, control: ExecutionControl) !void {
                for (members) |member| {
                    try control.check();
                    const current = try self.stamp(member.path);
                    if (!std.meta.eql(member.stamp, current)) return error.ModelArtifactsChanging;
                }
            }

            fn freeze(self: *const MemberCollector) ![]const Member {
                const checks = try self.allocator.alloc(Member, self.members.count());
                var it = self.members.iterator();
                for (checks) |*member| {
                    const entry = it.next().?;
                    member.* = .{ .path = entry.key_ptr.*, .stamp = entry.value_ptr.* };
                }
                return checks;
            }
        };
        const FailedMember = struct {
            path: []const u8,
            reason: anyerror,
            stamp: ?managed.ArtifactStamp,

            fn capture(collector: *const MemberCollector, owner: std.mem.Allocator, reason: anyerror) !?FailedMember {
                const path = collector.failed_member orelse return null;
                if (path.len > 4096) return null;
                return .{ .path = try owner.dupe(u8, path), .reason = reason, .stamp = collector.failed_stamp };
            }

            fn recheck(self: *?FailedMember, owner: std.mem.Allocator, artifacts: *const PublishedArtifacts) !void {
                if (self.*) |failure| {
                    const current = artifacts.requireCatalogMember(failure.path) catch |err| switch (err) {
                        error.ChecksumMismatch, error.CatalogArtifactUnavailable => return failure.reason,
                        else => return err,
                    };
                    if (failure.stamp) |stamp| {
                        if (std.meta.eql(stamp, current)) return failure.reason;
                    }
                    owner.free(failure.path);
                    self.* = null;
                }
            }

            fn deinit(self: ?FailedMember, owner: std.mem.Allocator) void {
                if (self) |failure| owner.free(failure.path);
            }
        };

        const Dependency = enum { native, onnx, jina_adapter };
        const DependencyCache = struct {
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
            // Protected by build. Recheck only this member until it recovers;
            // an ONNX graph may be large and have many healthy dependencies.
            failure: ?FailedMember = null,
            retry_at: ?std.Io.Timestamp = null,

            fn check(self: *DependencyCache, owner: std.mem.Allocator, artifacts: *const PublishedArtifacts, manifest: *const manifest_mod.ModelManifest, dependency: Dependency, control: ExecutionControl) !bool {
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
                FailedMember.recheck(&self.failure, owner, artifacts) catch |err| switch (err) {
                    error.ChecksumMismatch, error.CatalogArtifactUnavailable, error.InvalidEmbeddingGemma2Processor, error.InvalidEmbeddingGemma2Tokenizer => return false,
                    else => return err,
                };
                if (self.retry_at) |retry| {
                    if (std.Io.Timestamp.now(artifacts.io, .awake).nanoseconds < retry.nanoseconds) return false;
                }

                const current = try owner.create(Stamps);
                current.arena = .init(owner);
                var retained = false;
                defer if (!retained) current.destroy(owner);
                var collector = MemberCollector{ .allocator = current.arena.allocator(), .artifacts = artifacts };
                collectDependencyMembers(&collector, manifest, dependency, control) catch |err| return self.reject(owner, collector, err);
                current.members = try collector.freeze();
                collector.verify(current.members, control) catch |err| return self.reject(owner, collector, err);
                if (current.arena.queryCapacity() <= if (dependency == .jina_adapter) max_cached_adapter_bytes else max_cached_primary_bytes) {
                    retained = true;
                    self.available.store(current, .release);
                }
                return true;
            }

            fn reject(self: *DependencyCache, owner: std.mem.Allocator, collector: MemberCollector, err: anyerror) !bool {
                switch (err) {
                    error.OutOfMemory, error.ResourceTemporarilyUnavailable, error.MemoryBudgetExceeded, error.Timeout, error.Canceled, error.Cancelled, error.ModelArtifactsChanging => return err,
                    else => {},
                }
                self.failure = try FailedMember.capture(&collector, owner, err);
                if (self.failure != null) {
                    self.retry_at = null;
                    return false;
                }
                self.retry_at = .{ .nanoseconds = std.Io.Timestamp.now(collector.artifacts.io, .awake).nanoseconds + dependency_retry_ns };
                return false;
            }
        };
        const Assessment = struct {
            lock: std.Io.Mutex = .init,
            available: std.atomic.Value(bool) = .init(false),
            summary: ?CompatibilitySummary = undefined,
        };

        arena: std.heap.ArenaAllocator,
        members: []const Member,
        dependencies_by_role: [std.enums.values(Dependency).len]DependencyCache,
        backends_by_type: [std.enums.values(backends.BackendType).len]Assessment,
        onnx_by_slot: [std.enums.values(model_manager.CompatibilityOnnxSlot).len]Assessment,
        execution_manifest: manifest_mod.ModelManifest,

        fn create(owner: std.mem.Allocator, artifacts: *const PublishedArtifacts, path: []const u8, failure: *?FailedMember, control: ExecutionControl) !*Inspection {
            const result = try owner.create(Inspection);
            errdefer owner.destroy(result);
            result.arena = .init(owner);
            errdefer result.arena.deinit();
            const a = result.arena.allocator();
            const execution_manifest = try manifest_mod.loadFromPublishedArtifacts(a, path, artifacts, false);
            var collector = MemberCollector{ .allocator = a, .artifacts = artifacts };
            collectCommon(&collector, &execution_manifest, control) catch |err| {
                failure.* = try FailedMember.capture(&collector, owner, err);
                return err;
            };
            result.members = try collector.freeze();
            collector.verify(result.members, control) catch |err| {
                failure.* = try FailedMember.capture(&collector, owner, err);
                return err;
            };
            result.dependencies_by_role = @splat(.{});
            result.backends_by_type = @splat(.{});
            result.onnx_by_slot = @splat(.{});
            result.execution_manifest = execution_manifest;
            return result;
        }

        fn backendSummary(self: *Inspection, artifacts: *const PublishedArtifacts, path: []const u8, backend: backends.BackendType, roles: []const Dependency, control: ExecutionControl) !CompatibilitySummary {
            const entry = &self.backends_by_type[@backingInt(backend)];
            if (entry.available.load(.acquire)) return entry.summary.?;
            try managed.lockVerification(control, &entry.lock, artifacts.io);
            defer entry.lock.unlock(artifacts.io);
            if (entry.available.load(.acquire)) return entry.summary.?;
            var check = OnnxCheck{ .inspection = self, .artifacts = artifacts };
            const summary = (try model_manager.compatibilitySummaryForBackendWithOnnxCheck(
                artifacts.allocator,
                path,
                &self.execution_manifest,
                backend,
                control,
                .{ .context = &check, .check = OnnxCheck.check },
            )) orelse no_compatible_backend;

            // Publish an assessment only for the stamps checked before
            // inspection. A repaired artifact must get a fresh assessment.
            var cacheable = true;
            for (roles) |role| {
                if (self.dependencies_by_role[@backingInt(role)].available.load(.acquire)) |stamps| {
                    try validate(stamps.members, artifacts, control);
                } else cacheable = false;
            }
            try validate(self.members, artifacts, control);
            if (cacheable) {
                entry.summary = summary;
                entry.available.store(true, .release);
            }
            return summary;
        }

        const OnnxCheck = struct {
            inspection: *Inspection,
            artifacts: *const PublishedArtifacts,

            fn check(raw: *anyopaque, slot: model_manager.CompatibilityOnnxSlot, path: []const u8, logical_path: ?[]const u8, control: ExecutionControl) !?CompatibilitySummary {
                const self: *OnnxCheck = @ptrCast(@alignCast(raw));
                const entry = &self.inspection.onnx_by_slot[@backingInt(slot)];
                if (entry.available.load(.acquire)) return entry.summary;
                try managed.lockVerification(control, &entry.lock, self.artifacts.io);
                defer entry.lock.unlock(self.artifacts.io);
                if (entry.available.load(.acquire)) return entry.summary;
                if (comptime builtin.is_test) onnx_compatibility_checks_for_test += 1;
                const summary = try model_manager.inspectOnnxCompatibility(self.artifacts.allocator, path, logical_path, control);
                if (slot == .primary) {
                    const cache = &self.inspection.dependencies_by_role[@backingInt(Dependency.onnx)];
                    const stamps = cache.available.load(.acquire) orelse return summary;
                    try validate(stamps.members, self.artifacts, control);
                } else {
                    try validate(self.inspection.members, self.artifacts, control);
                }
                entry.summary = summary;
                entry.available.store(true, .release);
                return summary;
            }
        };

        fn collectCommon(collector: *MemberCollector, manifest: *const manifest_mod.ModelManifest, control: ExecutionControl) !void {
            inline for (@typeInfo(manifest_mod.ModelManifest).@"struct".field_names) |field| {
                if (comptime std.mem.endsWith(u8, field, "_path") and @TypeOf(@field(manifest.*, field)) == ?[]const u8 and
                    !std.mem.eql(u8, field, "onnx_path") and !std.mem.eql(u8, field, "onnx_logical_path") and
                    !std.mem.eql(u8, field, "gguf_path") and !std.mem.eql(u8, field, "safetensors_path") and
                    !std.mem.eql(u8, field, "safetensors_index_path") and
                    !std.mem.eql(u8, field, "gguf_projector_path") and
                    !std.mem.eql(u8, field, "gliner_head_gguf_path") and
                    !std.mem.eql(u8, field, "gliner_head_safetensors_path") and
                    !std.mem.eql(u8, field, "jina_adapter_config_path") and
                    !std.mem.eql(u8, field, "jina_adapter_weights_path"))
                {
                    if (@field(manifest.*, field)) |member| {
                        try control.check();
                        _ = try collector.add(member);
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
                    try collectOnnx(collector, graph, graph, control);
            }
        }

        fn collectDependencyMembers(collector: *MemberCollector, manifest: *const manifest_mod.ModelManifest, dependency: Dependency, control: ExecutionControl) !void {
            const artifacts = collector.artifacts;
            try control.check();
            switch (dependency) {
                .onnx => if (manifest.onnx_path) |graph| {
                    try collectOnnx(collector, graph, manifest.onnx_logical_path orelse graph, control);
                },
                .jina_adapter => if (try manifest.jinaRetrievalAdapter()) |adapter| {
                    _ = try collector.add(adapter.config);
                    _ = try collector.add(adapter.weights);
                },
                .native => {
                    if (manifest.embedding_style == .embedding_gemma2) {
                        const weights = manifest.safetensors_path orelse return error.UnsupportedEmbeddingGemma2Weights;
                        const weight_member = try std.fs.path.join(artifacts.allocator, &.{ artifacts.root, std.fs.path.basename(weights) });
                        defer artifacts.allocator.free(weight_member);
                        if (try collector.add(weight_member) > gemma_identity.max_weight_bytes) return error.InvalidManagedDownload;
                        try gemma_identity.requireWeightBinding(artifacts.allocator, artifacts.io, artifacts.root, weights);
                        for (gemma_identity.sidecars) |sidecar| {
                            try control.check();
                            const member = try std.fs.path.join(artifacts.allocator, &.{ artifacts.root, sidecar });
                            defer artifacts.allocator.free(member);
                            if (try collector.add(member) > gemma_identity.max_sidecar_bytes) return error.InvalidManagedDownload;
                        }
                        // Check the complete receipt closure first. The cheap
                        // missing-member path stays independent of JSON parsing.
                        for (gemma_identity.sidecars) |sidecar| {
                            const check = gemma_identity.CheckedSidecar.fromPath(sidecar) orelse continue;
                            try control.check();
                            const member = try std.fs.path.join(collector.allocator, &.{ artifacts.root, sidecar });
                            const discovered = collector.members.get(member).?;
                            validateGemmaSidecar(artifacts, check, discovered.size, control) catch |err| switch (err) {
                                error.ChecksumMismatch, error.FileTooLarge, error.InvalidEmbeddingGemma2Processor, error.InvalidEmbeddingGemma2Tokenizer => {
                                    collector.failed_member = member;
                                    collector.failed_stamp = discovered;
                                    return if (err == error.FileTooLarge) check.invalidError() else err;
                                },
                                else => return err,
                            };
                        }
                    }
                    if (manifest.nativeCompanionPath()) |companion| _ = try collector.add(companion);
                    switch (manifest.nativeWeightArtifactKind() orelse return) {
                        .gguf => _ = try collector.add(manifest.gguf_path.?),
                        .safetensors => _ = try collector.add(manifest.safetensors_path.?),
                        .sharded_safetensors => {
                            const index = manifest.safetensors_index_path.?;
                            _ = try collector.add(index);
                            const bytes = try artifacts.readPath(index, safetensors.max_header_size);
                            defer artifacts.allocator.free(bytes);
                            var shards = try safetensors.inspectIndexDependencies(artifacts.allocator, index, bytes, control);
                            defer shards.deinit();
                            for (shards.paths[1..]) |shard| {
                                try control.check();
                                _ = try collector.add(shard);
                            }
                        },
                    }
                },
            }
        }

        fn collectOnnx(collector: *MemberCollector, graph: []const u8, logical: []const u8, control: ExecutionControl) !void {
            const artifacts = collector.artifacts;
            try control.check();
            _ = try collector.add(logical);
            var mapped = try c_file.MmapRegion.init(artifacts.allocator, graph);
            defer mapped.deinit();
            mapped.preserveFileCacheOnDeinit();
            const MemberReader = struct {
                fn size(raw: *anyopaque, path: []const u8) !u64 {
                    const self: *MemberCollector = @ptrCast(@alignCast(raw));
                    return self.add(path);
                }
            };
            var tensors = dependencies.onnxWithMemberSizes(artifacts.allocator, logical, mapped.data, control, .{
                .context = collector,
                .size = MemberReader.size,
            }) catch |err| switch (err) {
                error.OutOfMemory, error.Timeout, error.Canceled, error.Cancelled, error.ChecksumMismatch, error.ModelArtifactsChanging, error.CatalogArtifactUnavailable => return err,
                else => return error.InvalidManagedDownload,
            };
            tensors.deinit();
        }

        fn validate(members: []const Member, artifacts: *const PublishedArtifacts, control: ExecutionControl) !void {
            for (members) |member| {
                try control.check();
                const stamp = try artifacts.requireCatalogMember(member.path);
                if (!std.meta.eql(member.stamp, stamp)) return error.ModelArtifactsChanging;
            }
        }

        fn destroy(self: *Inspection, owner: std.mem.Allocator) void {
            for (&self.dependencies_by_role) |*cache| {
                if (cache.available.load(.monotonic)) |cached| cached.destroy(owner);
                FailedMember.deinit(cache.failure, owner);
            }
            self.arena.deinit();
            owner.destroy(self);
        }
    };

    const Snapshot = struct {
        refs: std.atomic.Value(usize) = .init(1),
        artifacts: PublishedArtifacts,
        inspection_build: std.Io.Mutex = .init,
        common_failure: ?Inspection.FailedMember = null,
        inspection: std.atomic.Value(?*Inspection) = .init(null),
        root_stamp: managed.ArtifactStamp,
        checked_at: std.Io.Timestamp,
        inherited_aliases: []const usize = &.{},

        fn release(self: *Snapshot) void {
            if (self.refs.fetchSub(1, .acq_rel) != 1) return;
            const a = self.artifacts.allocator;
            const path = self.artifacts.root;
            if (self.inspection.load(.monotonic)) |inspection| inspection.destroy(a);
            Inspection.FailedMember.deinit(self.common_failure, a);
            self.artifacts.deinit();
            a.free(self.inherited_aliases);
            a.free(path);
            a.destroy(self);
        }

        fn prepareForCache(self: *Snapshot, control: ExecutionControl) !bool {
            const receipt = if (self.artifacts.receipt) |*receipt| receipt else return false;
            if (!receipt.hasCompleteChecksums()) return false;
            // Include the parsed receipt, lookup tables, and resolved paths.
            var bytes = self.artifacts.installation.?.receipt_stamp.size *| 2;
            for (receipt.artifacts) |artifact| {
                bytes +|= 192 + artifact.canonical_path.len;
                if (bytes > max_cached_bytes_per_slot) return false;
            }
            if (!receipt.all_checksums_declared) {
                const a = self.artifacts.allocator;
                var aliases: std.ArrayList(usize) = .empty;
                defer aliases.deinit(a);
                const root = std.mem.trimEnd(u8, self.artifacts.physical_root, "/");
                for (receipt.artifacts, 0..) |artifact, index| {
                    try control.check();
                    if (std.mem.startsWith(u8, artifact.canonical_path, root)) {
                        const tail = artifact.canonical_path[root.len..];
                        if (tail.len > 0 and tail[0] == '/' and std.mem.eql(u8, tail[1..], artifact.path)) continue;
                    }
                    try aliases.append(a, index);
                }
                self.inherited_aliases = try aliases.toOwnedSlice(a);
                try self.validateAliasBindings(a, self.artifacts.io, control);
                self.artifacts.bound_declared_revision = try loaded_runtime.receiptedModelRevision(a, self.artifacts.io, receipt, null, control, false);
            }
            return true;
        }

        fn validateAliasBindings(self: *const Snapshot, a: std.mem.Allocator, io: std.Io, control: ExecutionControl) !void {
            for (self.inherited_aliases) |index| {
                try control.check();
                const artifact = self.artifacts.receipt.?.artifacts[index];
                const logical = try std.fs.path.join(a, &.{ self.artifacts.physical_root, artifact.path });
                defer a.free(logical);
                const resolved = try std.Io.Dir.cwd().realPathFileAlloc(io, logical, a);
                defer a.free(resolved);
                if (!std.mem.eql(u8, resolved, artifact.canonical_path)) return error.ModelArtifactsChanging;
            }
        }
    };

    pub const Borrow = struct {
        snapshot: *Snapshot,
        slot: *Slot,
        request_inspection: ?*Inspection = null,
        checked_roles: [std.enums.values(Inspection.Dependency).len]?bool = @splat(null),

        /// These views are only used by read-only catalog consumers. All
        /// temporary allocations and I/O belong to the borrowing request.
        pub fn view(self: Borrow, a: std.mem.Allocator, io: std.Io) PublishedArtifacts {
            var artifacts = self.snapshot.artifacts;
            artifacts.allocator = a;
            artifacts.io = io;
            return artifacts;
        }

        fn inspectionFor(self: *Borrow, artifacts: *const PublishedArtifacts, path: []const u8, control: ExecutionControl) !*Inspection {
            const snapshot = self.snapshot;
            if (snapshot.inspection.load(.acquire)) |cached| {
                try Inspection.validate(cached.members, artifacts, control);
                return cached;
            }
            try managed.lockVerification(control, &snapshot.inspection_build, artifacts.io);
            defer snapshot.inspection_build.unlock(artifacts.io);
            if (snapshot.inspection.load(.acquire)) |cached| {
                try Inspection.validate(cached.members, artifacts, control);
                return cached;
            }
            const owner = snapshot.artifacts.allocator;
            try Inspection.FailedMember.recheck(&snapshot.common_failure, owner, artifacts);
            const inspection = try Inspection.create(owner, artifacts, path, &snapshot.common_failure, control);
            if (inspection.arena.queryCapacity() <= max_cached_bytes_per_slot - 2 * max_cached_primary_bytes - max_cached_adapter_bytes) {
                snapshot.inspection.store(inspection, .release);
                return inspection;
            }
            self.request_inspection = inspection;
            return inspection;
        }

        pub fn compatibility(self: *Borrow, artifacts: *const PublishedArtifacts, path: []const u8, backend_order: []const backends.BackendType, control: ExecutionControl) !CompatibilitySummary {
            const inspection = self.inspectionFor(artifacts, path, control) catch |err| {
                if (err == error.ModelArtifactsChanging or self.snapshot.inspection.load(.acquire) != null and
                    (err == error.InvalidManagedDownload or err == error.ChecksumMismatch or err == error.CatalogArtifactUnavailable)) self.invalidate();
                return err;
            };
            var best: ?CompatibilitySummary = null;
            for (backend_order) |backend| {
                const summary = try self.checkBackend(inspection, artifacts, path, backend, control);
                best = model_manager.selectBetterCompatibility(best, summary);
                if (best.?.level == .compatible) break;
            }
            return best orelse no_compatible_backend;
        }

        fn checkBackend(self: *Borrow, inspection: *Inspection, artifacts: *const PublishedArtifacts, path: []const u8, backend: backends.BackendType, control: ExecutionControl) !CompatibilitySummary {
            const manifest = &inspection.execution_manifest;
            const primary: Inspection.Dependency = if (model_manager.embeddingBackendUsesOnnxPrimary(manifest.*, backend)) .onnx else .native;
            const roles = [_]Inspection.Dependency{ primary, .jina_adapter };
            const count: usize = if (model_manager.embeddingBackendUsesJinaAdapter(manifest.*, backend)) 2 else 1;
            for (roles[0..count]) |dependency| {
                const i = @backingInt(dependency);
                if (self.checked_roles[i] == null) {
                    const cache = &inspection.dependencies_by_role[i];
                    self.checked_roles[i] = cache.check(self.snapshot.artifacts.allocator, artifacts, &inspection.execution_manifest, dependency, control) catch |err| switch (err) {
                        error.OutOfMemory, error.ResourceTemporarilyUnavailable, error.MemoryBudgetExceeded, error.Timeout, error.Canceled, error.Cancelled => return err,
                        else => invalid: {
                            self.invalidate();
                            break :invalid false;
                        },
                    };
                }
                if (!self.checked_roles[i].?) return unavailable_dependency;
            }
            return inspection.backendSummary(artifacts, path, backend, roles[0..count], control) catch |err| switch (err) {
                error.OutOfMemory, error.ResourceTemporarilyUnavailable, error.MemoryBudgetExceeded, error.Timeout, error.Canceled, error.Cancelled => return err,
                error.ModelArtifactsChanging, error.InvalidManagedDownload, error.ChecksumMismatch, error.CatalogArtifactUnavailable => {
                    self.invalidate();
                    return unavailable_dependency;
                },
                else => return .{ .level = .unknown, .code = .artifact_unreadable, .message = "model compatibility could not be determined" },
            };
        }

        pub fn executionManifest(self: Borrow) ?*const manifest_mod.ModelManifest {
            const inspection = self.request_inspection orelse self.snapshot.inspection.load(.acquire) orelse return null;
            return &inspection.execution_manifest;
        }

        pub fn filterEmbeddingRevisions(self: *Borrow, artifacts: *const PublishedArtifacts, revisions: *model_manager.ModelManager.EmbeddingRevisions, allow_unknown: bool, control: ExecutionControl) !model_manager.ModelManager.EmbeddingRevisions {
            var rejected: model_manager.ModelManager.EmbeddingRevisions = .{};
            const inspection = self.request_inspection orelse self.snapshot.inspection.load(.acquire) orelse return rejected;
            var kept: usize = 0;
            for (revisions.backends[0..revisions.len], revisions.revisions[0..revisions.len]) |backend, revision| {
                const summary = try self.checkBackend(inspection, artifacts, artifacts.root, backend, control);
                const available = switch (summary.level) {
                    .compatible => true,
                    .unknown => allow_unknown,
                    .incompatible => false,
                };
                if (available) {
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

        pub fn verifyAliasBindings(self: Borrow, a: std.mem.Allocator, io: std.Io, control: ExecutionControl) !void {
            self.snapshot.validateAliasBindings(a, io, control) catch |err| switch (err) {
                error.ModelArtifactsChanging, error.FileNotFound, error.NotDir, error.InvalidModelArtifactKind => {
                    self.invalidate();
                    return error.ModelArtifactsChanging;
                },
                else => return err,
            };
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
        var snapshot_owns_artifacts = false;
        errdefer if (!snapshot_owns_artifacts) owner.free(owned_path);
        var artifacts = try PublishedArtifacts.initForSelectedArtifacts(owner, io, owned_path, .{ .publication = publication }, control);
        errdefer if (!snapshot_owns_artifacts) artifacts.deinit();
        const snapshot = try owner.create(Snapshot);
        snapshot.* = .{ .artifacts = artifacts, .root_stamp = stamp, .checked_at = std.Io.Timestamp.now(io, .awake) };
        snapshot_owns_artifacts = true;
        errdefer snapshot.release();
        if (try snapshot.prepareForCache(control)) slot.publish(snapshot);
        return .{ .snapshot = snapshot, .slot = slot };
    }
};

test "scoped model discovery retries a valid Gemma sidecar after scratch underestimation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const processor = @embedFile("../models/embedding_gemma2_processor.json");
    try tmp.dir.writeFile(io, .{ .sub_path = "processor_config.json", .data = processor });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var artifacts = try PublishedArtifacts.init(a, io, root, null);
    defer artifacts.deinit();

    try std.testing.expectError(error.GemmaInspectionScratchExceeded, validateGemmaSidecarWithBudget(&artifacts, .processor, processor.len, .{}));
    try validateGemmaSidecarWithEstimate(&artifacts, .processor, processor.len, .{});
}

test "scoped model discovery reuses inherited checksums until the binding changes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // The checked member itself is an alias. Changing it must invalidate the
    // inherited digest even though the checksum-free member has not moved.
    try tmp.dir.writeFile(io, .{ .sub_path = "model.safetensors", .data = "first" });
    try tmp.dir.writeFile(io, .{ .sub_path = "other.safetensors", .data = "other" });
    try tmp.dir.symLink(io, "model.safetensors", "checked.safetensors", .{});
    try tmp.dir.writeFile(io, .{ .sub_path = managed.complete_filename, .data =
        \\{"version":1,"artifacts":[
        \\{"path":"model.safetensors","size":5},
        \\{"path":"checked.safetensors","size":5,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}
    });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const publication = (try managed.publicationGeneration(a, io, root)).?;
    var cache: Cache = .{};
    defer cache.deinit();
    var first = try cache.acquire(a, io, root, publication, .{});
    defer first.release();
    try std.testing.expect(first.view(a, io).bound_declared_revision != null);
    var reused = try cache.acquire(a, io, root, publication, .{});
    defer reused.release();
    try std.testing.expect(first.snapshot == reused.snapshot);

    try tmp.dir.deleteFile(io, "checked.safetensors");
    try tmp.dir.symLink(io, "other.safetensors", "checked.safetensors", .{});
    try std.testing.expectError(error.ModelArtifactsChanging, reused.verifyAliasBindings(a, io, .{}));
    var changed = try cache.acquire(a, io, root, publication, .{});
    defer changed.release();
    try std.testing.expect(changed.snapshot != first.snapshot);
    try std.testing.expect(!changed.view(a, io).receipt.?.hasCompleteChecksums());
}
