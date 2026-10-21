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

//! Owned preparation dependencies. These retain immutable inputs and original
//! policy authority; observations do not replace topology/read conflict leases.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const schema = @import("schema_registry.zig");
const indexes = @import("catalog/index_manager.zig");
const relational = @import("relational_index_catalog.zig");
const policy = @import("row_policy_gate.zig");
const revisions = @import("../document_mutation_revision.zig");
const namespace = @import("doc_identity_namespace.zig");
const platform_time = @import("antfly_platform").time;

pub const GeneratedRead = struct { key: revisions.Key, revision: u64 };
pub const TransformRead = struct { key: []const u8, value: ?[]u8, expected_version: u64, revision: ?GeneratedRead = null };
pub const TransformPredicate = struct { key: []const u8, expected_version: u64, revision: ?GeneratedRead };
pub const Sources = struct {
    // Observe these before preparation; never rebind an old view to a newer
    // catalog or database namespace just because its contents look compatible.
    schema_namespace: u64,
    catalog_revision: u64,
    schema_view: ?schema.SchemaView,
    require_current_schema: bool,
    write_plan: ?indexes.IndexManager.WritePlanSnapshotView,
    relational_indexes: ?relational.WriteSnapshot,
    policy_lease: ?*const policy.Gate.Lease,
    generated_reads: []const GeneratedRead,
    transform_reads: []const TransformRead,
};

pub const Owned = struct {
    alloc: std.mem.Allocator,
    owner: *const anyopaque,
    store: *const @import("../docstore.zig").DocStore,
    root_generation: u64,
    identity_namespace: namespace.Namespace,
    schema_namespace: u64,
    import_revision: u64,
    catalog_revision: u64,
    range_start: []const u8 = &.{},
    range_end: []const u8 = &.{},
    schema_view: ?schema.SchemaView = null,
    require_current_schema: bool,
    write_plan: ?indexes.IndexManager.WritePlanSnapshotView = null,
    write_plan_generation: u64,
    relational_indexes: ?relational.WriteSnapshot = null,
    policy_lease: ?policy.Gate.Lease = null,
    generated_reads: []GeneratedRead = &.{},
    transform_reads: []TransformPredicate = &.{},
    metadata_reads: ?@import("prepared_read_set.zig").Owned = null,
    preapply_reads: ?@import("prepared_read_set.zig").Owned = null,

    pub fn capture(alloc: std.mem.Allocator, core: anytype, source: Sources, original: ?admission.Port) !Owned {
        var trace: admission.RejectionTrace = .{};
        var result = captureInner(alloc, core, source, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| {
            result.deinit();
            return cause;
        }
        return result;
    }

    fn captureInner(alloc: std.mem.Allocator, core: anytype, source: Sources, control: ?admission.Port) !Owned {
        if (control) |original| try original.checkpoint();
        if (core.store.portableImportPublicationInProgress()) return error.PreparedGenerationChanged;
        var result: Owned = .{
            .alloc = alloc,
            .owner = core,
            .store = core.store,
            .root_generation = core.root_generation,
            .identity_namespace = core.identity_namespace,
            .schema_namespace = source.schema_namespace,
            .import_revision = try core.store.portableImportRevision(),
            .catalog_revision = source.catalog_revision,
            .require_current_schema = source.require_current_schema,
            .write_plan_generation = if (source.write_plan) |view| view.generation() else core.index_manager.writePlanGeneration(),
        };
        errdefer result.deinit();
        if (source.schema_view) |view| result.schema_view = view.clone();
        // Retain only the immutable inputs actually used by preparation. A
        // snapshot-free in-lock assembly needs a generation observation, not
        // an allocation-heavy catalog snapshot built under the apply fence.
        if (source.write_plan) |view| result.write_plan = view.clone();
        if (source.relational_indexes) |view| result.relational_indexes = view.clone();
        if (source.policy_lease) |lease| {
            try lease.checkAt(nowSeconds());
            result.policy_lease = lease.clone();
        }
        const range = core.byteRange();
        result.range_start = try copyKey(alloc, range.start, control);
        result.range_end = try copyKey(alloc, range.end, control);
        result.generated_reads = try alloc.alloc(GeneratedRead, source.generated_reads.len);
        for (source.generated_reads, result.generated_reads) |read, *owned| {
            if (control) |original| try original.checkpoint();
            owned.* = read;
        }
        result.transform_reads = blk: {
            const transforms = try alloc.alloc(TransformPredicate, source.transform_reads.len);
            errdefer alloc.free(transforms);
            var initialized: usize = 0;
            errdefer for (transforms[0..initialized]) |read| alloc.free(read.key);
            for (source.transform_reads, transforms) |read, *owned| {
                if (control) |original| try original.checkpoint();
                owned.* = .{ .key = try copyKey(alloc, read.key, control), .expected_version = read.expected_version, .revision = read.revision };
                initialized += 1;
            }
            break :blk transforms;
        };
        try result.validate(core, control);
        return result;
    }

    fn copyKey(alloc: std.mem.Allocator, key: []const u8, control: ?admission.Port) ![]const u8 {
        const result = try alloc.alloc(u8, key.len);
        errdefer alloc.free(result);
        var offset: usize = 0;
        while (offset < key.len) {
            if (control) |original| try original.checkpoint();
            const end = offset + @min(@as(usize, 4096), key.len - offset);
            @memcpy(result[offset..end], key[offset..end]);
            offset = end;
        }
        return result;
    }

    /// Call while the same publication/apply fence is held. This validates the
    /// captured explicit read set, not group/backend reads which lack witnesses.
    pub fn validate(self: *const Owned, core: anytype, original: ?admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        self.validateInner(core, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
    }

    fn validateInner(self: *const Owned, core: anytype, control: ?admission.Port) !void {
        if (control) |original| try original.checkpoint();
        if (self.owner != @as(*const anyopaque, core) or self.store != core.store or
            self.root_generation != core.root_generation or !self.identity_namespace.eql(core.identity_namespace) or
            self.schema_namespace != core.schemaNamespaceGeneration() or
            core.store.portableImportPublicationInProgress() or self.import_revision != try core.store.portableImportRevision()) return error.PreparedGenerationChanged;
        const range = core.byteRange();
        if (!std.mem.eql(u8, self.range_start, range.start) or !std.mem.eql(u8, self.range_end, range.end)) return error.PreparedGenerationChanged;
        if (!core.store.resolver_catalog_revision.matches(self.catalog_revision)) return error.PreparedGenerationChanged;
        if (self.require_current_schema) {
            if (self.schema_view) |view| {
                if (!core.isSchemaViewCurrent(view)) return error.PreparedGenerationChanged;
            } else {
                var current = core.acquireSchemaView();
                defer if (current) |*view| view.release();
                if (current != null) return error.PreparedGenerationChanged;
            }
        }
        if (self.write_plan_generation != core.index_manager.writePlanGeneration()) return error.PreparedGenerationChanged;
        if (!core.relational_indexes.isCurrent(self.relational_indexes)) return error.PreparedGenerationChanged;
        if (self.policy_lease) |*lease| try lease.checkAt(nowSeconds());
        try validateExplicitReads(core, self.generated_reads, self.transform_reads, control);
        if (self.preapply_reads) |*reads| try reads.validate(core.store, control);
        if (self.metadata_reads) |*reads| try reads.validate(core.store, control);
        if (control) |original| try original.checkpoint();
    }

    pub fn deinit(self: *Owned) void {
        if (self.preapply_reads) |*reads| reads.deinit();
        if (self.metadata_reads) |*reads| reads.deinit();
        for (self.transform_reads) |read| self.alloc.free(read.key);
        self.alloc.free(self.transform_reads);
        self.alloc.free(self.generated_reads);
        self.alloc.free(self.range_start);
        self.alloc.free(self.range_end);
        if (self.policy_lease) |*lease| lease.release();
        if (self.relational_indexes) |*view| view.deinit();
        if (self.write_plan) |*view| view.release();
        if (self.schema_view) |*view| view.release();
        self.* = undefined;
    }
};

fn nowSeconds() i64 {
    return @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s));
}

/// The caller holds its apply/publication fence. Transform values, timestamps
/// and mutation revisions must originate from the same immutable read snapshot.
pub fn validateExplicitReads(core: anytype, generated: []const GeneratedRead, transforms: anytype, original: ?admission.Port) !void {
    var trace: admission.RejectionTrace = .{};
    validateExplicitReadsInner(core, generated, transforms, trace.bind(original)) catch |err| return trace.failure orelse err;
    if (trace.failure) |cause| return cause;
}

fn validateExplicitReadsInner(core: anytype, generated: []const GeneratedRead, transforms: anytype, control: ?admission.Port) !void {
    if (control) |original| try original.checkpoint();
    if (generated.len == 0 and transforms.len == 0) return;
    {
        var txn = if (control) |original| try core.store.beginProbeTxnWithAdmission(.retain, original) else try core.store.beginProbeTxn();
        defer txn.abort();
        for (generated) |read| {
            if (control) |original| try original.checkpoint();
            if (try revisions.load(&txn, read.key) != read.revision) return error.PreparedReadSetChanged;
        }
        for (transforms) |read| if (read.revision) |revision| {
            if (control) |original| try original.checkpoint();
            if (try revisions.load(&txn, revision.key) != revision.revision) return error.PreparedReadSetChanged;
        };
    }
    var manager = try core.initTxnManager();
    defer manager.deinit();
    for (transforms) |read| {
        if (control) |original| try original.checkpoint();
        manager.checkVersionPredicatesWithAdmission(&.{.{ .key = read.key, .expected_version = read.expected_version }}, null, control) catch |err| switch (err) {
            error.VersionConflict => return error.PreparedReadSetChanged,
            else => return err,
        };
    }
    if (control) |original| try original.checkpoint();
}
