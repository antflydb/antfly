// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Callback contracts injected by distributed control into storage-owned
//! resolution and promotion workers. Implementations live on either side of
//! the compiled storage boundary; this module must remain implementation-free.

const std = @import("std");

pub const CandidateSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const Consume = *const fn (ctx: *anyopaque, entity_key: []const u8, value: []const u8) anyerror!void;
    pub const NearestQuery = struct {
        index_name: []const u8,
        embedding: []const f32,
        k: usize,
    };
    pub const ScanOptions = struct {
        limit: usize = 0,
    };

    pub const Batch = struct {
        source: CandidateSource,
        release: ?*const fn (*anyopaque, std.mem.Allocator) void = null,
        pub fn deinit(self: Batch, alloc: std.mem.Allocator) void {
            if (self.release) |f| f(self.source.ptr, alloc);
        }
    };
    pub fn beginBatch(self: CandidateSource, alloc: std.mem.Allocator, tables: []const []const u8) !Batch {
        if (self.vtable.begin_batch) |f| return f(self.ptr, alloc, tables);
        return .{ .source = self };
    }
    pub fn boundTable(self: CandidateSource, table: []const u8) !?[]const u8 {
        if (self.vtable.bound_table) |f| return f(self.ptr, table);
        return null;
    }
    pub const VTable = struct {
        /// Bulk point candidates. Missing keys are omitted. Values are borrowed
        /// during consume; this optional capability must preserve point-read semantics.
        get_many: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const []const u8, *anyopaque, Consume) anyerror!void = null,
        /// A work-unit-owned binding; never shared between background workers.
        begin_batch: ?*const fn (*anyopaque, std.mem.Allocator, []const []const u8) anyerror!Batch = null,
        /// Borrowed immutable destination, retained through batch deinit.
        bound_table: ?*const fn (*anyopaque, []const u8) anyerror!?[]const u8 = null,
        /// Fetch the entity doc for `key` in `table` (owned bytes or null).
        get: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8,
        /// Scan `table` for entities whose key starts with `prefix`.
        scan_prefix: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: ScanOptions, ctx: *anyopaque, consume: Consume) anyerror!void = null,
        /// The nearest entities in `table` for a named dense index.
        nearest: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, query: NearestQuery, ctx: *anyopaque, consume: Consume) anyerror!void = null,
    };

    pub fn get(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8 {
        return self.vtable.get(self.ptr, allocator, table, key);
    }
    pub fn scanPrefix(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: ScanOptions, ctx: *anyopaque, consume: Consume) anyerror!void {
        const f = self.vtable.scan_prefix orelse return error.ScanUnsupported;
        return f(self.ptr, allocator, table, prefix, opts, ctx, consume);
    }
    pub fn nearest(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, query: NearestQuery, ctx: *anyopaque, consume: Consume) anyerror!void {
        const f = self.vtable.nearest orelse return error.NearestUnsupported;
        return f(self.ptr, allocator, table, query, ctx, consume);
    }
};

pub const EntityUpsert = struct {
    table: []const u8,
    storage_table: ?[]const u8 = null,
    key: []const u8,
    doc_json: []const u8,
};

pub const MissingSinkPolicy = enum {
    wait,
    disabled,
};

pub const EntitySink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Upsert the entity at `key` in `table` with the canonical document
        /// `doc_json`. Must be idempotent under replay.
        upsert: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void,
        /// Upsert all entities resolved from one document atomically (a single
        /// multi-participant transaction), so a document never lands a partial
        /// set of its entities. Optional: when absent, `upsertBatch` falls back
        /// to per-entity upserts.
        upsert_batch: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void = null,
    };

    pub fn upsert(self: EntitySink, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void {
        return self.vtable.upsert(self.ptr, allocator, table, key, doc_json);
    }

    pub fn upsertBatch(self: EntitySink, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void {
        if (self.vtable.upsert_batch) |f| return f(self.ptr, allocator, entries);
        for (entries) |e| {
            if (e.storage_table != null) return error.EntityPromotionAtomicCommitUnavailable;
        }
        for (entries) |e| {
            try self.upsert(allocator, e.table, e.key, e.doc_json);
        }
    }
};

pub const PromotionOwner = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        is_local_owner: *const fn (ptr: *anyopaque) bool,
    };

    pub fn isLocalOwner(self: PromotionOwner) bool {
        return self.vtable.is_local_owner(self.ptr);
    }
};

pub const DenseNativeMigrationPolicySource = struct {
    ptr: *const anyopaque,
    authority_permitted: *const fn (ptr: *const anyopaque) bool,

    pub fn authorityPermitted(self: @This()) bool {
        return self.authority_permitted(self.ptr);
    }
};
