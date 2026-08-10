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

    pub const VTable = struct {
        get: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8,
        scan_prefix: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: ScanOptions, ctx: *anyopaque, consume: Consume) anyerror!void = null,
        nearest: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, query: NearestQuery, ctx: *anyopaque, consume: Consume) anyerror!void = null,
    };

    pub fn get(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8 {
        return self.vtable.get(self.ptr, allocator, table, key);
    }

    pub fn scanPrefix(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: ScanOptions, ctx: *anyopaque, consume: Consume) anyerror!void {
        const callback = self.vtable.scan_prefix orelse return error.ScanUnsupported;
        return callback(self.ptr, allocator, table, prefix, opts, ctx, consume);
    }

    pub fn nearest(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, query: NearestQuery, ctx: *anyopaque, consume: Consume) anyerror!void {
        const callback = self.vtable.nearest orelse return error.NearestUnsupported;
        return callback(self.ptr, allocator, table, query, ctx, consume);
    }
};

pub const EntityUpsert = struct {
    table: []const u8,
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
        upsert: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void,
        upsert_batch: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void = null,
    };

    pub fn upsert(self: EntitySink, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void {
        return self.vtable.upsert(self.ptr, allocator, table, key, doc_json);
    }

    pub fn upsertBatch(self: EntitySink, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void {
        if (self.vtable.upsert_batch) |callback| return callback(self.ptr, allocator, entries);
        for (entries) |entry| try self.upsert(allocator, entry.table, entry.key, entry.doc_json);
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
