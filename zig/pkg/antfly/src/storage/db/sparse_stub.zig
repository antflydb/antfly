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

const std = @import("std");
const Allocator = std.mem.Allocator;
const lsm_backend = @import("../lsm_backend/mod.zig");

pub const SparseVector = struct {
    indices: []const u32,
    values: []const f32,
};

pub const SparseWrite = struct {
    doc_id: []const u8,
    vec: SparseVector,
    doc_num: ?u32 = null,
};

pub const SearchResult = struct {
    doc_id: []u8,
    doc_num: ?u32 = null,
    score: f32,
};

pub const SearchConstraints = struct {
    filter_doc_ids: []const []const u8 = &.{},
    exclude_doc_ids: []const []const u8 = &.{},
    filter_doc_nums: []const u32 = &.{},
    exclude_doc_nums: []const u32 = &.{},
};

pub const SplitRebuildResult = struct {
    doc_ids: [][]u8,
    select_docs_ns: u64 = 0,
    terms_ns: u64 = 0,
    commit_ns: u64 = 0,

    pub fn deinit(self: *SplitRebuildResult, alloc: Allocator) void {
        for (self.doc_ids) |doc_id| alloc.free(doc_id);
        alloc.free(self.doc_ids);
        self.* = undefined;
    }
};

pub const SparseIndexOptions = struct {
    map_size: usize = 256 * 1024 * 1024,
    chunk_size: u32 = 1024,
    no_sync: bool = false,
    no_meta_sync: bool = false,
    backend: SparseBackend = .lsm,
    lsm_storage: ?lsm_backend.Storage = null,
    lsm_cache: ?*lsm_backend.Cache = null,
    lsm_options: lsm_backend.Options = .{ .flush_threshold = 1 },
    lsm_root_generation: u64 = 0,
};

pub const SparseBackend = enum {
    lmdb,
    mem,
    lsm_memory,
    lsm,
};

pub const BatchOptions = struct {
    defer_term_range_updates: bool = false,
};

pub const SparseIndex = struct {
    next_doc_num: u64 = 0,

    pub const Stats = struct {
        doc_count: u64 = 0,
        term_count: u64 = 0,
    };

    pub fn open(_: Allocator, _: [*:0]const u8, _: SparseIndexOptions) !SparseIndex {
        return error.UnsupportedPlatform;
    }

    pub fn close(_: *SparseIndex) void {}

    pub fn sync(_: *SparseIndex, _: bool) !void {}

    pub fn syncReplayState(_: *SparseIndex) !void {
        return error.UnsupportedPlatform;
    }

    pub fn stats(_: *SparseIndex) Stats {
        return .{};
    }

    pub fn batch(_: *SparseIndex, _: []const SparseWrite, _: []const []const u8) !void {
        return error.UnsupportedPlatform;
    }

    pub fn batchWithOptions(_: *SparseIndex, _: []const SparseWrite, _: []const []const u8, _: BatchOptions) !void {
        return error.UnsupportedPlatform;
    }

    pub fn search(_: *SparseIndex, _: Allocator, _: *const SparseVector, _: u32) ![]SearchResult {
        return error.UnsupportedPlatform;
    }

    pub fn searchConstrained(_: *SparseIndex, _: Allocator, _: *const SparseVector, _: u32, _: SearchConstraints) ![]SearchResult {
        return error.UnsupportedPlatform;
    }

    pub fn debugDocNumForDocId(_: *SparseIndex, _: []const u8) !?u32 {
        return error.UnsupportedPlatform;
    }

    pub fn freeResults(alloc: Allocator, results: []SearchResult) void {
        for (results) |hit| alloc.free(hit.doc_id);
        alloc.free(results);
    }

    pub fn handoffRangeInto(
        _: *SparseIndex,
        _: *SparseIndex,
        _: Allocator,
        _: []const u8,
        _: []const u8,
        _: bool,
    ) !SplitRebuildResult {
        return error.UnsupportedPlatform;
    }

    pub fn handoffPreparedDocIdsInto(
        _: *SparseIndex,
        _: *SparseIndex,
        _: Allocator,
        _: []const []const u8,
        _: []const u8,
        _: []const u8,
        _: bool,
    ) !SplitRebuildResult {
        return error.UnsupportedPlatform;
    }
};
