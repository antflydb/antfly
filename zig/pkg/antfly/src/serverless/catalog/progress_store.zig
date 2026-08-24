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
const catalog_types = @import("types.zig");
const head_coordination = @import("../head_coordination.zig");

pub const PublicationFence = head_coordination.Fence;

pub const EnrichmentStageProgress = struct {
    head_version: u64,
    doc_offset: u64,
};

pub const ProgressStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        get_head: *const fn (*anyopaque, []const u8) anyerror!u64,
        compare_and_swap_head: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        compare_and_swap_head_fenced: *const fn (*anyopaque, []const u8, ?u64, u64, PublicationFence) anyerror!bool,
        get_gc_watermark: *const fn (*anyopaque, []const u8) anyerror!?u64,
        compare_and_swap_gc_watermark: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        get_enrichment_head_version: *const fn (*anyopaque, []const u8) anyerror!?u64,
        compare_and_swap_enrichment_head_version: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        get_enrichment_stage: *const fn (*anyopaque, []const u8) anyerror!?u64,
        compare_and_swap_enrichment_stage: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        get_enrichment_doc_offset: *const fn (*anyopaque, []const u8) anyerror!?u64,
        compare_and_swap_enrichment_doc_offset: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        get_enrichment_stage_head_version: *const fn (*anyopaque, []const u8, u8) anyerror!?u64,
        compare_and_swap_enrichment_stage_head_version: *const fn (*anyopaque, []const u8, u8, ?u64, u64) anyerror!bool,
        get_enrichment_stage_doc_offset: *const fn (*anyopaque, []const u8, u8) anyerror!?u64,
        compare_and_swap_enrichment_stage_doc_offset: *const fn (*anyopaque, []const u8, u8, ?u64, u64) anyerror!bool,
        get_enrichment_stage_head_doc_offset: *const fn (*anyopaque, []const u8, u8, u64) anyerror!?u64,
        compare_and_swap_enrichment_stage_head_doc_offset: *const fn (*anyopaque, []const u8, u8, u64, ?u64, u64) anyerror!bool,
        delete_enrichment_stage_head_doc_offset: *const fn (*anyopaque, []const u8, u8, u64) anyerror!void,
        get_enrichment_stage_progress: *const fn (*anyopaque, []const u8, u8) anyerror!?EnrichmentStageProgress,
        compare_and_swap_enrichment_stage_progress: *const fn (*anyopaque, []const u8, u8, ?EnrichmentStageProgress, EnrichmentStageProgress) anyerror!bool,
    };

    pub fn deinit(self: *ProgressStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn getHead(self: *ProgressStore, namespace: []const u8) !u64 {
        return try self.vtable.get_head(self.ptr, namespace);
    }

    pub fn compareAndSwapHead(self: *ProgressStore, namespace: []const u8, expected: ?u64, version: u64) !bool {
        return try self.vtable.compare_and_swap_head(self.ptr, namespace, expected, version);
    }

    /// Publishes a head only while the exact durable fencing token still owns
    /// the same coordination record. Backends that cannot provide this atomic
    /// guarantee fail closed rather than degrading to a check-then-CAS race.
    pub fn compareAndSwapHeadFenced(
        self: *ProgressStore,
        namespace: []const u8,
        expected: ?u64,
        version: u64,
        fence: PublicationFence,
    ) !bool {
        return try self.vtable.compare_and_swap_head_fenced(
            self.ptr,
            namespace,
            expected,
            version,
            fence,
        );
    }

    pub fn getGcWatermark(self: *ProgressStore, namespace: []const u8) !?u64 {
        return try self.vtable.get_gc_watermark(self.ptr, namespace);
    }

    pub fn compareAndSwapGcWatermark(self: *ProgressStore, namespace: []const u8, expected: ?u64, watermark: u64) !bool {
        return try self.vtable.compare_and_swap_gc_watermark(self.ptr, namespace, expected, watermark);
    }

    pub fn getEnrichmentHeadVersion(self: *ProgressStore, namespace: []const u8) !?u64 {
        return try self.getEnrichmentStageHeadVersion(namespace, .lexical_sparse);
    }

    pub fn compareAndSwapEnrichmentHeadVersion(self: *ProgressStore, namespace: []const u8, expected: ?u64, head_version: u64) !bool {
        return try self.compareAndSwapEnrichmentStageHeadVersion(namespace, .lexical_sparse, expected, head_version);
    }

    pub fn getEnrichmentStage(self: *ProgressStore, namespace: []const u8) !?u64 {
        return try self.vtable.get_enrichment_stage(self.ptr, namespace);
    }

    pub fn compareAndSwapEnrichmentStage(self: *ProgressStore, namespace: []const u8, expected: ?u64, stage: u64) !bool {
        return try self.vtable.compare_and_swap_enrichment_stage(self.ptr, namespace, expected, stage);
    }

    pub fn getEnrichmentDocOffset(self: *ProgressStore, namespace: []const u8) !?u64 {
        return try self.getEnrichmentStageDocOffset(namespace, .lexical_sparse);
    }

    pub fn compareAndSwapEnrichmentDocOffset(self: *ProgressStore, namespace: []const u8, expected: ?u64, doc_offset: u64) !bool {
        return try self.compareAndSwapEnrichmentStageDocOffset(namespace, .lexical_sparse, expected, doc_offset);
    }

    pub fn getEnrichmentStageHeadVersion(self: *ProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage) !?u64 {
        return try self.vtable.get_enrichment_stage_head_version(self.ptr, namespace, @intFromEnum(stage));
    }

    pub fn compareAndSwapEnrichmentStageHeadVersion(
        self: *ProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?u64,
        head_version: u64,
    ) !bool {
        // Published manifest versions are monotonic. A worker that captured an
        // older HEAD must not move the visible enrichment stage backward.
        if (expected) |current| {
            if (head_version < current) return false;
        }
        return try self.vtable.compare_and_swap_enrichment_stage_head_version(self.ptr, namespace, @intFromEnum(stage), expected, head_version);
    }

    pub fn getEnrichmentStageDocOffset(self: *ProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage) !?u64 {
        if (try self.getEnrichmentStageHeadVersion(namespace, stage)) |head_version| {
            if (try self.getEnrichmentStageHeadDocOffset(namespace, stage, head_version)) |offset| return offset;
        }
        return try self.vtable.get_enrichment_stage_doc_offset(self.ptr, namespace, @intFromEnum(stage));
    }

    pub fn compareAndSwapEnrichmentStageDocOffset(
        self: *ProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        if (try self.getEnrichmentStageHeadVersion(namespace, stage)) |head_version| {
            const scoped = try self.getEnrichmentStageHeadDocOffset(namespace, stage, head_version);
            if (scoped != null) {
                return try self.compareAndSwapEnrichmentStageHeadDocOffset(
                    namespace,
                    stage,
                    head_version,
                    expected,
                    doc_offset,
                );
            }
            const legacy = try self.vtable.get_enrichment_stage_doc_offset(self.ptr, namespace, @intFromEnum(stage));
            if (legacy != expected) return false;
            return try self.compareAndSwapEnrichmentStageHeadDocOffset(
                namespace,
                stage,
                head_version,
                null,
                doc_offset,
            );
        }
        return try self.vtable.compare_and_swap_enrichment_stage_doc_offset(self.ptr, namespace, @intFromEnum(stage), expected, doc_offset);
    }

    /// Progress for a particular published head. Head-scoped offsets isolate
    /// workers that overlap a publication transition: an old worker can only
    /// advance its old key, while the new head starts from an independently
    /// initialized offset.
    pub fn getEnrichmentStageHeadDocOffset(
        self: *ProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        head_version: u64,
    ) !?u64 {
        return try self.vtable.get_enrichment_stage_head_doc_offset(
            self.ptr,
            namespace,
            @intFromEnum(stage),
            head_version,
        );
    }

    pub fn compareAndSwapEnrichmentStageHeadDocOffset(
        self: *ProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        head_version: u64,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        return try self.vtable.compare_and_swap_enrichment_stage_head_doc_offset(
            self.ptr,
            namespace,
            @intFromEnum(stage),
            head_version,
            expected,
            doc_offset,
        );
    }

    pub fn deleteEnrichmentStageHeadDocOffset(
        self: *ProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        head_version: u64,
    ) !void {
        return try self.vtable.delete_enrichment_stage_head_doc_offset(
            self.ptr,
            namespace,
            @intFromEnum(stage),
            head_version,
        );
    }

    /// A single durable record for the active head and offset of one stage.
    /// Advancing the head and resetting its offset in one CAS fences stale
    /// workers without allocating progress objects for historical heads.
    pub fn getEnrichmentStageProgress(
        self: *ProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
    ) !?EnrichmentStageProgress {
        return try self.vtable.get_enrichment_stage_progress(
            self.ptr,
            namespace,
            @intFromEnum(stage),
        );
    }

    pub fn compareAndSwapEnrichmentStageProgress(
        self: *ProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?EnrichmentStageProgress,
        desired: EnrichmentStageProgress,
    ) !bool {
        if (expected) |current| {
            if (desired.head_version < current.head_version) return false;
            if (desired.head_version == current.head_version and desired.doc_offset < current.doc_offset) return false;
        }
        return try self.vtable.compare_and_swap_enrichment_stage_progress(
            self.ptr,
            namespace,
            @intFromEnum(stage),
            expected,
            desired,
        );
    }
};
