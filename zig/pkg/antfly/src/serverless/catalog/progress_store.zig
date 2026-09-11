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
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const work_lease = @import("../build/work_lease.zig");

pub const PublicationFence = head_coordination.Fence;

pub const EnrichmentStageProgress = struct {
    pub const max_encoded_bytes = 92 + 2 * @import("../build/document_facts.zig").max_pending_key_bytes;
    head_version: u64,
    doc_offset: u64,
    revision: u64 = 0,
    pipeline_version: u32 = 0,
    policy_fingerprint: [32]u8 = @splat(0),
    after_order_key: ?[]const u8 = null,
    /// Inclusive, immutable key boundary of the current scan cycle.
    cycle_upper_order_key: ?[]const u8 = null,
    completed_cycles: u64 = 0,
    failed_documents: u64 = 0,

    pub fn deinit(self: *EnrichmentStageProgress, alloc: Allocator) void {
        if (self.after_order_key) |key| alloc.free(key);
        if (self.cycle_upper_order_key) |key| alloc.free(key);
        self.* = undefined;
    }

    pub fn eql(lhs: EnrichmentStageProgress, rhs: EnrichmentStageProgress) bool {
        if (lhs.head_version != rhs.head_version or lhs.doc_offset != rhs.doc_offset or lhs.revision != rhs.revision or
            lhs.pipeline_version != rhs.pipeline_version or lhs.completed_cycles != rhs.completed_cycles or lhs.failed_documents != rhs.failed_documents) return false;
        if (!std.mem.eql(u8, &lhs.policy_fingerprint, &rhs.policy_fingerprint)) return false;
        return optionalKeyEql(lhs.after_order_key, rhs.after_order_key) and optionalKeyEql(lhs.cycle_upper_order_key, rhs.cycle_upper_order_key);
    }

    fn optionalKeyEql(lhs: ?[]const u8, rhs: ?[]const u8) bool {
        if (lhs == null or rhs == null) return lhs == null and rhs == null;
        return std.mem.eql(u8, lhs.?, rhs.?);
    }

    pub fn encodeAlloc(self: EnrichmentStageProgress, alloc: Allocator) ![]u8 {
        const key = self.after_order_key orelse "";
        const upper = self.cycle_upper_order_key orelse "";
        if (self.after_order_key != null and (key.len <= 8 or key.len > @import("../build/document_facts.zig").max_pending_key_bytes)) return error.InvalidEnrichmentStageProgress;
        if (self.cycle_upper_order_key != null and (upper.len <= 8 or upper.len > @import("../build/document_facts.zig").max_pending_key_bytes)) return error.InvalidEnrichmentStageProgress;
        if (self.after_order_key != null and (self.cycle_upper_order_key == null or std.mem.order(u8, key, upper) == .gt)) return error.InvalidEnrichmentStageProgress;
        const key_len: u32 = if (self.after_order_key == null) std.math.maxInt(u32) else @intCast(key.len);
        const upper_len: u32 = if (self.cycle_upper_order_key == null) std.math.maxInt(u32) else @intCast(upper.len);
        const out = try alloc.alloc(u8, 92 + key.len + upper.len);
        @memcpy(out[0..8], "AFESCAN3");
        std.mem.writeInt(u64, out[8..16], self.head_version, .little);
        std.mem.writeInt(u64, out[16..24], self.doc_offset, .little);
        std.mem.writeInt(u64, out[24..32], self.revision, .little);
        std.mem.writeInt(u64, out[32..40], self.completed_cycles, .little);
        std.mem.writeInt(u64, out[40..48], self.failed_documents, .little);
        std.mem.writeInt(u32, out[48..52], self.pipeline_version, .little);
        std.mem.writeInt(u32, out[52..56], key_len, .little);
        std.mem.writeInt(u32, out[56..60], upper_len, .little);
        @memcpy(out[60..92], &self.policy_fingerprint);
        @memcpy(out[92..][0..key.len], key);
        @memcpy(out[92 + key.len ..], upper);
        return out;
    }

    pub fn decodeAlloc(alloc: Allocator, raw: []const u8) !EnrichmentStageProgress {
        if (raw.len < 92 or !std.mem.eql(u8, raw[0..8], "AFESCAN3")) return error.InvalidEnrichmentStageProgress;
        const key_len = std.mem.readInt(u32, raw[52..56], .little);
        const upper_len = std.mem.readInt(u32, raw[56..60], .little);
        if (key_len != std.math.maxInt(u32) and (key_len <= 8 or key_len > @import("../build/document_facts.zig").max_pending_key_bytes)) return error.InvalidEnrichmentStageProgress;
        if (upper_len != std.math.maxInt(u32) and (upper_len <= 8 or upper_len > @import("../build/document_facts.zig").max_pending_key_bytes)) return error.InvalidEnrichmentStageProgress;
        const key_bytes: usize = if (key_len == std.math.maxInt(u32)) 0 else key_len;
        const upper_bytes: usize = if (upper_len == std.math.maxInt(u32)) 0 else upper_len;
        if (raw.len - 92 != key_bytes + upper_bytes) return error.InvalidEnrichmentStageProgress;
        if (key_bytes != 0 and (upper_bytes == 0 or std.mem.order(u8, raw[92..][0..key_bytes], raw[92 + key_bytes ..]) == .gt)) return error.InvalidEnrichmentStageProgress;
        const key = if (key_bytes == 0) null else try alloc.dupe(u8, raw[92..][0..key_bytes]);
        errdefer if (key) |owned| alloc.free(owned);
        const upper = if (upper_bytes == 0) null else try alloc.dupe(u8, raw[92 + key_bytes ..]);
        return .{
            .head_version = std.mem.readInt(u64, raw[8..16], .little),
            .doc_offset = std.mem.readInt(u64, raw[16..24], .little),
            .revision = std.mem.readInt(u64, raw[24..32], .little),
            .completed_cycles = std.mem.readInt(u64, raw[32..40], .little),
            .failed_documents = std.mem.readInt(u64, raw[40..48], .little),
            .pipeline_version = std.mem.readInt(u32, raw[48..52], .little),
            .policy_fingerprint = raw[60..92].*,
            .after_order_key = key,
            .cycle_upper_order_key = upper,
        };
    }
};

test "serverless enrichment stage cursor codec owns binary keys and bounds malformed lengths" {
    const a = std.testing.allocator;
    const value: EnrichmentStageProgress = .{ .head_version = 9, .doc_offset = 7, .revision = 12, .pipeline_version = 3, .policy_fingerprint = @splat(17), .completed_cycles = 4, .failed_documents = 5, .after_order_key = "00000001a\x00\xffb", .cycle_upper_order_key = "00000001z\x00\xff" };
    const encoded = try value.encodeAlloc(a);
    defer a.free(encoded);
    var decoded = try EnrichmentStageProgress.decodeAlloc(a, encoded);
    defer decoded.deinit(a);
    try std.testing.expect(value.eql(decoded));
    try std.testing.expect(decoded.after_order_key.?.ptr != value.after_order_key.?.ptr);
    try std.testing.expect(decoded.cycle_upper_order_key.?.ptr != value.cycle_upper_order_key.?.ptr);
    var none = value;
    none.after_order_key = null;
    none.cycle_upper_order_key = null;
    const empty = try none.encodeAlloc(a);
    defer a.free(empty);
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var decoded_none = try EnrichmentStageProgress.decodeAlloc(denied.allocator(), empty);
    defer decoded_none.deinit(a);
    try std.testing.expect(none.eql(decoded_none));
    const max_key = @import("../build/document_facts.zig").max_pending_key_bytes;
    const large = try a.alloc(u8, max_key + 1);
    defer a.free(large);
    @memset(large, 'k');
    var invalid = value;
    invalid.after_order_key = "";
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, invalid.encodeAlloc(denied.allocator()));
    invalid.after_order_key = large;
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, invalid.encodeAlloc(denied.allocator()));
    invalid.after_order_key = large[0..max_key];
    invalid.cycle_upper_order_key = large[0..max_key];
    const largest = try invalid.encodeAlloc(a);
    defer a.free(largest);
    var largest_decoded = try EnrichmentStageProgress.decodeAlloc(a, largest);
    defer largest_decoded.deinit(a);
    try std.testing.expect(invalid.eql(largest_decoded));
    invalid = value;
    invalid.cycle_upper_order_key = null;
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, invalid.encodeAlloc(denied.allocator()));
    invalid.cycle_upper_order_key = "a";
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, invalid.encodeAlloc(denied.allocator()));
    invalid.cycle_upper_order_key = "";
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, invalid.encodeAlloc(denied.allocator()));
    invalid.cycle_upper_order_key = large;
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, invalid.encodeAlloc(denied.allocator()));
    for ([_]u32{ 0, 1, max_key + 1 }) |length| {
        std.mem.writeInt(u32, empty[52..56], length, .little);
        try std.testing.expectError(error.InvalidEnrichmentStageProgress, EnrichmentStageProgress.decodeAlloc(denied.allocator(), empty));
    }
    std.mem.writeInt(u32, empty[52..56], std.math.maxInt(u32), .little);
    for ([_]u32{ 0, 1, max_key + 1 }) |length| {
        std.mem.writeInt(u32, empty[56..60], length, .little);
        try std.testing.expectError(error.InvalidEnrichmentStageProgress, EnrichmentStageProgress.decodeAlloc(denied.allocator(), empty));
    }
    const malformed = try a.dupe(u8, encoded);
    defer a.free(malformed);
    malformed[92 + value.after_order_key.?.len + 8] = '0';
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, EnrichmentStageProgress.decodeAlloc(denied.allocator(), malformed));
    std.mem.writeInt(u32, malformed[56..60], std.math.maxInt(u32), .little);
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, EnrichmentStageProgress.decodeAlloc(denied.allocator(), malformed[0 .. 92 + value.after_order_key.?.len]));
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, EnrichmentStageProgress.decodeAlloc(a, encoded[0..55]));
    encoded[7] = '1';
    try std.testing.expectError(error.InvalidEnrichmentStageProgress, EnrichmentStageProgress.decodeAlloc(a, encoded));
}

test "serverless enrichment stage cursor codec releases allocations at every failure point" {
    const Exercise = struct {
        fn run(alloc: Allocator) !void {
            const value: EnrichmentStageProgress = .{ .head_version = 2, .doc_offset = 3, .revision = 4, .pipeline_version = 1, .after_order_key = "00000001binary\x00key", .cycle_upper_order_key = "00000001z\x00bound" };
            const encoded = try value.encodeAlloc(alloc);
            defer alloc.free(encoded);
            var decoded = try EnrichmentStageProgress.decodeAlloc(alloc, encoded);
            defer decoded.deinit(alloc);
            try std.testing.expect(value.eql(decoded));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}

pub const ProgressStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        work_lease_provider: ?*const fn (*anyopaque) work_lease.Provider = null,
        deinit: *const fn (Allocator, *anyopaque) void,
        get_head: *const fn (*anyopaque, []const u8) anyerror!u64,
        compare_and_swap_head: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        compare_and_swap_head_fenced: *const fn (*anyopaque, []const u8, ?u64, u64, PublicationFence) anyerror!bool,
        get_gc_watermark: *const fn (*anyopaque, []const u8) anyerror!?u64,
        compare_and_swap_gc_watermark: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        get_manifest_gc_floor: *const fn (*anyopaque, []const u8) anyerror!?u64,
        compare_and_swap_manifest_gc_floor: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        get_manifest_read_deadline: ?*const fn (*anyopaque, []const u8, u64) anyerror!?u64 = null,
        compare_and_swap_manifest_read_deadline: ?*const fn (*anyopaque, []const u8, u64, ?u64, u64) anyerror!bool = null,
        prune_manifest_read_deadlines: ?*const fn (*anyopaque, []const u8, u64, u64, CancellationToken) anyerror!void = null,
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

    /// The provider and HEAD CAS must share the same atomic coordination record.
    pub fn workLeaseProvider(self: *ProgressStore) !work_lease.Provider {
        return (self.vtable.work_lease_provider orelse return error.WorkLeaseUnsupported)(self.ptr);
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

    /// Versions strictly below this durable, monotonic boundary are retired.
    /// Record it before removing content so interrupted GC cannot resurrect
    /// partially deleted history when the retention policy increases.
    pub fn getManifestGcFloor(self: *ProgressStore, namespace: []const u8) !?u64 {
        return try self.vtable.get_manifest_gc_floor(self.ptr, namespace);
    }

    pub fn compareAndSwapManifestGcFloor(self: *ProgressStore, namespace: []const u8, expected: ?u64, floor: u64) !bool {
        return try self.vtable.compare_and_swap_manifest_gc_floor(self.ptr, namespace, expected, floor);
    }

    /// Shared, monotonic wall-clock deadline for readers of an immutable
    /// version. Publish before checking MANIFEST_GC_FLOOR; collectors advance
    /// that floor before reading deadlines. An unsupported backend fails closed.
    pub fn getManifestReadDeadline(self: *ProgressStore, namespace: []const u8, version: u64) !?u64 {
        const get = self.vtable.get_manifest_read_deadline orelse return error.ManifestReadLeasesUnsupported;
        return get(self.ptr, namespace, version);
    }

    pub fn compareAndSwapManifestReadDeadline(self: *ProgressStore, namespace: []const u8, version: u64, expected: ?u64, deadline: u64) !bool {
        if (expected) |prior| if (deadline < prior) return false;
        const cas = self.vtable.compare_and_swap_manifest_read_deadline orelse return error.ManifestReadLeasesUnsupported;
        return cas(self.ptr, namespace, version, expected, deadline);
    }

    /// Includes pins left by failed acquisitions after their manifest vanished.
    /// The committed floor prevents cleanup from removing renewable authority.
    pub fn pruneManifestReadDeadlines(self: *ProgressStore, namespace: []const u8, floor: u64, expired_before: u64, cancellation: CancellationToken) !void {
        const prune = self.vtable.prune_manifest_read_deadlines orelse return;
        if (floor > (try self.getManifestGcFloor(namespace) orelse 0)) return error.ManifestGcFloorNotCommitted;
        return prune(self.ptr, namespace, floor, expired_before, cancellation);
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

    /// A single durable, revision-fenced key cursor for one stage. The key
    /// survives HEAD changes while the offset is only a current-head diagnostic.
    /// Both returned keys are owned: release with value.deinit(self.allocator).
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

    /// Both cursor values are borrowed for the duration of this call.
    pub fn compareAndSwapEnrichmentStageProgress(
        self: *ProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?EnrichmentStageProgress,
        desired: EnrichmentStageProgress,
    ) !bool {
        if (expected) |current| {
            if (desired.head_version < current.head_version) return false;
            if (desired.revision < current.revision or desired.completed_cycles < current.completed_cycles) return false;
            if (desired.head_version == current.head_version and desired.revision == current.revision and desired.doc_offset < current.doc_offset) return false;
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

/// Exercise the erased capability too: its validation must allow the same
/// revision-fenced wrap that the durable backend accepts.
pub fn testEnrichmentCursorCompareAndSwap(store: *ProgressStore) !void {
    const first: EnrichmentStageProgress = .{ .head_version = 1, .doc_offset = 1, .revision = 1, .pipeline_version = 1, .policy_fingerprint = @splat(3), .after_order_key = "00000001a\x00", .cycle_upper_order_key = "00000001z\x00" };
    try std.testing.expect(try store.compareAndSwapEnrichmentStageProgress("cursor", .lexical_sparse, null, first));
    const second: EnrichmentStageProgress = .{ .head_version = 2, .doc_offset = 2, .revision = 2, .pipeline_version = 1, .policy_fingerprint = @splat(3), .after_order_key = "00000001b\x00", .cycle_upper_order_key = "00000001z\x00" };
    try std.testing.expect(try store.compareAndSwapEnrichmentStageProgress("cursor", .lexical_sparse, first, second));
    var reread = (try store.getEnrichmentStageProgress("cursor", .lexical_sparse)).?;
    defer reread.deinit(store.allocator);
    try std.testing.expect(second.eql(reread));
    try std.testing.expect(reread.after_order_key.?.ptr != second.after_order_key.?.ptr);
    try std.testing.expect(reread.cycle_upper_order_key.?.ptr != second.cycle_upper_order_key.?.ptr);
    var wrong_boundary = second;
    wrong_boundary.cycle_upper_order_key = "00000001y\x00";
    const wrapped: EnrichmentStageProgress = .{ .head_version = 2, .doc_offset = 0, .revision = 3, .pipeline_version = 1, .completed_cycles = 1 };
    try std.testing.expect(!try store.compareAndSwapEnrichmentStageProgress("cursor", .lexical_sparse, wrong_boundary, wrapped));
    wrong_boundary = second;
    wrong_boundary.policy_fingerprint[0] ^= 1;
    try std.testing.expect(!try store.compareAndSwapEnrichmentStageProgress("cursor", .lexical_sparse, wrong_boundary, wrapped));
    try std.testing.expect(!try store.compareAndSwapEnrichmentStageProgress("cursor", .lexical_sparse, first, wrapped));
    try std.testing.expect(try store.compareAndSwapEnrichmentStageProgress("cursor", .lexical_sparse, reread, wrapped));
    try std.testing.expect(!try store.compareAndSwapEnrichmentStageProgress("cursor", .lexical_sparse, second, wrapped));
    var final = (try store.getEnrichmentStageProgress("cursor", .lexical_sparse)).?;
    defer final.deinit(store.allocator);
    try std.testing.expect(wrapped.eql(final));
}
