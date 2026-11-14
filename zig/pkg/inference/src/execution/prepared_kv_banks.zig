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

//! Cold, owning KV bank capability. Numerical execution retains its existing
//! backend implementation; no tensor vtable or hot backend field is added.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const capacity = @import("../runtime/kv/device_capacity.zig");
const Epoch = @import("retirement_sink.zig").Epoch;
const Target = @import("../backends/cold_compute_target.zig").Target;
const DType = @import("../runtime/kv/pool.zig").KvDType;

pub const Mode = enum { shared_physical_pages, sequence_private, ring, fitted_match };
pub const EncodedLayer = struct {
    geometry: capacity.LayerGeometry,
    key_row_bytes: usize,
    base_key_row_bytes: usize,
    value_row_bytes: usize,
    /// Full backend tuple, never a hash. CUDA: [compressed key format with bit63
    /// indicating presence, value format, private slot base-alignment (bits1+) + replay boolean (bit0), forced replay
    /// token reserve (0 means none)]. The latter two words are zero for shared
    /// paged formats. Private slot strides apply the original monotone uncompressed
    /// replay-capacity rule to Window.sequence_tokens. Metal: provider's exact key
    /// encoding, value encoding, key ancillary flags, value ancillary flags.
    /// Providers canonically zero unused words; row strides remain explicit.
    codec_words: [4]u64 = @splat(0),
};
pub const Coverage = struct {
    target: Target,
    window: capacity.Window,
    dtype: DType,
    mode: Mode,
    /// Borrowed immutable rows. Actual leaf/context or pre-join factory owner
    /// pins these through the full comparison; no mutable storage is consulted.
    rows: []const EncodedLayer,
    pub fn covers(self: Coverage, request: Coverage) bool {
        if (!std.meta.eql(self.target, request.target) or self.dtype != request.dtype or self.mode != request.mode or !self.window.covers(request.window) or self.rows.len != request.rows.len) return false;
        for (self.rows, request.rows) |a, b| if (!std.meta.eql(a, b)) return false;
        return true;
    }
};
/// Self-contained pre-join metadata. Call coverage only AFTER this value reaches
/// its final Factory/Context placement; no slice points into a returned stack copy.
pub const Descriptor = struct {
    target: Target,
    window: capacity.Window,
    dtype: DType,
    mode: Mode,
    inline_rows: [256]EncodedLayer = undefined,
    row_count: usize = 0,
    pub fn append(self: *Descriptor, row: EncodedLayer) !void {
        if (self.row_count == self.inline_rows.len) return error.InvalidDeviceCapacityLayout;
        // Geometry.append is the canonical sparse-ID/shape/duplicate validator.
        var geometry = capacity.Geometry{};
        // Every earlier row was already validated; append only needs the last
        // sparse ID plus the new width/order. Keep per-page description O(n).
        if (self.row_count != 0) {
            geometry.layers[0] = self.inline_rows[self.row_count - 1].geometry;
            geometry.count = 1;
        }
        try geometry.append(row.geometry);
        if (row.key_row_bytes == 0 or row.value_row_bytes == 0 or row.base_key_row_bytes > row.key_row_bytes)
            return error.InvalidDeviceCapacityLayout;
        self.inline_rows[self.row_count] = row;
        self.row_count += 1;
    }
    pub fn coverage(self: *const Descriptor) Coverage {
        return .{ .target = self.target, .window = self.window, .dtype = self.dtype, .mode = self.mode, .rows = self.inline_rows[0..self.row_count] };
    }
};
pub const Owned = struct {
    ptr: ?*anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        snapshot: *const fn (*const anyopaque) Coverage,
        adopt: *const fn (*anyopaque, *const ops.ComputeBackend, *@import("../runtime/kv/storage_runtime.zig").KvStorageRuntime) anyerror!void,
        abort: *const fn (*anyopaque, *const Epoch) void,
        finish: *const fn (*anyopaque, *const Epoch) void,
    };
    pub fn fromLeaf(comptime Leaf: type, value: *Leaf) Owned {
        const Adapter = struct {
            fn snapshotOpaque(raw: *const anyopaque) Coverage {
                const leaf: *const Leaf = @ptrCast(@alignCast(raw));
                return leaf.snapshotCoverage();
            }
            fn adoptOpaque(raw: *anyopaque, backend: *const ops.ComputeBackend, runtime: *@import("../runtime/kv/storage_runtime.zig").KvStorageRuntime) !void {
                const leaf: *Leaf = @ptrCast(@alignCast(raw));
                return leaf.adopt(backend, runtime);
            }
            fn abortOpaque(raw: *anyopaque, epoch: *const Epoch) void {
                const leaf: *Leaf = @ptrCast(@alignCast(raw));
                leaf.abortOutsideLanes(epoch);
            }
            fn finishOpaque(raw: *anyopaque, epoch: *const Epoch) void {
                const leaf: *Leaf = @ptrCast(@alignCast(raw));
                leaf.finishAfterUnlock(epoch);
            }
            const table = VTable{ .snapshot = snapshotOpaque, .adopt = adoptOpaque, .abort = abortOpaque, .finish = finishOpaque };
        };
        return .{ .ptr = value, .vtable = &Adapter.table };
    }
    /// Capture outside Poolmutex. Store this immutable value alongside the
    /// actual context/Factory pin; Pool eligibility calls Coverage.covers only.
    pub fn snapshotCoverage(self: Owned) !Coverage {
        return self.vtable.snapshot(self.ptr orelse return error.PreparedKvBankConsumed);
    }
    pub fn adopt(self: *Owned, backend: *const ops.ComputeBackend, runtime: *@import("../runtime/kv/storage_runtime.zig").KvStorageRuntime) !void {
        return self.vtable.adopt(self.ptr orelse return error.PreparedKvBankConsumed, backend, runtime);
    }
    pub fn abortOutsideLanes(self: *Owned, epoch: *const Epoch) void {
        const raw = self.ptr orelse return;
        self.ptr = null;
        self.vtable.abort(raw, epoch);
    }
    pub fn finishAfterUnlock(self: *Owned, epoch: *const Epoch) void {
        const raw = self.ptr orelse return;
        self.ptr = null;
        self.vtable.finish(raw, epoch);
    }
};

test "cold KV descriptor owns sparse rows after value placement and matches full immutable tuple" {
    const target = Target{ .cuda = .{ .implementation_context = @ptrFromInt(16), .implementation_table = @ptrFromInt(32), .primary_context = @ptrFromInt(48), .device = 0 } };
    const window = try capacity.Window.fromRequest(3, 5, 2, 4, 1);
    var draft = Descriptor{ .target = target, .window = window, .dtype = .f16, .mode = .shared_physical_pages };
    const row = EncodedLayer{ .geometry = .{ .index = 1, .num_kv_heads = 2, .head_dim = 64 }, .key_row_bytes = 256, .base_key_row_bytes = 256, .value_row_bytes = 256, .codec_words = .{ 1, 2, 0, 0 } };
    try draft.append(row);
    var placed = draft;
    // The caller takes a slice only from the final owner, after this copy.
    const available = placed.coverage();
    draft.inline_rows[0].key_row_bytes = 99;
    try std.testing.expectEqual(@as(usize, 256), available.rows[0].key_row_bytes);
    try std.testing.expect(available.covers(available));
    var requested = placed;
    requested.inline_rows[0].codec_words[3] = 1;
    try std.testing.expect(!available.covers(requested.coverage()));
    requested = placed;
    requested.target.cuda.primary_context = @ptrFromInt(64);
    try std.testing.expect(!available.covers(requested.coverage()));
    requested = placed;
    requested.mode = .sequence_private;
    try std.testing.expect(!available.covers(requested.coverage()));
    requested = placed;
    requested.window = try capacity.Window.fromRequest(3, 9, 2, 4, 1);
    try std.testing.expect(!available.covers(requested.coverage()));
    try std.testing.expectError(error.InvalidDeviceCapacityLayout, placed.append(row));
    try std.testing.expectEqual(@as(usize, 1), placed.row_count);
}
