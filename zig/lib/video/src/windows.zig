// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Immutable request-local window/frame reuse; no URL-keyed persistent cache.
const std = @import("std");
const media = @import("antfly_media");
const sampling = @import("sampling.zig");
const decode_plan = @import("decode_plan.zig");
pub const Window = struct { interval: sampling.Interval, step: u64 };
pub const Limits = struct {
    max_windows: usize = 32,
    max_frames_per_window: usize = 32,
    max_unique_frames: usize = 64,
    max_total_selections: usize = 1024,
};
pub const Plan = struct {
    allocator: std.mem.Allocator,
    stamp: decode_plan.Stamp,
    windows: []Window,
    unique_indexes: []usize,
    unique_pts: []i64,
    offsets: []usize,
    references: []usize,
    pub fn window(self: *const Plan, index: usize) ![]const usize {
        if (index >= self.windows.len) return error.InvalidWindowIndex;
        return self.references[self.offsets[index]..self.offsets[index + 1]];
    }
    pub fn deinit(self: *Plan) void {
        self.allocator.free(self.windows);
        self.allocator.free(self.unique_indexes);
        self.allocator.free(self.unique_pts);
        self.allocator.free(self.offsets);
        self.allocator.free(self.references);
        self.* = undefined;
    }
};
/// Native timestamp policy, not a claim of upstream model FPS/geometry parity.
/// Every window preserves its own presentation order while sharing picture IDs.
pub fn create(allocator: std.mem.Allocator, reader: *const media.mp4.Reader, requested: []const Window, limits: Limits) !Plan {
    try reader.input.control.check();
    if (requested.len == 0 or requested.len > limits.max_windows or limits.max_unique_frames == 0) return error.ResourceLimitExceeded;
    const owned_windows = try allocator.dupe(Window, requested);
    errdefer allocator.free(owned_windows);
    const offsets = try allocator.alloc(usize, requested.len + 1);
    errdefer allocator.free(offsets);
    var unique: std.ArrayList(usize) = .empty;
    errdefer unique.deinit(allocator);
    var pts: std.ArrayList(i64) = .empty;
    errdefer pts.deinit(allocator);
    var references: std.ArrayList(usize) = .empty;
    errdefer references.deinit(allocator);
    for (requested, 0..) |clip, i| {
        try reader.input.control.check();
        offsets[i] = references.items.len;
        const selected = try sampling.timestamps(allocator, reader.packets, clip.interval, clip.step, limits.max_frames_per_window, reader.input.control);
        defer allocator.free(selected);
        if (selected.len == 0) return error.EmptyVideoWindow;
        if (selected.len > limits.max_total_selections -| references.items.len) return error.ResourceLimitExceeded;
        for (selected) |index| {
            var slot: ?usize = null;
            for (unique.items, 0..) |existing, j| if (existing == index) {
                slot = j;
                break;
            };
            if (slot == null) {
                if (unique.items.len >= limits.max_unique_frames) return error.ResourceLimitExceeded;
                slot = unique.items.len;
                try unique.append(allocator, index);
                try pts.append(allocator, reader.packets[index].pts);
            }
            try references.append(allocator, slot.?);
        }
    }
    offsets[requested.len] = references.items.len;
    const owned_unique = try unique.toOwnedSlice(allocator);
    errdefer allocator.free(owned_unique);
    const owned_pts = try pts.toOwnedSlice(allocator);
    errdefer allocator.free(owned_pts);
    return .{ .allocator = allocator, .stamp = decode_plan.Stamp.init(reader), .windows = owned_windows, .unique_indexes = owned_unique, .unique_pts = owned_pts, .offsets = offsets, .references = try references.toOwnedSlice(allocator) };
}
