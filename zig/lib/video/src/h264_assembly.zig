// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Owned bounded reconstruction workspaces. Packet boundaries do not delimit
//! pictures, and prediction snapshots do not share mutable DPB marking state.
const std = @import("std");
const entropy = @import("h264_entropy.zig");
const motion = @import("h264_motion.zig");
const references = @import("h264_references.zig");
pub fn Workspace(comptime Sample: type, comptime Header: type) type {
    return struct {
        const Self = @This();
        pub const State = references.StateFor(Sample);
        planar: []Sample,
        counts: []u8,
        modes: []u8,
        qps: []u8,
        metadata: []entropy.Meta,
        motions: [2][]motion.Motion,
        groups: []u8,
        selected: []bool,
        headers: [2]?Header = .{ null, null },
        committed: [2]bool = .{ false, false },
        snapshot: ?*State = null,
        active: bool = false,
        complete: bool = false,
        id: u32 = 0,
        slices: usize = 0,
        pts: ?i64 = null,
        end: i64 = 0,
        pub fn init(allocator: std.mem.Allocator, pixels: usize, sub: usize, groups: usize, selections: usize) !Self {
            const chroma_pixels = if (sub == 0) @as(usize, 0) else pixels / sub;
            const planar = try allocator.alloc(Sample, pixels + 2 * chroma_pixels);
            errdefer allocator.free(planar);
            const counts = try allocator.alloc(u8, (pixels + 2 * chroma_pixels) / 16);
            errdefer allocator.free(counts);
            const modes = try allocator.alloc(u8, pixels / 16);
            errdefer allocator.free(modes);
            const qps = try allocator.alloc(u8, pixels / 256);
            errdefer allocator.free(qps);
            const metadata = try allocator.alloc(entropy.Meta, pixels / 256);
            errdefer allocator.free(metadata);
            const motion0 = try allocator.alloc(motion.Motion, pixels / 16);
            errdefer allocator.free(motion0);
            const motion1 = try allocator.alloc(motion.Motion, pixels / 16);
            errdefer allocator.free(motion1);
            const map = try allocator.alloc(u8, groups);
            errdefer allocator.free(map);
            const selected = try allocator.alloc(bool, selections);
            var self = Self{ .planar = planar, .counts = counts, .modes = modes, .qps = qps, .metadata = metadata, .motions = .{ motion0, motion1 }, .groups = map, .selected = selected };
            self.reset(0);
            self.active = false;
            return self;
        }
        pub fn reset(self: *Self, id: u32) void {
            std.debug.assert(self.snapshot == null);
            @memset(self.planar, 0);
            @memset(self.counts, 0);
            @memset(self.modes, 255);
            @memset(self.metadata, .{});
            @memset(self.motions[0], .{ .reference = -2 });
            @memset(self.motions[1], .{ .reference = -2 });
            @memset(self.selected, false);
            self.headers = .{ null, null };
            self.committed = .{ false, false };
            self.active = true;
            self.complete = false;
            self.id = id;
            self.slices = 0;
            self.pts = null;
            self.end = 0;
        }
        pub fn releaseSnapshot(self: *Self, allocator: std.mem.Allocator) void {
            if (self.snapshot) |snapshot| {
                snapshot.deinit(allocator);
                allocator.destroy(snapshot);
                self.snapshot = null;
            }
        }
        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.releaseSnapshot(allocator);
            allocator.free(self.planar);
            allocator.free(self.counts);
            allocator.free(self.modes);
            allocator.free(self.qps);
            allocator.free(self.metadata);
            for (self.motions) |m| allocator.free(m);
            allocator.free(self.groups);
            allocator.free(self.selected);
        }
        pub fn covers(self: *const Self, field: bool, parity: usize, width: usize) bool {
            for (self.metadata, 0..) |m, i| {
                if (field and i / (width / 16) % 2 != parity) continue;
                if (m.kind == 255) return false;
            }
            return true;
        }
        pub fn wanted(self: *const Self) bool {
            for (self.selected) |selected| if (selected) return true;
            return false;
        }
    };
}
