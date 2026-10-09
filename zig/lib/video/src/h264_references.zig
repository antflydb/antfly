// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const motion = @import("h264_motion.zig");
pub const Picture = struct {
    long_term: ?u32 = null,
    planar: []u8,
    motions: [2][]motion.Motion,
    frame_num: u32,
    poc: i32,
    id: u32,
    list_ids: [2][16]u32,
};
pub const State = struct {
    pictures: [16]Picture = undefined,
    count: usize = 0,
    list0: [16]usize = undefined,
    list1: [16]usize = undefined,
    next_id: u32 = 0,
    weights: @import("h264_weights.zig").Table = @splat(@splat(@splat(.{}))),
    weight_mode: enum { none, explicit, implicit } = .none,
    list_count: usize = 0,
    current_num: u32 = 0,
    current_poc: i32 = 0,
    previous_lsb: i32 = 0,
    previous_msb: i32 = 0,
    reference: bool = false,
    frame_bits: usize = 4,
    frame_offset: i32 = 0,
    previous_num: u32 = 0,
    adaptive: bool = false,
    current_long: ?u32 = null,
    commands: [32]Command = undefined,
    command_count: usize = 0,
    pub const Command = struct { operation: u32, first: u32 = 0, second: u32 = 0 };
    pub fn marking(self: *State, bits: *@import("h264_bits.zig").Bits, idr: bool) !void {
        self.command_count = 0;
        self.current_long = null;
        self.adaptive = false;
        if (!self.reference) return;
        if (idr) {
            _ = try bits.read(1);
            if (try bits.read(1) != 0) self.current_long = 0;
            return;
        }
        self.adaptive = try bits.read(1) != 0;
        if (!self.adaptive) return;
        while (true) {
            const operation = try bits.ue();
            if (operation == 0) break;
            if (operation > 6 or self.command_count >= self.commands.len) return error.MalformedVideoPacket;
            var command = Command{ .operation = operation };
            if (operation == 1 or operation == 2 or operation == 3 or operation == 4 or operation == 6) command.first = try bits.ue();
            if (operation == 3) command.second = try bits.ue();
            if ((operation == 2 or operation == 6) and command.first > 15 or operation == 3 and command.second > 15 or operation == 4 and command.first > 16) return error.UnsupportedVideoProfile;
            self.commands[self.command_count] = command;
            self.command_count += 1;
        }
    }
    fn remove(self: *State, allocator: std.mem.Allocator, index: usize) void {
        const picture = self.pictures[index];
        allocator.free(picture.planar);
        for (picture.motions) |m| allocator.free(m);
        std.mem.copyForwards(Picture, self.pictures[index .. self.count - 1], self.pictures[index + 1 .. self.count]);
        self.count -= 1;
    }
    fn removeLong(self: *State, allocator: std.mem.Allocator, number: u32) void {
        var i: usize = 0;
        while (i < self.count) {
            if (self.pictures[i].long_term == number) self.remove(allocator, i) else i += 1;
        }
    }
    pub fn deinit(self: *State, allocator: std.mem.Allocator) void {
        for (self.pictures[0..self.count]) |pic| {
            allocator.free(pic.planar);
            for (pic.motions) |m| allocator.free(m);
        }
        self.count = 0;
    }
    pub fn order(self: *State, frame_bits: usize) void {
        const max_num: i32 = @as(i32, 1) << @as(u5, @intCast(frame_bits));
        self.list_count = self.count;
        self.list0 = @splat(16);
        for (0..self.count) |i| self.list0[i] = i;
        for (0..self.count) |i| for (i + 1..self.count) |j| {
            const a = self.pictures[self.list0[i]].frame_num;
            const b = self.pictures[self.list0[j]].frame_num;
            const an = @as(i32, @intCast(a)) - (if (a > self.current_num) max_num else 0);
            const bn = @as(i32, @intCast(b)) - (if (b > self.current_num) max_num else 0);
            const al = self.pictures[self.list0[i]].long_term;
            const bl = self.pictures[self.list0[j]].long_term;
            const better = if (al != null and bl != null) bl.? < al.? else if (al != null or bl != null) al != null else bn > an;
            if (better) std.mem.swap(usize, &self.list0[i], &self.list0[j]);
        };
        self.list1 = self.list0;
    }
    pub fn orderB(self: *State) void {
        self.list_count = self.count;
        self.list0 = @splat(16);
        self.list1 = @splat(16);
        for (0..self.count) |i| {
            self.list0[i] = i;
            self.list1[i] = i;
        }
        for (0..2) |list| {
            const indexes = if (list == 0) &self.list0 else &self.list1;
            for (0..self.count) |i| for (i + 1..self.count) |j| {
                const a = self.pictures[indexes[i]].poc;
                const b = self.pictures[indexes[j]].poc;
                const af = if (list == 0) a < self.current_poc else a > self.current_poc;
                const bf = if (list == 0) b < self.current_poc else b > self.current_poc;
                const better = if (af != bf) bf else if (list == 0) (if (bf) b > a else b < a) else (if (bf) b < a else b > a);
                const al = self.pictures[indexes[i]].long_term;
                const bl = self.pictures[indexes[j]].long_term;
                const ordered = if (al != null and bl != null) bl.? < al.? else if (al != null or bl != null) al != null else better;
                if (ordered) std.mem.swap(usize, &indexes[i], &indexes[j]);
            };
        }
        if (self.count > 1 and std.mem.eql(usize, self.list0[0..self.count], self.list1[0..self.count])) std.mem.swap(usize, &self.list1[0], &self.list1[1]);
    }
    pub fn commit(self: *State, allocator: std.mem.Allocator, planar: []const u8, motions: [2][]const motion.Motion, max_refs: usize) !void {
        if (!self.reference or max_refs == 0) return;
        var list_ids: [2][16]u32 = @splat(@splat(std.math.maxInt(u32)));
        for (0..self.list_count) |i| {
            if (self.list0[i] < self.count) list_ids[0][i] = self.pictures[self.list0[i]].id;
            if (self.list1[i] < self.count) list_ids[1][i] = self.pictures[self.list1[i]].id;
        }
        var reset = false;
        for (self.commands[0..self.command_count]) |command| {
            switch (command.operation) {
                1, 3 => {
                    const maximum: u32 = @as(u32, 1) << @as(u5, @intCast(self.frame_bits));
                    if (command.first >= maximum) return error.MalformedVideoPacket;
                    const number = (self.current_num + maximum - command.first - 1) % maximum;
                    var id: ?u32 = null;
                    for (self.pictures[0..self.count]) |pic| if (pic.long_term == null and pic.frame_num == number) {
                        id = pic.id;
                        break;
                    };
                    if (id == null) return error.MissingVideoReference;
                    if (command.operation == 3) self.removeLong(allocator, command.second);
                    for (self.pictures[0..self.count], 0..) |*pic, i| if (pic.id == id.?) {
                        if (command.operation == 1) self.remove(allocator, i) else pic.long_term = command.second;
                        break;
                    };
                },
                2 => self.removeLong(allocator, command.first),
                4 => {
                    var i: usize = 0;
                    while (i < self.count) {
                        if (self.pictures[i].long_term) |n| {
                            if (n >= command.first) {
                                self.remove(allocator, i);
                                continue;
                            }
                        }
                        i += 1;
                    }
                },
                5 => {
                    self.deinit(allocator);
                    reset = true;
                },
                6 => {
                    self.removeLong(allocator, command.first);
                    self.current_long = command.first;
                },
                else => unreachable,
            }
        }
        if (!self.adaptive and self.count >= max_refs) {
            var oldest: ?usize = null;
            var oldest_num: i32 = std.math.maxInt(i32);
            const maximum: i32 = @as(i32, 1) << @as(u5, @intCast(self.frame_bits));
            for (self.pictures[0..self.count], 0..) |pic, i| if (pic.long_term == null) {
                const n = @as(i32, @intCast(pic.frame_num)) - (if (pic.frame_num > self.current_num) maximum else 0);
                if (n < oldest_num) {
                    oldest_num = n;
                    oldest = i;
                }
            };
            self.remove(allocator, oldest orelse return error.MissingVideoReference);
        }
        if (self.count >= max_refs) return error.MalformedVideoPacket;
        if (reset) {
            self.current_num = 0;
            self.current_poc = 0;
            self.previous_num = 0;
            self.frame_offset = 0;
            self.previous_lsb = 0;
            self.previous_msb = 0;
        }
        const owned = try allocator.dupe(u8, planar);
        errdefer allocator.free(owned);
        const motion0 = try allocator.dupe(motion.Motion, motions[0]);
        errdefer allocator.free(motion0);
        const motion1 = try allocator.dupe(motion.Motion, motions[1]);
        errdefer allocator.free(motion1);
        self.pictures[self.count] = .{ .planar = owned, .motions = .{ motion0, motion1 }, .long_term = self.current_long, .frame_num = self.current_num, .poc = self.current_poc, .id = self.next_id, .list_ids = list_ids };
        self.next_id += 1;
        self.count += 1;
    }
    pub fn planes(self: *State, list: usize, reference: usize, width: usize, height: usize) ![3][]const u8 {
        if (reference >= self.list_count) return error.MissingVideoReference;
        const index = (if (list == 0) self.list0 else self.list1)[reference];
        if (index >= self.count) return error.MissingVideoReference;
        const picture = self.pictures[index].planar;
        const pixels = width * height;
        return .{ picture[0..pixels], picture[pixels..][0 .. pixels / 4], picture[pixels + pixels / 4 ..] };
    }
};
