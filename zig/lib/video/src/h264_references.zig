// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const motion = @import("h264_motion.zig");
const MarkCommand = struct { operation: u32, first: u32 = 0, second: u32 = 0 };
fn PictureFor(comptime Sample: type) type {
    return struct {
        owners: *usize,
        non_existing: bool = false,
        pair_id: ?u32 = null,
        long_term: ?u32 = null,
        field_long: [2]?u32 = .{ null, null },
        planar: []Sample,
        motions: [2][]motion.Motion,
        frame_num: u32,
        poc: i32,
        field_poc: [2]i32,
        fields: [2]bool,
        id: u32,
        list_ids: [2][16]u32,
        meta: []@import("h264_entropy.zig").Meta,
        paired: bool,
    };
}
pub const State = StateFor(u8);
pub fn StateFor(comptime Sample: type) type {
    return struct {
        const Self = @This();
        const Picture = PictureFor(Sample);
        pictures: [16]Picture = undefined,
        count: usize = 0,
        chroma_format: u8 = 1,
        field_mode: bool = false,
        field_picture: bool = false,
        current_fields: [2]bool = .{ true, true },
        field_lists: [2][32]u8 = @splat(@splat(255)),
        field_counts: [2]usize = .{ 0, 0 },
        field_parity: usize = 0,
        paired: bool = false,
        current_meta: []const @import("h264_entropy.zig").Meta = &.{},
        list0: [16]usize = undefined,
        list1: [16]usize = undefined,
        next_id: u32 = 0,
        weights: @import("h264_weights.zig").Table = @splat(@splat(@splat(.{}))),
        weight_mode: enum { none, explicit, implicit } = .none,
        list_count: usize = 0,
        current_pair: ?u32 = null,
        current_num: u32 = 0,
        current_poc: i32 = 0,
        current_field_poc: [2]i32 = .{ 0, 0 },
        previous_lsb: i32 = 0,
        previous_msb: i32 = 0,
        reference: bool = false,
        frame_bits: usize = 4,
        frame_offset: i32 = 0,
        previous_num: u32 = 0,
        previous_reference_num: u32 = 0,
        adaptive: bool = false,
        current_long: ?u32 = null,
        max_field_long: ?u32 = null,
        commands: [32]Command = undefined,
        command_count: usize = 0,
        pub const Command = MarkCommand;
        /// H.264 8.2.5.2: inferred frames occupy the DPB without sample storage.
        pub fn inferGaps(self: *Self, allocator: std.mem.Allocator, number: u32, cfg: @import("h264.zig").Config, control: @import("antfly_media").source.Control) !void {
            const maximum: u32 = @as(u32, 1) << @as(u5, @intCast(cfg.frame_bits));
            var missing = (self.previous_reference_num + 1) % maximum;
            if (number == self.previous_reference_num or number == missing) return;
            if (!cfg.gaps_allowed) return error.MissingVideoReference;
            while (missing != number) : (missing = (missing + 1) % maximum) {
                try control.check();
                if (self.previous_num > missing) self.frame_offset = std.math.add(i32, self.frame_offset, @intCast(maximum)) catch return error.TimestampOverflow;
                self.previous_num = missing;
                self.current_num = missing;
                self.current_pair = null;
                self.reference = true;
                self.field_picture = false;
                self.paired = false;
                self.current_fields = .{ true, true };
                self.current_meta = &.{};
                self.command_count = 0;
                self.adaptive = false;
                self.current_long = null;
                self.list_count = 0;
                var poc: i64 = 0;
                if (cfg.poc_type == 2) poc = (@as(i64, self.frame_offset) + missing) * 2;
                if (cfg.poc_type == 1 and cfg.poc_cycle != 0) {
                    const absolute = @as(i64, self.frame_offset) + missing;
                    if (absolute > 0) {
                        var total: i64 = 0;
                        for (cfg.poc_offsets[0..cfg.poc_cycle]) |offset| total += offset;
                        poc = @divTrunc(absolute - 1, @as(i64, @intCast(cfg.poc_cycle))) * total;
                        for (cfg.poc_offsets[0 .. @as(usize, @intCast(@mod(absolute - 1, @as(i64, @intCast(cfg.poc_cycle))))) + 1]) |offset| poc += offset;
                    }
                }
                self.current_poc = std.math.cast(i32, poc) orelse return error.TimestampOverflow;
                self.current_field_poc = .{ self.current_poc, std.math.cast(i32, poc + (if (cfg.poc_type == 1) @as(i64, cfg.poc_bottom) else 0)) orelse return error.TimestampOverflow };
                try self.commit(allocator, &.{}, .{ &.{}, &.{} }, @max(cfg.max_refs, 1));
                self.pictures[self.count - 1].non_existing = true;
            }
        }
        pub fn marking(self: *Self, bits: *@import("h264_bits.zig").Bits, idr: bool) !void {
            self.command_count = 0;
            self.current_long = null;
            self.adaptive = false;
            if (!self.reference) return;
            if (idr) {
                _ = try bits.read(1);
                if (try bits.read(1) != 0) self.current_long = 0;
                self.max_field_long = self.current_long;
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
                if ((operation == 2 and command.first > (if (self.field_picture) @as(u32, 31) else 15) or operation == 6 and command.first > 15) or operation == 3 and command.second > 15 or operation == 4 and command.first > 16) return error.UnsupportedVideoProfile;
                self.commands[self.command_count] = command;
                self.command_count += 1;
            }
        }
        fn release(allocator: std.mem.Allocator, picture: Picture) void {
            std.debug.assert(picture.owners.* != 0);
            picture.owners.* -= 1;
            if (picture.owners.* == 0) {
                allocator.free(picture.planar);
                allocator.free(picture.meta);
                for (picture.motions) |m| allocator.free(m);
                allocator.destroy(picture.owners);
            }
        }
        /// A field's prediction view retains the original DPB allocations even
        /// if subsequent coded pictures evict or change their marking status.
        pub fn clone(self: *const Self) Self {
            const copy = self.*;
            for (copy.pictures[0..copy.count]) |pic| pic.owners.* += 1;
            return copy;
        }
        fn samePair(self: *const Self, pic: Picture) bool {
            return if (self.current_pair) |id| pic.pair_id == id else pic.frame_num == self.current_num;
        }
        fn remove(self: *Self, allocator: std.mem.Allocator, index: usize) void {
            release(allocator, self.pictures[index]);
            std.mem.copyForwards(Picture, self.pictures[index .. self.count - 1], self.pictures[index + 1 .. self.count]);
            self.count -= 1;
        }
        fn removeLong(self: *Self, allocator: std.mem.Allocator, number: u32) void {
            var i: usize = 0;
            while (i < self.count) {
                if (self.pictures[i].long_term == number) self.remove(allocator, i) else i += 1;
            }
        }
        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            for (self.pictures[0..self.count]) |pic| release(allocator, pic);
            self.count = 0;
        }
        pub fn order(self: *Self, frame_bits: usize) void {
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
        pub fn orderB(self: *Self) void {
            self.list_count = 0;
            self.list0 = @splat(16);
            self.list1 = @splat(16);
            for (0..self.count) |i| if (!self.pictures[i].non_existing) {
                self.list0[self.list_count] = i;
                self.list1[self.list_count] = i;
                self.list_count += 1;
            };
            for (0..2) |list| {
                const indexes = if (list == 0) &self.list0 else &self.list1;
                for (0..self.list_count) |i| for (i + 1..self.list_count) |j| {
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
            if (!self.field_picture and self.list_count > 1 and std.mem.eql(usize, self.list0[0..self.list_count], self.list1[0..self.list_count])) std.mem.swap(usize, &self.list1[0], &self.list1[1]);
        }
        pub fn orderFields(self: *Self, parity: usize, b_slice: bool) void {
            self.field_parity = parity;
            self.field_lists = @splat(@splat(255));
            self.field_counts = .{ 0, 0 };
            for (0..2) |list| for (0..2) |phase| {
                const frames = if (list == 0) self.list0 else self.list1;
                var parity_frames = [2][16]usize{ frames, frames };
                if (phase == 1) for (0..2) |field_side| {
                    for (0..self.list_count) |i| for (i + 1..self.list_count) |j| {
                        const al = self.pictures[parity_frames[field_side][i]].field_long[field_side] orelse std.math.maxInt(u32);
                        const bl = self.pictures[parity_frames[field_side][j]].field_long[field_side] orelse std.math.maxInt(u32);
                        if (bl < al) std.mem.swap(usize, &parity_frames[field_side][i], &parity_frames[field_side][j]);
                    };
                };
                var cursors: [2]usize = .{ 0, 0 };
                var wanted = parity;
                while (true) {
                    var found: ?usize = null;
                    for (0..2) |_| {
                        while (cursors[wanted] < self.list_count) {
                            const i = parity_frames[wanted][cursors[wanted]];
                            cursors[wanted] += 1;
                            if (i < self.count and self.pictures[i].fields[wanted] and (self.pictures[i].field_long[wanted] != null) == (phase == 1)) {
                                found = i;
                                break;
                            }
                        }
                        if (found != null) break;
                        wanted = 1 - wanted;
                    }
                    const index = found orelse break;
                    self.field_lists[list][self.field_counts[list]] = @intCast(index * 2 + wanted);
                    self.field_counts[list] += 1;
                    wanted = 1 - wanted;
                }
            };
            if (b_slice and self.field_counts[0] >= 2 and self.field_counts[0] == self.field_counts[1] and std.mem.eql(u8, self.field_lists[0][0..self.field_counts[0]], self.field_lists[1][0..self.field_counts[1]])) std.mem.swap(u8, &self.field_lists[1][0], &self.field_lists[1][1]);
        }
        pub fn reorderFields(self: *Self, bits: *@import("h264_bits.zig").Bits, list: usize, active: usize) !void {
            if (active > 32) return error.UnsupportedVideoProfile;
            const maximum: u32 = @as(u32, 1) << @as(u5, @intCast(self.frame_bits + 1));
            var predicted = self.current_num * 2 + 1;
            var insertion: usize = 0;
            self.field_counts[list] = @max(self.field_counts[list], active);
            while (true) {
                const operation = try bits.ue();
                if (operation == 3) return;
                if (operation > 2 or insertion >= active) return error.MalformedVideoPacket;
                const argument = try bits.ue();
                var number = argument;
                if (operation < 2) {
                    if (argument >= maximum) return error.MalformedVideoPacket;
                    predicted = if (operation == 0) (predicted + maximum - argument - 1) % maximum else (predicted + argument + 1) % maximum;
                    number = predicted;
                }
                var target: ?u8 = null;
                for (self.pictures[0..self.count], 0..) |pic, i| for (0..2) |parity| {
                    if (!pic.fields[parity]) continue;
                    const same: u32 = @intFromBool(parity == self.field_parity);
                    const matches = if (operation == 2) (if (pic.field_long[parity]) |long| long * 2 + same == number else false) else pic.field_long[parity] == null and (pic.frame_num * 2 + same) % maximum == number;
                    if (matches) target = @intCast(i * 2 + parity);
                };
                const encoded = target orelse return error.MissingVideoReference;
                if (self.pictures[encoded / 2].non_existing) return error.MissingVideoReference;
                var next = self.field_lists[list];
                next[insertion] = encoded;
                var cursor = insertion + 1;
                for (self.field_lists[list][insertion..self.field_counts[list]]) |entry| if (entry != encoded and cursor < self.field_counts[list]) {
                    next[cursor] = entry;
                    cursor += 1;
                };
                @memset(next[cursor..], 255);
                self.field_lists[list] = next;
                insertion += 1;
            }
        }
        pub fn referenceIndex(self: *Self, list: usize, reference: usize) !usize {
            const index = if (self.field_picture) blk: {
                if (reference >= self.field_counts[list]) return error.MissingVideoReference;
                break :blk @as(usize, self.field_lists[list][reference]) / 2;
            } else blk: {
                const frame_index = reference / (if (self.field_mode) @as(usize, 2) else 1);
                if (frame_index >= self.list_count) return error.MissingVideoReference;
                break :blk (if (list == 0) self.list0 else self.list1)[frame_index];
            };
            if (index >= self.count or self.pictures[index].non_existing) return error.MissingVideoReference;
            const pic = self.pictures[index];
            if (self.field_picture) {
                if (!pic.fields[@as(usize, self.field_lists[list][reference]) % 2]) return error.MissingVideoReference;
            } else if (!self.field_mode and (!pic.fields[0] or !pic.fields[1] or pic.field_long[0] != pic.field_long[1])) return error.MissingVideoReference;
            return index;
        }
        pub fn referenceParity(self: *Self, list: usize, reference: usize) !usize {
            _ = try self.referenceIndex(list, reference);
            return if (self.field_picture) @as(usize, self.field_lists[list][reference]) % 2 else self.field_parity ^ (reference % 2);
        }
        pub fn referenceLong(self: *Self, list: usize, reference: usize) !bool {
            const pic = self.pictures[try self.referenceIndex(list, reference)];
            return if (self.field_mode or self.field_picture) pic.field_long[try self.referenceParity(list, reference)] != null else pic.long_term != null;
        }
        pub fn referenceCount(self: *Self, list: usize) usize {
            return if (self.field_picture) self.field_counts[list] else self.list_count * (if (self.field_mode) @as(usize, 2) else 1);
        }
        fn refreshPicture(pic: *Picture) void {
            pic.long_term = null;
            var has_short = false;
            var poc: i32 = std.math.maxInt(i32);
            for (0..2) |parity| if (pic.fields[parity]) {
                poc = @min(poc, pic.field_poc[parity]);
                if (pic.field_long[parity]) |long| {
                    pic.long_term = if (pic.long_term) |old| @min(old, long) else long;
                } else has_short = true;
            };
            if (has_short) {
                pic.long_term = null;
                poc = std.math.maxInt(i32);
                for (0..2) |parity| if (pic.fields[parity] and pic.field_long[parity] == null) {
                    poc = @min(poc, pic.field_poc[parity]);
                };
            }
            pic.poc = poc;
        }
        fn pruneFields(self: *Self, allocator: std.mem.Allocator) void {
            var i: usize = 0;
            while (i < self.count) {
                if (!self.pictures[i].fields[0] and !self.pictures[i].fields[1]) self.remove(allocator, i) else {
                    refreshPicture(&self.pictures[i]);
                    i += 1;
                }
            }
        }
        /// MMCO 3/6 preserve the other field of the target complementary pair.
        fn replaceFieldLong(self: *Self, allocator: std.mem.Allocator, number: u32, preserve: ?u32) void {
            for (self.pictures[0..self.count]) |*pic| {
                if (preserve != null and pic.id == preserve.?) continue;
                for (0..2) |parity| if (pic.fields[parity] and pic.field_long[parity] == number) {
                    pic.fields[parity] = false;
                };
            }
            self.pruneFields(allocator);
        }
        fn markFields(self: *Self, allocator: std.mem.Allocator) !bool {
            var reset = false;
            for (self.commands[0..self.command_count]) |command| switch (command.operation) {
                1, 3 => {
                    const maximum: u32 = @as(u32, 1) << @as(u5, @intCast(self.frame_bits + 1));
                    if (command.first >= maximum) return error.MalformedVideoPacket;
                    const number = (self.current_num * 2 + 1 + maximum - command.first - 1) % maximum;
                    var target: ?struct { id: u32, parity: usize } = null;
                    for (self.pictures[0..self.count]) |pic| for (0..2) |parity| {
                        const same: u32 = @intFromBool(parity == self.field_parity);
                        if (pic.fields[parity] and pic.field_long[parity] == null and (pic.frame_num * 2 + same) % maximum == number) target = .{ .id = pic.id, .parity = parity };
                    };
                    const found = target orelse return error.MissingVideoReference;
                    if (command.operation == 3) {
                        for (self.pictures[0..self.count]) |pic| if (pic.id == found.id and pic.non_existing) return error.MissingVideoReference;
                        if (self.max_field_long == null or command.second > self.max_field_long.?) return error.MalformedVideoPacket;
                        self.replaceFieldLong(allocator, command.second, found.id);
                    }
                    for (self.pictures[0..self.count]) |*pic| if (pic.id == found.id) {
                        if (command.operation == 1) pic.fields[found.parity] = false else {
                            if (pic.fields[1 - found.parity] and pic.field_long[1 - found.parity] != null and pic.field_long[1 - found.parity].? != command.second) return error.MalformedVideoPacket;
                            pic.field_long[found.parity] = command.second;
                        }
                    };
                },
                2 => {
                    var found = false;
                    for (self.pictures[0..self.count]) |*pic| for (0..2) |parity| {
                        const same: u32 = @intFromBool(parity == self.field_parity);
                        if (pic.fields[parity] and pic.field_long[parity] != null and pic.field_long[parity].? * 2 + same == command.first) {
                            pic.fields[parity] = false;
                            found = true;
                        }
                    };
                    if (!found) return error.MissingVideoReference;
                },
                4 => {
                    self.max_field_long = if (command.first == 0) null else command.first - 1;
                    for (self.pictures[0..self.count]) |*pic| for (0..2) |parity| {
                        if (pic.fields[parity] and pic.field_long[parity] != null and pic.field_long[parity].? >= command.first) pic.fields[parity] = false;
                    };
                },
                5 => {
                    self.deinit(allocator);
                    self.max_field_long = null;
                    reset = true;
                },
                6 => {
                    if (self.max_field_long == null or command.first > self.max_field_long.?) return error.MalformedVideoPacket;
                    var preserve: ?u32 = null;
                    for (self.pictures[0..self.count]) |pic| if (self.samePair(pic) and pic.fields[1 - self.field_parity] and !pic.fields[self.field_parity]) {
                        preserve = pic.id;
                    };
                    self.replaceFieldLong(allocator, command.first, preserve);
                    self.current_long = command.first;
                },
                else => return error.MalformedVideoPacket,
            };
            self.pruneFields(allocator);
            return reset;
        }
        pub fn commit(self: *Self, allocator: std.mem.Allocator, planar: []const Sample, motions: [2][]const motion.Motion, max_refs: usize) !void {
            if (!self.reference) return;
            self.previous_reference_num = self.current_num;
            if (max_refs == 0) return;
            for (self.commands[0..self.command_count]) |command| {
                if (command.operation == 4 and command.first > max_refs) return error.MalformedVideoPacket;
            }
            var list_ids: [2][16]u32 = @splat(@splat(std.math.maxInt(u32)));
            for (0..self.list_count) |i| {
                if (self.list0[i] < self.count) list_ids[0][i] = self.pictures[self.list0[i]].id;
                if (self.list1[i] < self.count) list_ids[1][i] = self.pictures[self.list1[i]].id;
            }
            var reset = if (self.field_picture) try self.markFields(allocator) else false;
            if (!self.field_picture) for (self.commands[0..self.command_count]) |command| {
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
                        if (command.operation == 3) {
                            for (self.pictures[0..self.count]) |pic| if (pic.id == id.? and pic.non_existing) return error.MissingVideoReference;
                            self.removeLong(allocator, command.second);
                        }
                        for (self.pictures[0..self.count], 0..) |*pic, i| if (pic.id == id.?) {
                            if (command.operation == 1) self.remove(allocator, i) else {
                                pic.long_term = command.second;
                                pic.field_long = @splat(command.second);
                            }
                            break;
                        };
                    },
                    2 => self.removeLong(allocator, command.first),
                    4 => {
                        self.max_field_long = if (command.first == 0) null else command.first - 1;
                        for (self.pictures[0..self.count]) |*pic| for (0..2) |parity| {
                            if (pic.fields[parity] and pic.field_long[parity] != null and pic.field_long[parity].? >= command.first) pic.fields[parity] = false;
                        };
                        self.pruneFields(allocator);
                    },
                    5 => {
                        self.deinit(allocator);
                        self.max_field_long = null;
                        reset = true;
                    },
                    6 => {
                        self.removeLong(allocator, command.first);
                        self.current_long = command.first;
                    },
                    else => unreachable,
                }
            };
            if (reset) {
                self.current_num = 0;
                if (self.field_picture) self.current_field_poc[self.field_parity] = 0 else self.current_field_poc = .{ 0, 0 };
                self.current_poc = 0;
                self.previous_num = 0;
                self.previous_reference_num = 0;
                self.frame_offset = 0;
                self.previous_lsb = 0;
                self.previous_msb = 0;
            }
            var short_complement = false;
            if (self.field_picture) for (self.pictures[0..self.count]) |pic| {
                if (self.samePair(pic) and pic.fields[1 - self.field_parity] and !pic.fields[self.field_parity] and pic.field_long[1 - self.field_parity] == null) short_complement = true;
            };
            if (!self.adaptive and !short_complement) {
                var used: usize = 0;
                for (self.pictures[0..self.count]) |pic| {
                    var short = false;
                    var long = false;
                    for (0..2) |parity| if (pic.fields[parity]) {
                        if (pic.field_long[parity] == null) short = true else long = true;
                    };
                    used += @intFromBool(short) + @as(usize, @intFromBool(long));
                }
                if (used >= max_refs) {
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
            }
            var complementary: ?usize = null;
            if (self.field_picture) for (self.pictures[0..self.count], 0..) |pic, i| {
                if (self.samePair(pic) and pic.fields[1 - self.field_parity] and !pic.fields[self.field_parity]) {
                    complementary = i;
                    break;
                }
            };
            if (complementary) |slot| {
                // Complete the already admitted frame buffer in place. Its first
                // field was available to the second field's reference list.
                const pic = &self.pictures[slot];
                if (pic.planar.len != planar.len or pic.meta.len != self.current_meta.len or pic.motions[0].len != motions[0].len or pic.motions[1].len != motions[1].len) return error.UnsupportedDynamicGeometry;
                if (pic.owners.* == 1) {
                    @memcpy(pic.planar, planar);
                    @memcpy(pic.meta, self.current_meta);
                    for (0..2) |list| @memcpy(pic.motions[list], motions[list]);
                } else {
                    const owned = try allocator.dupe(Sample, planar);
                    errdefer allocator.free(owned);
                    const owned_meta = try allocator.dupe(@import("h264_entropy.zig").Meta, self.current_meta);
                    errdefer allocator.free(owned_meta);
                    const motion0 = try allocator.dupe(motion.Motion, motions[0]);
                    errdefer allocator.free(motion0);
                    const motion1 = try allocator.dupe(motion.Motion, motions[1]);
                    errdefer allocator.free(motion1);
                    const owners = try allocator.create(usize);
                    owners.* = 1;
                    release(allocator, pic.*);
                    pic.planar = owned;
                    pic.meta = owned_meta;
                    pic.motions = .{ motion0, motion1 };
                    pic.owners = owners;
                }
                if (pic.field_long[1 - self.field_parity] != null and self.current_long != null and pic.field_long[1 - self.field_parity].? != self.current_long.?) return error.MalformedVideoPacket;
                pic.fields[self.field_parity] = true;
                pic.field_poc[self.field_parity] = self.current_field_poc[self.field_parity];
                pic.field_long[self.field_parity] = self.current_long;
                pic.list_ids = list_ids;
                refreshPicture(pic);
                return;
            }
            if (complementary == null and self.count >= max_refs) return error.MalformedVideoPacket;
            const owned = try allocator.dupe(Sample, planar);
            errdefer allocator.free(owned);
            const motion0 = try allocator.dupe(motion.Motion, motions[0]);
            errdefer allocator.free(motion0);
            const motion1 = try allocator.dupe(motion.Motion, motions[1]);
            errdefer allocator.free(motion1);
            const owned_meta = try allocator.dupe(@import("h264_entropy.zig").Meta, self.current_meta);
            errdefer allocator.free(owned_meta);
            const owners = try allocator.create(usize);
            owners.* = 1;
            self.pictures[self.count] = .{ .owners = owners, .pair_id = self.current_pair, .fields = self.current_fields, .meta = owned_meta, .paired = self.paired, .planar = owned, .motions = .{ motion0, motion1 }, .long_term = self.current_long, .field_long = .{ if (self.current_fields[0]) self.current_long else null, if (self.current_fields[1]) self.current_long else null }, .frame_num = self.current_num, .poc = self.current_poc, .field_poc = self.current_field_poc, .id = self.next_id, .list_ids = list_ids };
            self.next_id += 1;
            self.count += 1;
        }
        pub fn planes(self: *Self, list: usize, reference: usize, width: usize, height: usize) ![3][]Sample {
            const index = try self.referenceIndex(list, reference);
            const picture = self.pictures[index].planar;
            const pixels = width * height;
            const sub_y: usize = if (self.chroma_format == 1) 2 else 1;
            const sub_x: usize = if (self.chroma_format == 3) 1 else 2;
            const chroma_pixels = if (self.chroma_format == 0) @as(usize, 0) else pixels / (sub_x * sub_y);
            return .{ picture[0..pixels], picture[pixels..][0..chroma_pixels], picture[pixels + chroma_pixels ..] };
        }
    };
}
