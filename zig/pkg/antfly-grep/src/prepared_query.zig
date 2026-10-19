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

//! Bounded, process-local immutable query registrations. A handle binds all
//! effective options and credentials; requests cannot override that identity.
const std = @import("std");
const A = std.mem.Allocator;
const Options = @import("options.zig").Options;
const Matcher = @import("matcher.zig").Matcher;
pub fn Registry(comptime Search: type) type {
    return struct {
        const Self = @This();
        pub const capacity = 8;
        pub const plan_limit = 2 * 1024 * 1024;
        pub const Plan = struct {
            a: A,
            budget: @import("antfly_regex").Budget,
            arena: std.heap.ArenaAllocator,
            options: Options = .{},
            matcher: Matcher = undefined,
            cwd: [:0]const u8 = undefined,
            identity: [32]u8 = undefined,
            evaluation: [32]u8 = undefined,
            handle: [64]u8 = undefined,
            stamp: u64 = 0,
            // One scalar certificate per immutable registration. No result
            // payload, generation, matcher scratch or source bytes are pinned.
            completion: ?struct { token: [64]u8, revision: [64]u8, summary: Search.SummaryType } = null,
            lexical: Search.LexicalIndex = undefined,
            lexical_admission: Search.LexicalIndex.Admission = .{},
            index: Search.IncrementalIndex = undefined,
            delivery_arena: ?std.heap.ArenaAllocator = null,
            delivery: ?Search.Delivery = null,
            incremental_disabled_revision: ?[64]u8 = null,
            fn create(a: A, io: std.Io, base: *const Options, args: []const []const u8, compact: bool) !*Plan {
                const plan = try a.create(Plan);
                errdefer a.destroy(plan);
                plan.* = .{ .a = a, .budget = .{ .backing = a, .limit = plan_limit }, .arena = undefined };
                plan.index = Search.IncrementalIndex.init(a, io);
                plan.lexical = Search.LexicalIndex.init(a, io);
                plan.arena = .init(plan.budget.allocator());
                errdefer plan.arena.deinit();
                const scratch = plan.arena.allocator();
                const owned = try scratch.alloc([]const u8, args.len);
                for (args, owned) |arg, *dest| dest.* = try scratch.dupe(u8, arg);
                plan.options = try @import("workspace_options.zig").prepare(scratch, base, owned, compact);
                plan.matcher = try Matcher.initForSearch(plan.budget.allocator(), &plan.options);
                errdefer plan.matcher.deinit();
                plan.cwd = try std.process.currentPathAlloc(io, scratch);
                const ids = try Search.Reuse.identities(scratch, &plan.options);
                plan.identity = ids.full;
                plan.evaluation = ids.evaluation;
                var random: [32]u8 = undefined;
                io.random(&random);
                plan.handle = std.fmt.bytesToHex(random, .lower);
                return plan;
            }
            pub fn remember(plan: *Plan, token: [64]u8) void {
                var arena = std.heap.ArenaAllocator.init(plan.budget.allocator());
                var keep = false;
                defer if (!keep) arena.deinit();
                const a = arena.allocator();
                var members: std.ArrayList(@import("incremental.zig").Member) = .empty;
                var by_path: std.StringHashMapUnmanaged([32]u8) = .empty;
                var current = plan.index.first();
                while (current) |file| : (current = Search.IncrementalIndex.next(file)) {
                    if (members.items.len >= plan.options.max_result_files) break;
                    const path = a.dupe(u8, file.path) catch return;
                    members.append(a, .{ .path = path, .tag = file.tag }) catch return;
                    by_path.put(a, path, file.tag) catch return;
                }
                if (plan.delivery_arena) |*old| old.deinit();
                plan.delivery_arena = arena;
                plan.delivery = .{ .token = token, .revision = plan.index.revision.?, .members = members.items, .by_path = by_path };
                keep = true;
            }
            pub fn deinit(plan: *Plan) void {
                const a = plan.a;
                plan.lexical.deinit();
                plan.index.deinit();
                if (plan.delivery_arena) |*arena| arena.deinit();
                plan.matcher.deinit();
                plan.arena.deinit();
                a.destroy(plan);
            }
        };
        a: A,
        io: std.Io,
        slots: [capacity]?*Plan = @splat(null),
        clock: u64 = 0,
        pub fn retainedBytes(self: *const Self) usize {
            var bytes: usize = 0;
            for (self.slots) |slot| if (slot) |plan| {
                bytes += @sizeOf(Plan) + plan.budget.live + plan.index.budget.live + plan.lexical.budget.live;
            };
            return bytes;
        }
        pub fn deinit(self: *Self) void {
            for (&self.slots) |*slot| if (slot.*) |plan| {
                plan.deinit();
                slot.* = null;
            };
        }
        pub fn register(self: *Self, base: *const Options, args: []const []const u8, compact: bool) !*Plan {
            // Construct before eviction. Up to one additional bounded plan is
            // live during admission; a failed registration preserves all handles.
            const plan = try Plan.create(self.a, self.io, base, args, compact);
            var victim: usize = 0;
            for (self.slots, 0..) |slot, i| {
                if (slot == null) {
                    victim = i;
                    break;
                }
                if (slot.?.stamp < self.slots[victim].?.stamp) victim = i;
            }
            if (self.slots[victim]) |old| old.deinit();
            self.clock +%= 1;
            plan.stamp = self.clock;
            self.slots[victim] = plan;
            return plan;
        }
        pub fn get(self: *Self, handle: []const u8) ?*Plan {
            for (self.slots) |slot| if (slot) |plan| {
                if (std.mem.eql(u8, handle, &plan.handle)) {
                    self.clock +%= 1;
                    plan.stamp = self.clock;
                    return plan;
                }
            };
            return null;
        }
        pub fn forget(self: *Self, handle: []const u8) bool {
            for (&self.slots) |*slot| if (slot.*) |plan| {
                if (std.mem.eql(u8, handle, &plan.handle)) {
                    slot.* = null;
                    plan.deinit();
                    return true;
                }
            };
            return false;
        }
    };
}
