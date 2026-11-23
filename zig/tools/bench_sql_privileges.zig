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

//! Compare one atomic grant matrix with repeated per-grant ACL preparation.
const std = @import("std");
const graph = @import("privileges");
fn principal(id: u128) graph.Principal {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &bytes, id, .little);
    return .{ .kind = .user, .instance = bytes };
}
pub fn main(init: std.process.Init) !void {
    const backing = init.arena.allocator();
    for ([_]usize{ 128, 1024, 8192 }) |count| {
        const initial = try backing.alloc(graph.Grant, count);
        for (initial, 0..) |*grant, i| grant.* = .{ .grantor = .bootstrap, .grantee = principal(i + 1), .privilege = .select };
        var grantees: [64]graph.Principal = undefined;
        for (&grantees, 0..) |*grantee, i| grantee.* = principal(count + i + 1);
        var elapsed = [_]i96{ 0, 0 };
        for (0..7) |sample| {
            for (0..2) |order| {
                const mode = (sample + order) % 2;
                var arena = std.heap.ArenaAllocator.init(backing);
                defer arena.deinit();
                const a = arena.allocator();
                var object: graph.Object = .{ .owner = .bootstrap, .grants = initial };
                const start = std.Io.Clock.awake.now(init.io).nanoseconds;
                if (mode == 0) {
                    object.grants = try graph.apply(a, object, .{ .grantees = &grantees, .privileges = &.{.select}, .grantors = &.{.bootstrap} });
                } else {
                    for (grantees) |grantee| object.grants = try object.add(a, .{ .grantor = .bootstrap, .grantee = grantee, .privilege = .select });
                }
                elapsed[mode] += std.Io.Clock.awake.now(init.io).nanoseconds - start;
                if (object.grants.len != count + grantees.len) return error.InvalidResult;
            }
        }
        std.debug.print("edges={d} recipients=64 samples=7 batch_us={d} per_grant_us={d} ratio={d}\n", .{ count, @divTrunc(elapsed[0], 7000), @divTrunc(elapsed[1], 7000), @divTrunc(elapsed[1], elapsed[0]) });
    }
}
