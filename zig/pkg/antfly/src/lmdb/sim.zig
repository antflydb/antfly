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

pub fn reduceFailingSequence(
    comptime Action: type,
    allocator: std.mem.Allocator,
    original: []const Action,
    replayer: anytype,
) ![]Action {
    var best = try allocator.dupe(Action, original);
    errdefer allocator.free(best);

    if (!sequenceFails(Action, best, replayer)) return best;

    var granularity: usize = 2;
    while (best.len > 1) {
        const chunk_size = @max(@divFloor(best.len, granularity), 1);
        var progress = false;
        var start: usize = 0;

        while (start < best.len) : (start += chunk_size) {
            const end = @min(start + chunk_size, best.len);
            if (end == start) break;

            const candidate = try removeRange(Action, allocator, best, start, end);
            defer allocator.free(candidate);

            if (!sequenceFails(Action, candidate, replayer)) continue;

            allocator.free(best);
            best = try allocator.dupe(Action, candidate);
            progress = true;
            granularity = 2;
            break;
        }

        if (progress) continue;
        if (granularity >= best.len) break;
        granularity = @min(best.len, granularity * 2);
    }

    return best;
}

fn sequenceFails(comptime Action: type, actions: []const Action, replayer: anytype) bool {
    _ = replayer.replay(actions) catch return true;
    return false;
}

fn removeRange(
    comptime Action: type,
    allocator: std.mem.Allocator,
    actions: []const Action,
    start: usize,
    end: usize,
) ![]Action {
    const removed = end - start;
    const out = try allocator.alloc(Action, actions.len - removed);
    @memcpy(out[0..start], actions[0..start]);
    @memcpy(out[start..], actions[end..]);
    return out;
}

test "reduceFailingSequence removes unrelated actions" {
    const allocator = std.testing.allocator;
    const Action = u8;
    const actions = [_]Action{ 1, 2, 9, 3, 4 };

    const Replayer = struct {
        fn replay(_: @This(), candidate: []const Action) !void {
            for (candidate) |action| {
                if (action == 9) return error.ScheduleFailed;
            }
        }
    };

    const reduced = try reduceFailingSequence(Action, allocator, &actions, Replayer{});
    defer allocator.free(reduced);

    try std.testing.expectEqual(@as(usize, 1), reduced.len);
    try std.testing.expectEqual(@as(Action, 9), reduced[0]);
}
