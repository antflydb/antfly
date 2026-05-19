// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

pub const RFTConfig = struct {
    reward_threshold: f32 = 0.5,
    top_k_per_prompt: usize = 0,
    dedupe: bool = true,
};

pub const ScoredCompletion = struct {
    prompt_idx: usize,
    tokens: []const i32,
    reward: f32,
};

pub const FilterResult = struct {
    allocator: std.mem.Allocator,
    kept: []usize,
    kept_per_prompt: []usize,

    pub fn deinit(self: *FilterResult) void {
        self.allocator.free(self.kept);
        self.allocator.free(self.kept_per_prompt);
        self.* = undefined;
    }
};

const IndexedEntry = struct {
    orig_idx: usize,
    reward: f32,
};

fn entryDesc(_: void, a: IndexedEntry, b: IndexedEntry) bool {
    return a.reward > b.reward;
}

fn tokensEqual(a: []const i32, b: []const i32) bool {
    if (a.len != b.len) return false;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        if (a[i] != b[i]) return false;
    }
    return true;
}

pub fn filterByReward(
    allocator: std.mem.Allocator,
    completions: []const ScoredCompletion,
    config: RFTConfig,
) !FilterResult {
    var num_prompts: usize = 0;
    for (completions) |c| {
        if (c.prompt_idx + 1 > num_prompts) num_prompts = c.prompt_idx + 1;
    }

    const kept_per_prompt = try allocator.alloc(usize, num_prompts);
    errdefer allocator.free(kept_per_prompt);
    @memset(kept_per_prompt, 0);

    var kept_list: std.ArrayList(usize) = .empty;
    defer kept_list.deinit(allocator);

    var group_buf: std.ArrayList(IndexedEntry) = .empty;
    defer group_buf.deinit(allocator);

    var kept_in_group: std.ArrayList(usize) = .empty;
    defer kept_in_group.deinit(allocator);

    var p: usize = 0;
    while (p < num_prompts) : (p += 1) {
        group_buf.clearRetainingCapacity();
        for (completions, 0..) |c, i| {
            if (c.prompt_idx == p) {
                try group_buf.append(allocator, .{ .orig_idx = i, .reward = c.reward });
            }
        }
        if (group_buf.items.len == 0) continue;

        std.mem.sort(IndexedEntry, group_buf.items, {}, entryDesc);

        kept_in_group.clearRetainingCapacity();
        for (group_buf.items, 0..) |entry, rank| {
            const meets_threshold = entry.reward >= config.reward_threshold;
            const in_top_k = config.top_k_per_prompt > 0 and rank < config.top_k_per_prompt;
            if (!(meets_threshold or in_top_k)) continue;

            if (config.dedupe) {
                var duplicate = false;
                for (kept_in_group.items) |existing_idx| {
                    if (tokensEqual(completions[existing_idx].tokens, completions[entry.orig_idx].tokens)) {
                        duplicate = true;
                        break;
                    }
                }
                if (duplicate) continue;
            }

            try kept_in_group.append(allocator, entry.orig_idx);
        }

        kept_per_prompt[p] = kept_in_group.items.len;
        for (kept_in_group.items) |idx| {
            try kept_list.append(allocator, idx);
        }
    }

    const kept = try kept_list.toOwnedSlice(allocator);
    return FilterResult{
        .allocator = allocator,
        .kept = kept,
        .kept_per_prompt = kept_per_prompt,
    };
}

// -------------------- tests --------------------

const testing = std.testing;

test "all below threshold, top_k=0 -> empty" {
    const alloc = testing.allocator;
    const t = [_]i32{1};
    const comps = [_]ScoredCompletion{
        .{ .prompt_idx = 0, .tokens = &t, .reward = 0.1 },
        .{ .prompt_idx = 0, .tokens = &t, .reward = 0.2 },
        .{ .prompt_idx = 1, .tokens = &t, .reward = 0.3 },
    };
    var res = try filterByReward(alloc, &comps, .{ .reward_threshold = 0.9, .top_k_per_prompt = 0, .dedupe = false });
    defer res.deinit();

    try testing.expectEqual(@as(usize, 0), res.kept.len);
    try testing.expectEqual(@as(usize, 2), res.kept_per_prompt.len);
    try testing.expectEqual(@as(usize, 0), res.kept_per_prompt[0]);
    try testing.expectEqual(@as(usize, 0), res.kept_per_prompt[1]);
}

test "top_k=2 with infinite threshold keeps exactly 2 per prompt" {
    const alloc = testing.allocator;
    const ta = [_]i32{1};
    const tb = [_]i32{2};
    const tc = [_]i32{3};
    const td = [_]i32{4};
    const te = [_]i32{5};
    const comps = [_]ScoredCompletion{
        .{ .prompt_idx = 0, .tokens = &ta, .reward = 0.1 },
        .{ .prompt_idx = 0, .tokens = &tb, .reward = 0.5 },
        .{ .prompt_idx = 0, .tokens = &tc, .reward = 0.9 },
        .{ .prompt_idx = 1, .tokens = &td, .reward = 0.2 },
        .{ .prompt_idx = 1, .tokens = &te, .reward = 0.8 },
    };
    var res = try filterByReward(alloc, &comps, .{
        .reward_threshold = std.math.inf(f32),
        .top_k_per_prompt = 2,
        .dedupe = true,
    });
    defer res.deinit();

    try testing.expectEqual(@as(usize, 2), res.kept_per_prompt[0]);
    try testing.expectEqual(@as(usize, 2), res.kept_per_prompt[1]);
    try testing.expectEqual(@as(usize, 4), res.kept.len);

    // prompt 0 top-2 by reward: indices 2 (0.9) then 1 (0.5)
    try testing.expectEqual(@as(usize, 2), res.kept[0]);
    try testing.expectEqual(@as(usize, 1), res.kept[1]);
    // prompt 1 top-2: 4 (0.8) then 3 (0.2)
    try testing.expectEqual(@as(usize, 4), res.kept[2]);
    try testing.expectEqual(@as(usize, 3), res.kept[3]);
}

test "dedup keeps only first by reward" {
    const alloc = testing.allocator;
    const same = [_]i32{ 7, 8, 9 };
    const other = [_]i32{ 1, 2 };
    const comps = [_]ScoredCompletion{
        .{ .prompt_idx = 0, .tokens = &same, .reward = 0.7 },
        .{ .prompt_idx = 0, .tokens = &same, .reward = 0.9 },
        .{ .prompt_idx = 0, .tokens = &other, .reward = 0.8 },
    };
    var res = try filterByReward(alloc, &comps, .{
        .reward_threshold = 0.5,
        .top_k_per_prompt = 0,
        .dedupe = true,
    });
    defer res.deinit();

    try testing.expectEqual(@as(usize, 2), res.kept.len);
    try testing.expectEqual(@as(usize, 2), res.kept_per_prompt[0]);
    // Sorted desc: index 1 (0.9, same) first kept; index 2 (0.8, other) kept;
    // index 0 (0.7, same) dropped as duplicate.
    try testing.expectEqual(@as(usize, 1), res.kept[0]);
    try testing.expectEqual(@as(usize, 2), res.kept[1]);
}

test "multi-prompt mixed counts" {
    const alloc = testing.allocator;
    const t1 = [_]i32{1};
    const t2 = [_]i32{2};
    const t3 = [_]i32{3};
    const t4 = [_]i32{4};
    const t5 = [_]i32{5};
    const t6 = [_]i32{6};
    const comps = [_]ScoredCompletion{
        .{ .prompt_idx = 0, .tokens = &t1, .reward = 0.9 },
        .{ .prompt_idx = 0, .tokens = &t2, .reward = 0.6 },
        .{ .prompt_idx = 1, .tokens = &t3, .reward = 0.1 },
        .{ .prompt_idx = 1, .tokens = &t4, .reward = 0.2 },
        .{ .prompt_idx = 2, .tokens = &t5, .reward = 0.55 },
        .{ .prompt_idx = 2, .tokens = &t6, .reward = 0.8 },
    };
    var res = try filterByReward(alloc, &comps, .{
        .reward_threshold = 0.5,
        .top_k_per_prompt = 0,
        .dedupe = true,
    });
    defer res.deinit();

    try testing.expectEqual(@as(usize, 3), res.kept_per_prompt.len);
    try testing.expectEqual(@as(usize, 2), res.kept_per_prompt[0]);
    try testing.expectEqual(@as(usize, 0), res.kept_per_prompt[1]);
    try testing.expectEqual(@as(usize, 2), res.kept_per_prompt[2]);
    try testing.expectEqual(@as(usize, 4), res.kept.len);
}
