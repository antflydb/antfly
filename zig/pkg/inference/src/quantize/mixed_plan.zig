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

//! Mixed-precision quantization plans.
//!
//! A sensitivity file lists, for each quantizable tensor, candidate formats
//! with their encoded size and a measured loss (how much quantizing only that
//! tensor to that format hurts the chosen metric). `allocate` picks one
//! candidate per tensor so the summed loss is as small as possible within a
//! byte budget. The resulting plan maps GGUF output tensor names to formats
//! and is consumed by `antfly inference export --quantize-plan`.
//!
//! Losses are treated as additive across tensors. That is exact for the
//! data-free weight-error proxy and a first-order approximation for
//! output-space metrics (embedding cosine loss, token KL, WER) measured one
//! tensor at a time.
//!
//! The allocator walks each tensor's lower convex hull of (bytes, loss) and
//! takes upgrades in order of loss saved per byte. Every allocation it reaches
//! at a hull breakpoint is optimal for its byte total (the Lagrangian
//! solution); the remaining budget is then filled greedily with any upgrade
//! that still fits, and pairwise exchanges repair the gaps between
//! breakpoints. A dynamic program over bytes rounded up to 1/65536 of the
//! budget runs beside it when its table fits in memory, and the better of the
//! two plans wins. Both always respect the real budget.

const std = @import("std");

const Allocator = std.mem.Allocator;

pub const sensitivity_schema = "antfly-quant-sensitivity/v1";
pub const plan_schema = "antfly-quant-plan/v1";

pub const Candidate = struct {
    format: []const u8,
    bytes: u64,
    loss: f64,
};

pub const Item = struct {
    name: []const u8,
    elements: u64,
    candidates: []const Candidate,
};

pub const SensitivityFile = struct {
    schema: []const u8,
    metric: []const u8,
    tensors: []const Item,
};

pub const Allocation = struct {
    /// Index into `items[i].candidates` for every item.
    choices: []usize,
    total_bytes: u64,
    total_loss: f64,
    total_elements: u64,

    pub fn deinit(self: *Allocation, allocator: Allocator) void {
        allocator.free(self.choices);
    }

    pub fn bitsPerWeight(self: Allocation) f64 {
        if (self.total_elements == 0) return 0;
        return @as(f64, @floatFromInt(self.total_bytes)) * 8.0 / @as(f64, @floatFromInt(self.total_elements));
    }
};

pub const Budget = union(enum) {
    bytes: u64,
    bits_per_weight: f64,

    pub fn resolve(self: Budget, items: []const Item) u64 {
        return switch (self) {
            .bytes => |value| value,
            .bits_per_weight => |bpw| blk: {
                var elements: u64 = 0;
                for (items) |item| elements += item.elements;
                break :blk @intFromFloat(@floor(bpw * @as(f64, @floatFromInt(elements)) / 8.0));
            },
        };
    }
};

pub const AllocateError = error{
    EmptyCandidates,
    InvalidLoss,
    BudgetTooSmall,
    OutOfMemory,
};

const Step = struct {
    item: usize,
    /// Position in the item's hull this step moves to.
    hull_position: usize,
    slope: f64,
    bytes: u64,
};

/// Choose one candidate per item minimizing total loss with total bytes at
/// most `budget_bytes`. Fails with `BudgetTooSmall` when even the smallest
/// candidate of every item exceeds the budget.
pub fn allocate(allocator: Allocator, items: []const Item, budget_bytes: u64) AllocateError!Allocation {
    const choices = try allocator.alloc(usize, items.len);
    errdefer allocator.free(choices);

    var hulls = try allocator.alloc([]usize, items.len);
    var hulls_built: usize = 0;
    defer {
        for (hulls[0..hulls_built]) |hull| allocator.free(hull);
        allocator.free(hulls);
    }

    var total_bytes: u64 = 0;
    var total_elements: u64 = 0;
    var step_count: usize = 0;
    for (items, 0..) |item, i| {
        if (item.candidates.len == 0) return error.EmptyCandidates;
        for (item.candidates) |candidate| {
            if (!std.math.isFinite(candidate.loss)) return error.InvalidLoss;
        }
        hulls[i] = try lowerHull(allocator, item.candidates);
        hulls_built += 1;
        choices[i] = hulls[i][0];
        total_bytes += item.candidates[choices[i]].bytes;
        total_elements += item.elements;
        step_count += hulls[i].len - 1;
    }
    if (total_bytes > budget_bytes) return error.BudgetTooSmall;

    // Hull slopes decrease along each hull, so one global sort by slope keeps
    // every item's steps in hull order.
    const steps = try allocator.alloc(Step, step_count);
    defer allocator.free(steps);
    var next: usize = 0;
    for (items, 0..) |item, i| {
        const hull = hulls[i];
        for (1..hull.len) |position| {
            const from = item.candidates[hull[position - 1]];
            const to = item.candidates[hull[position]];
            const added = to.bytes - from.bytes;
            steps[next] = .{
                .item = i,
                .hull_position = position,
                .slope = (from.loss - to.loss) / @as(f64, @floatFromInt(added)),
                .bytes = added,
            };
            next += 1;
        }
    }
    std.mem.sort(Step, steps, {}, struct {
        fn greater(_: void, a: Step, b: Step) bool {
            if (a.slope != b.slope) return a.slope > b.slope;
            if (a.item != b.item) return a.item < b.item;
            return a.hull_position < b.hull_position;
        }
    }.greater);

    const position = try allocator.alloc(usize, items.len);
    defer allocator.free(position);
    @memset(position, 0);
    const frozen = try allocator.alloc(bool, items.len);
    defer allocator.free(frozen);
    @memset(frozen, false);

    for (steps) |step| {
        if (frozen[step.item]) continue;
        if (position[step.item] + 1 != step.hull_position) continue;
        if (total_bytes + step.bytes > budget_bytes) {
            frozen[step.item] = true;
            continue;
        }
        total_bytes += step.bytes;
        position[step.item] = step.hull_position;
        choices[step.item] = hulls[step.item][step.hull_position];
    }

    fillRemaining(items, choices, &total_bytes, budget_bytes);

    // The hull walk is optimal only at breakpoints, which matters when a few
    // large tensors dominate the budget. Exchange passes (upgrade one tensor,
    // downgrade another to pay for it) close most of that gap; they are
    // quadratic in candidates, so very large plans keep the greedy result.
    var candidate_count: u64 = 0;
    for (items) |item| candidate_count += item.candidates.len;
    if (candidate_count * candidate_count <= max_exchange_work) {
        var passes: usize = 0;
        while (passes < max_exchange_passes and exchangeOnce(items, choices, &total_bytes, budget_bytes)) : (passes += 1) {
            fillRemaining(items, choices, &total_bytes, budget_bytes);
        }
    }

    var total_loss = summedLoss(items, choices);

    // A dynamic program over bytes rounded up to 1/dp_resolution of the
    // budget is near-exact and always feasible; keep it when it is better.
    if (try scaledKnapsack(allocator, items, budget_bytes)) |dp_choices| {
        defer allocator.free(dp_choices);
        var dp_bytes: u64 = 0;
        for (items, 0..) |item, i| dp_bytes += item.candidates[dp_choices[i]].bytes;
        fillRemaining(items, dp_choices, &dp_bytes, budget_bytes);
        const dp_loss = summedLoss(items, dp_choices);
        if (dp_loss < total_loss) {
            @memcpy(choices, dp_choices);
            total_bytes = dp_bytes;
            total_loss = dp_loss;
        }
    }

    return .{
        .choices = choices,
        .total_bytes = total_bytes,
        .total_loss = total_loss,
        .total_elements = total_elements,
    };
}

const max_exchange_work: u64 = 64 * 1024 * 1024;
const max_exchange_passes: usize = 256;

const dp_resolution: u64 = 1 << 16;
const max_dp_table_bytes: u64 = 256 * 1024 * 1024;

fn summedLoss(items: []const Item, choices: []const usize) f64 {
    var total: f64 = 0;
    for (items, 0..) |item, i| total += item.candidates[choices[i]].loss;
    return total;
}

/// Multiple-choice knapsack over byte units of ceil(budget / dp_resolution).
/// Candidate sizes round up, so any solution it returns fits the real budget.
/// Returns null when the choice table would be too large or nothing fits.
fn scaledKnapsack(allocator: Allocator, items: []const Item, budget_bytes: u64) AllocateError!?[]usize {
    if (items.len == 0 or budget_bytes == 0) return null;
    const unit = @max(1, std.math.divCeil(u64, budget_bytes, dp_resolution) catch unreachable);
    const capacity: usize = @intCast(budget_bytes / unit);
    const width = capacity + 1;
    for (items) |item| if (item.candidates.len > std.math.maxInt(u8)) return null;
    if (@as(u64, items.len) * width > max_dp_table_bytes) return null;

    const inf = std.math.inf(f64);
    var cost = try allocator.alloc(f64, width);
    defer allocator.free(cost);
    var next = try allocator.alloc(f64, width);
    defer allocator.free(next);
    const pick = try allocator.alloc(u8, items.len * width);
    defer allocator.free(pick);
    @memset(cost, 0);

    for (items, 0..) |item, i| {
        @memset(next, inf);
        const row = pick[i * width ..][0..width];
        for (item.candidates, 0..) |candidate, c| {
            const units_u64 = std.math.divCeil(u64, candidate.bytes, unit) catch unreachable;
            if (units_u64 > capacity) continue;
            const units: usize = @intCast(units_u64);
            for (units..width) |b| {
                const value = cost[b - units] + candidate.loss;
                if (value < next[b]) {
                    next[b] = value;
                    row[b] = @intCast(c);
                }
            }
        }
        std.mem.swap([]f64, &cost, &next);
    }
    if (cost[capacity] == inf) return null;

    const choices = try allocator.alloc(usize, items.len);
    errdefer allocator.free(choices);
    var b = capacity;
    var i = items.len;
    while (i > 0) {
        i -= 1;
        const c = pick[i * width + b];
        choices[i] = c;
        const units: usize = @intCast(std.math.divCeil(u64, items[i].candidates[c].bytes, unit) catch unreachable);
        b -= units;
    }
    return choices;
}

/// Repeatedly apply the single best loss-per-byte upgrade that still fits,
/// including candidates off the hull, until nothing improves.
fn fillRemaining(items: []const Item, choices: []usize, total_bytes: *u64, budget_bytes: u64) void {
    while (true) {
        var best_item: ?usize = null;
        var best_candidate: usize = 0;
        var best_rate: f64 = 0;
        for (items, 0..) |item, i| {
            const current = item.candidates[choices[i]];
            for (item.candidates, 0..) |candidate, c| {
                if (candidate.loss >= current.loss) continue;
                const added = if (candidate.bytes > current.bytes) candidate.bytes - current.bytes else 0;
                if (total_bytes.* - current.bytes + candidate.bytes > budget_bytes) continue;
                const saved = current.loss - candidate.loss;
                const rate = if (added == 0) std.math.inf(f64) else saved / @as(f64, @floatFromInt(added));
                if (best_item == null or rate > best_rate) {
                    best_item = i;
                    best_candidate = c;
                    best_rate = rate;
                }
            }
        }
        const i = best_item orelse return;
        const old = items[i].candidates[choices[i]];
        const new = items[i].candidates[best_candidate];
        total_bytes.* = total_bytes.* - old.bytes + new.bytes;
        choices[i] = best_candidate;
    }
}

/// Apply the best pair change (one tensor up, another down to pay for it)
/// that lowers total loss within the budget. Returns false when none does.
fn exchangeOnce(items: []const Item, choices: []usize, total_bytes: *u64, budget_bytes: u64) bool {
    const slack = budget_bytes - total_bytes.*;
    var best_net: f64 = 0;
    var best: ?struct { i: usize, c: usize, j: usize, d: usize } = null;
    for (items, 0..) |item, i| {
        const current = item.candidates[choices[i]];
        for (item.candidates, 0..) |up, c| {
            if (up.loss >= current.loss or up.bytes <= current.bytes) continue;
            const added = up.bytes - current.bytes;
            if (added <= slack) continue;
            const need = added - slack;
            const gain = current.loss - up.loss;
            for (items, 0..) |other, j| {
                if (j == i) continue;
                const other_current = other.candidates[choices[j]];
                for (other.candidates, 0..) |down, d| {
                    if (down.bytes >= other_current.bytes) continue;
                    if (other_current.bytes - down.bytes < need) continue;
                    const net = gain - (down.loss - other_current.loss);
                    if (net > best_net * (1 + 1e-12) + 1e-300) {
                        best_net = net;
                        best = .{ .i = i, .c = c, .j = j, .d = d };
                    }
                }
            }
        }
    }
    const move = best orelse return false;
    const old_i = items[move.i].candidates[choices[move.i]];
    const old_j = items[move.j].candidates[choices[move.j]];
    total_bytes.* = total_bytes.* - old_i.bytes - old_j.bytes +
        items[move.i].candidates[move.c].bytes + items[move.j].candidates[move.d].bytes;
    choices[move.i] = move.c;
    choices[move.j] = move.d;
    return true;
}

/// Indices of the candidates on the lower convex hull of (bytes, loss),
/// ordered by increasing bytes and strictly decreasing loss.
fn lowerHull(allocator: Allocator, candidates: []const Candidate) ![]usize {
    const order = try allocator.alloc(usize, candidates.len);
    defer allocator.free(order);
    for (order, 0..) |*slot, i| slot.* = i;
    std.mem.sort(usize, order, candidates, struct {
        fn less(c: []const Candidate, a: usize, b: usize) bool {
            if (c[a].bytes != c[b].bytes) return c[a].bytes < c[b].bytes;
            if (c[a].loss != c[b].loss) return c[a].loss < c[b].loss;
            return a < b;
        }
    }.less);

    var hull = std.ArrayListUnmanaged(usize).empty;
    errdefer hull.deinit(allocator);
    for (order) |index| {
        const point = candidates[index];
        if (hull.items.len > 0) {
            const last = candidates[hull.items[hull.items.len - 1]];
            // Same size or larger without lower loss: dominated.
            if (point.bytes == last.bytes or point.loss >= last.loss) continue;
        }
        while (hull.items.len >= 2) {
            const a = candidates[hull.items[hull.items.len - 2]];
            const b = candidates[hull.items[hull.items.len - 1]];
            // Drop b when it lies on or above the segment from a to point.
            const ab = (a.loss - b.loss) / @as(f64, @floatFromInt(b.bytes - a.bytes));
            const bp = (b.loss - point.loss) / @as(f64, @floatFromInt(point.bytes - b.bytes));
            if (bp >= ab) {
                _ = hull.pop();
            } else break;
        }
        try hull.append(allocator, index);
    }
    return hull.toOwnedSlice(allocator);
}

pub fn parseSensitivity(allocator: Allocator, bytes: []const u8) !std.json.Parsed(SensitivityFile) {
    const parsed = try std.json.parseFromSlice(SensitivityFile, allocator, bytes, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    errdefer parsed.deinit();
    if (!std.mem.eql(u8, parsed.value.schema, sensitivity_schema)) return error.UnsupportedSensitivitySchema;
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(allocator);
    for (parsed.value.tensors) |item| {
        const entry = try seen.getOrPut(allocator, item.name);
        if (entry.found_existing) return error.DuplicateTensorName;
    }
    return parsed;
}

pub const PlanEntry = struct {
    name: []const u8,
    format: []const u8,
};

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    entries: []const PlanEntry,

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
    }

    pub fn find(self: *const Plan, name: []const u8) ?[]const u8 {
        for (self.entries) |entry| {
            if (std.mem.eql(u8, entry.name, name)) return entry.format;
        }
        return null;
    }
};

/// Parse `{"schema": "antfly-quant-plan/v1", "tensors": {"<name>": "<format>"}}`.
/// Other top-level fields (budget, totals) are informational and ignored.
pub fn parsePlan(allocator: Allocator, bytes: []const u8) !Plan {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    if (root != .object) return error.InvalidPlan;
    const schema = root.object.get("schema") orelse return error.InvalidPlan;
    if (schema != .string or !std.mem.eql(u8, schema.string, plan_schema)) return error.UnsupportedPlanSchema;
    const tensors = root.object.get("tensors") orelse return error.InvalidPlan;
    if (tensors != .object) return error.InvalidPlan;
    const entries = try a.alloc(PlanEntry, tensors.object.count());
    var it = tensors.object.iterator();
    var i: usize = 0;
    while (it.next()) |kv| : (i += 1) {
        if (kv.value_ptr.* != .string) return error.InvalidPlan;
        entries[i] = .{ .name = kv.key_ptr.*, .format = kv.value_ptr.string };
    }
    return .{ .arena = arena, .entries = entries };
}

pub fn writePlanJson(
    allocator: Allocator,
    metric: []const u8,
    budget_bytes: u64,
    items: []const Item,
    allocation: Allocation,
) ![]u8 {
    var text: std.Io.Writer.Allocating = .init(allocator);
    errdefer text.deinit();
    const w = &text.writer;
    try w.writeAll("{\n");
    try w.print("  \"schema\": \"{s}\",\n", .{plan_schema});
    try w.writeAll("  \"metric\": ");
    try std.json.Stringify.value(metric, .{}, w);
    try w.print(",\n  \"budget_bytes\": {d},\n", .{budget_bytes});
    try w.print("  \"planned_bytes\": {d},\n", .{allocation.total_bytes});
    try w.print("  \"planned_elements\": {d},\n", .{allocation.total_elements});
    try w.print("  \"bits_per_weight\": {d:.4},\n", .{allocation.bitsPerWeight()});
    try w.writeAll("  \"total_loss\": ");
    try std.json.Stringify.value(allocation.total_loss, .{}, w);
    try w.writeAll(",\n  \"tensors\": {");
    for (items, 0..) |item, i| {
        try w.writeAll(if (i == 0) "\n    " else ",\n    ");
        try std.json.Stringify.value(item.name, .{}, w);
        try w.writeAll(": ");
        try std.json.Stringify.value(item.candidates[allocation.choices[i]].format, .{}, w);
    }
    try w.writeAll(if (items.len == 0) "}\n}\n" else "\n  }\n}\n");
    return text.toOwnedSlice();
}

pub fn writeSensitivityJson(allocator: Allocator, metric: []const u8, items: []const Item) ![]u8 {
    const file: SensitivityFile = .{ .schema = sensitivity_schema, .metric = metric, .tensors = items };
    return std.json.Stringify.valueAlloc(allocator, file, .{ .whitespace = .indent_2 });
}

/// Encoded size of a quantized tensor with `rows` rows of `row_width`
/// elements, for formats whose blocks hold `block_elements` values in
/// `block_bytes` bytes. Returns null when the row does not divide into blocks.
pub fn quantizedBytes(rows: u64, row_width: u64, block_elements: u64, block_bytes: u64) ?u64 {
    if (block_elements == 0 or row_width % block_elements != 0) return null;
    return rows * (row_width / block_elements) * block_bytes;
}

// ----------------------------------------------------------------------------- command

pub fn main(allocator: Allocator, io: std.Io, args: []const []const u8) !void {
    var sensitivity_path: ?[]const u8 = null;
    var output_path: ?[]const u8 = null;
    var budget: ?Budget = null;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--sensitivity") and i + 1 < args.len) {
            sensitivity_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, arg, "--output") and i + 1 < args.len) {
            output_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, arg, "--bits-per-weight") and i + 1 < args.len) {
            const value = std.fmt.parseFloat(f64, args[i + 1]) catch return usage();
            if (!(value > 0)) return usage();
            budget = .{ .bits_per_weight = value };
            i += 1;
        } else if (std.mem.eql(u8, arg, "--budget-bytes") and i + 1 < args.len) {
            budget = .{ .bytes = std.fmt.parseInt(u64, args[i + 1], 10) catch return usage() };
            i += 1;
        } else {
            return usage();
        }
    }
    const path = sensitivity_path orelse return usage();
    const chosen_budget = budget orelse return usage();

    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(512 * 1024 * 1024));
    defer allocator.free(bytes);
    const parsed = try parseSensitivity(allocator, bytes);
    defer parsed.deinit();
    const items = parsed.value.tensors;

    const budget_bytes = chosen_budget.resolve(items);
    var allocation = allocate(allocator, items, budget_bytes) catch |err| switch (err) {
        error.BudgetTooSmall => {
            var minimum: u64 = 0;
            for (items) |item| {
                var smallest: u64 = std.math.maxInt(u64);
                for (item.candidates) |candidate| smallest = @min(smallest, candidate.bytes);
                minimum += smallest;
            }
            std.debug.print("budget {d} bytes is below the smallest plan ({d} bytes)\n", .{ budget_bytes, minimum });
            return err;
        },
        else => return err,
    };
    defer allocation.deinit(allocator);

    const json = try writePlanJson(allocator, parsed.value.metric, budget_bytes, items, allocation);
    defer allocator.free(json);
    if (output_path) |out| {
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = out, .data = json });
        std.debug.print("wrote plan to {s}: {d} tensors, {d} bytes, {d:.4} bits/weight, loss {e}\n", .{
            out, items.len, allocation.total_bytes, allocation.bitsPerWeight(), allocation.total_loss,
        });
    } else {
        std.debug.print("{s}", .{json});
    }
}

fn usage() error{InvalidArguments} {
    std.debug.print(
        \\usage: antfly inference quantize-plan --sensitivity <file.json> (--bits-per-weight <x> | --budget-bytes <n>) [--output <plan.json>]
        \\
        \\Chooses one format per tensor from a sensitivity file so the summed loss is
        \\smallest within the budget. The budget covers only the tensors the file lists.
        \\Apply the plan with: antfly inference export <model-dir> --target gguf --format <base> --quantize-plan <plan.json>
        \\
    , .{});
    return error.InvalidArguments;
}

// ----------------------------------------------------------------------------- tests

fn testItem(name: []const u8, elements: u64, candidates: []const Candidate) Item {
    return .{ .name = name, .elements = elements, .candidates = candidates };
}

test "lowerHull drops dominated and non-convex candidates" {
    const candidates = [_]Candidate{
        .{ .format = "q8_0", .bytes = 1000, .loss = 1 },
        .{ .format = "q4_k", .bytes = 500, .loss = 20 },
        .{ .format = "q5_k", .bytes = 600, .loss = 19 }, // nearly no gain: not on the hull
        .{ .format = "q6_k", .bytes = 800, .loss = 2 },
        .{ .format = "q4_0", .bytes = 560, .loss = 25 }, // dominated by q4_k
    };
    const hull = try lowerHull(std.testing.allocator, &candidates);
    defer std.testing.allocator.free(hull);
    try std.testing.expectEqualSlices(usize, &.{ 1, 3, 0 }, hull);
}

test "allocate spends bytes where they save the most loss" {
    const sensitive = [_]Candidate{
        .{ .format = "q4_k", .bytes = 100, .loss = 50 },
        .{ .format = "q6_k", .bytes = 150, .loss = 5 },
        .{ .format = "q8_0", .bytes = 200, .loss = 1 },
    };
    const robust = [_]Candidate{
        .{ .format = "q4_k", .bytes = 100, .loss = 2 },
        .{ .format = "q6_k", .bytes = 150, .loss = 1 },
        .{ .format = "q8_0", .bytes = 200, .loss = 0.5 },
    };
    const items = [_]Item{ testItem("robust", 400, &robust), testItem("sensitive", 400, &sensitive) };

    var tight = try allocate(std.testing.allocator, &items, 250);
    defer tight.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, tight.choices);
    try std.testing.expectEqual(@as(u64, 250), tight.total_bytes);
    try std.testing.expectApproxEqAbs(@as(f64, 7), tight.total_loss, 1e-9);

    var loose = try allocate(std.testing.allocator, &items, 400);
    defer loose.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(usize, &.{ 2, 2 }, loose.choices);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), loose.total_loss, 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), loose.bitsPerWeight(), 1e-9);

    try std.testing.expectError(error.BudgetTooSmall, allocate(std.testing.allocator, &items, 199));
}

test "allocate fills leftover budget with off-hull upgrades" {
    // q5_k is off the hull, but after the hull walk stops at q4_k (q8_0 does
    // not fit), it is the best upgrade the remaining 60 bytes can buy.
    const candidates = [_]Candidate{
        .{ .format = "q4_k", .bytes = 100, .loss = 10 },
        .{ .format = "q5_k", .bytes = 150, .loss = 8 },
        .{ .format = "q8_0", .bytes = 200, .loss = 0 },
    };
    const items = [_]Item{testItem("w", 400, &candidates)};
    var result = try allocate(std.testing.allocator, &items, 160);
    defer result.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(usize, &.{1}, result.choices);
}

test "allocate is exact on small random instances" {
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const random = prng.random();
    const bytes_per_format = [_]u64{ 56, 72, 88, 104, 136 };
    var candidates: [4][5]Candidate = undefined;
    var items: [4]Item = undefined;
    var trial: usize = 0;
    var optimal: usize = 0;
    var worst: f64 = 1;
    while (trial < 200) : (trial += 1) {
        for (&candidates, 0..) |*row, i| {
            // Loss falls roughly geometrically with bytes, as quantization error does.
            const scale = random.float(f64) * 100 + 1;
            for (row, 0..) |*candidate, f| {
                const noise = 0.75 + random.float(f64) * 0.5;
                candidate.* = .{
                    .format = "x",
                    .bytes = bytes_per_format[f] * (1 + i),
                    .loss = scale * std.math.pow(f64, 0.25, @floatFromInt(f)) * noise,
                };
            }
            items[i] = testItem("t", 256, &row.*);
        }
        const min_bytes: u64 = 56 * (1 + 2 + 3 + 4);
        const max_bytes: u64 = 136 * (1 + 2 + 3 + 4);
        const budget = min_bytes + random.uintLessThan(u64, max_bytes - min_bytes + 1);
        var result = try allocate(std.testing.allocator, &items, budget);
        defer result.deinit(std.testing.allocator);
        try std.testing.expect(result.total_bytes <= budget);

        var best: f64 = std.math.inf(f64);
        var pick: [4]usize = .{ 0, 0, 0, 0 };
        while (true) {
            var bytes: u64 = 0;
            var loss: f64 = 0;
            for (pick, 0..) |c, i| {
                bytes += candidates[i][c].bytes;
                loss += candidates[i][c].loss;
            }
            if (bytes <= budget) best = @min(best, loss);
            var k: usize = 0;
            while (k < 4) : (k += 1) {
                pick[k] += 1;
                if (pick[k] < 5) break;
                pick[k] = 0;
            }
            if (k == 4) break;
        }
        try std.testing.expect(result.total_loss >= best - 1e-9);
        worst = @max(worst, result.total_loss / best);
        if (result.total_loss <= best + 1e-9) optimal += 1;
    }
    try std.testing.expectEqual(@as(usize, 200), optimal);
    try std.testing.expect(worst <= 1 + 1e-9);
}

test "allocate stays within 0.1% of the exact optimum on model-sized instances" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xa11c);
    const random = prng.random();
    const n = 40;
    const formats = 5;
    // Bytes per block for q4_0, q4_k, q5_k, q6_k and q8_0 relative to 256 values.
    const block_bytes = [_]u64{ 144, 144, 176, 210, 272 };
    const block_loss = [_]f64{ 1.3, 1.0, 0.27, 0.07, 0.004 };
    var candidates: [n][formats]Candidate = undefined;
    var items: [n]Item = undefined;
    var worst: f64 = 1;
    var trial: usize = 0;
    while (trial < 20) : (trial += 1) {
        var min_bytes: u64 = 0;
        var max_bytes: u64 = 0;
        for (&candidates, 0..) |*row, i| {
            const blocks: u64 = 1 + random.uintLessThan(u64, 12);
            const sensitivity = std.math.pow(f64, 10, random.float(f64) * 3);
            for (row, 0..) |*candidate, f| {
                candidate.* = .{
                    .format = "x",
                    .bytes = blocks * block_bytes[f],
                    .loss = sensitivity * @as(f64, @floatFromInt(blocks)) * block_loss[f] * (0.8 + 0.4 * random.float(f64)),
                };
            }
            items[i] = testItem("t", blocks * 256, &row.*);
            min_bytes += row[0].bytes;
            max_bytes += row[formats - 1].bytes;
        }
        const budget = min_bytes + (max_bytes - min_bytes) * (1 + random.uintLessThan(u64, 8)) / 10;
        var result = try allocate(allocator, &items, budget);
        defer result.deinit(allocator);
        try std.testing.expect(result.total_bytes <= budget);

        // Exact multiple-choice knapsack over bytes.
        const dp = try allocator.alloc(f64, budget + 1);
        defer allocator.free(dp);
        const next = try allocator.alloc(f64, budget + 1);
        defer allocator.free(next);
        @memset(dp, 0);
        for (items) |item| {
            @memset(next, std.math.inf(f64));
            for (0..budget + 1) |b| {
                for (item.candidates) |candidate| {
                    if (candidate.bytes > b) continue;
                    next[b] = @min(next[b], dp[b - candidate.bytes] + candidate.loss);
                }
            }
            @memcpy(dp, next);
        }
        const best = dp[budget];
        try std.testing.expect(result.total_loss >= best - 1e-6 * best);
        worst = @max(worst, result.total_loss / best);
    }
    try std.testing.expect(worst < 1.001);
}

test "sensitivity and plan files round-trip" {
    const allocator = std.testing.allocator;
    const candidates = [_]Candidate{
        .{ .format = "q4_k", .bytes = 144, .loss = 3.5 },
        .{ .format = "q8_0", .bytes = 272, .loss = 0.25 },
    };
    const items = [_]Item{ testItem("blk.0.attn_q.weight", 256, &candidates), testItem("token_embd.weight", 256, &candidates) };
    const sensitivity = try writeSensitivityJson(allocator, "weight_sse", &items);
    defer allocator.free(sensitivity);
    const parsed = try parseSensitivity(allocator, sensitivity);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.tensors.len);
    try std.testing.expectEqualStrings("token_embd.weight", parsed.value.tensors[1].name);
    try std.testing.expectEqual(@as(u64, 272), parsed.value.tensors[1].candidates[1].bytes);

    var allocation = try allocate(allocator, parsed.value.tensors, 144 + 272);
    defer allocation.deinit(allocator);
    const plan_json = try writePlanJson(allocator, parsed.value.metric, 144 + 272, parsed.value.tensors, allocation);
    defer allocator.free(plan_json);
    var plan = try parsePlan(allocator, plan_json);
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.entries.len);
    const first = plan.find("blk.0.attn_q.weight").?;
    const second = plan.find("token_embd.weight").?;
    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expect(plan.find("missing.weight") == null);
}

test "parse rejects unknown schemas and duplicate names" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedPlanSchema, parsePlan(allocator, "{\"schema\":\"other\",\"tensors\":{}}"));
    try std.testing.expectError(error.UnsupportedSensitivitySchema, parseSensitivity(allocator, "{\"schema\":\"other\",\"metric\":\"m\",\"tensors\":[]}"));
    try std.testing.expectError(error.DuplicateTensorName, parseSensitivity(allocator,
        \\{"schema":"antfly-quant-sensitivity/v1","metric":"m","tensors":[
        \\ {"name":"a","elements":32,"candidates":[{"format":"q8_0","bytes":34,"loss":0}]},
        \\ {"name":"a","elements":32,"candidates":[{"format":"q8_0","bytes":34,"loss":0}]}]}
    ));
}

test "quantizedBytes requires whole blocks" {
    try std.testing.expectEqual(@as(?u64, 2 * 8 * 34), quantizedBytes(2, 256, 32, 34));
    try std.testing.expectEqual(@as(?u64, null), quantizedBytes(2, 100, 32, 34));
}
