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

//! Tree-packing invariants on a seeded synthetic checkpoint (LAYA.md,
//! "Verification"). These need no fixtures; PyTorch agreement for the same
//! layout is `laya_packed_parity_test.zig`.
const native_platform = @import("antfly_platform");
const std = @import("std");

const model = @import("../models/laya.zig");
const pipeline = @import("laya.zig");
const tree = @import("laya_tree.zig");
const synthetic = @import("../util/laya_synthetic.zig");
const factory = @import("../architectures/session_factory.zig");
const modern = @import("../architectures/modern_bert.zig");
const packed_arch = @import("../architectures/laya_packed.zig");
const c_file = @import("../util/c_file.zig");
const Session = @import("../backends/session.zig").Session;

const state_text = "please find the invoice from acme and check whether the payment is late or already settled";
const questions = [_]pipeline.Question{
    .{ .name = "tool", .kind = .choice, .instruction = "which tool is needed?", .labels = &.{ "search", "fetch", "none" }, .descriptions = &.{ "look it up", "", "" } },
    .{ .name = "urgency", .kind = .score, .instruction = "how urgent is this?", .labels = &.{ "low", "medium", "high" }, .descriptions = &.{ "", "", "" } },
    .{ .name = "late", .kind = .noul, .instruction = "is the payment late?", .labels = &.{ "false", "true" }, .descriptions = &.{ "", "" } },
};

const Fixture = struct {
    tmp: native_platform.testing.TmpDir,
    path: [:0]const u8,
    session: Session,
    cfg: model.Config,
    encoder: modern.Config,

    fn init(a: std.mem.Allocator, packing: ?[]const u8) !Fixture {
        var tmp = native_platform.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const path = try tmp.dir.realPathFileAlloc(native_platform.testing.io, ".", a);
        errdefer a.free(path);
        try synthetic.writeModel(a, native_platform.testing.io, path, packing, 128, 717);
        // ANTFLY_LAYA_BACKEND=metal runs the same invariants on Metal.
        const session = try @import("../util/laya_test_support.zig").createSession(a, path);
        errdefer session.close();
        const bytes = try c_file.readFileFromDir(a, path, "config.json");
        defer a.free(bytes);
        const encoder = try modern.parseConfig(a, bytes);
        return .{ .tmp = tmp, .path = path, .session = session, .cfg = factory.getLayaConfig(session).?, .encoder = encoder };
    }
    pub fn deinit(self: *Fixture, a: std.mem.Allocator) void {
        self.session.close();
        a.free(self.path);
        self.tmp.cleanup();
    }
};

fn maxError(expected: []const f32, actual: []const f32) !f32 {
    try std.testing.expectEqual(expected.len, actual.len);
    var worst: f32 = 0;
    for (expected, actual) |want, got| {
        try std.testing.expect(std.math.isFinite(want) and std.math.isFinite(got));
        worst = @max(worst, @abs(want - got));
    }
    return worst;
}

test "laya tree rows restart positions per branch and isolate siblings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    for ([_]model.PackingMode{ .question, .candidate }) |mode| {
        var cfg = model.Config{ .max_len = 128, .head_max_len = 48, .packing = .{ .mode = mode, .max_packed_len = 512 } };
        const rows = try tree.build(a, tok, cfg, state_text, &questions, null);
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        const row = rows[0];
        try tree.validate(row, cfg.max_len, cfg.packing.max_packed_len, cfg.maxOptions());
        const trunk: usize = @intCast(row.anchors[0]);
        // The trunk is [CLS] state [SEP] at positions 0..trunk-1 and sees only itself.
        for (0..trunk) |i| {
            try std.testing.expectEqual(@as(i64, @intCast(i)), row.positions[i]);
            try std.testing.expectEqual(tree.trunk_kind, row.kinds[i]);
            for (trunk..row.ids.len) |k| try std.testing.expect(!row.visible(i, k));
            for (0..trunk) |k| try std.testing.expect(row.visible(i, k));
        }
        // Every question branch restarts at the end of the trunk.
        for (row.anchors, 0..) |anchor, qi| {
            try std.testing.expectEqual(@as(i64, @intCast(trunk)), row.positions[@intCast(anchor)]);
            try std.testing.expectEqual(@as(i64, @backingInt(questions[qi].kind)), row.kinds[@intCast(anchor)]);
            for (row.anchors, 0..) |other, qj| if (qi != qj) {
                try std.testing.expect(!row.visible(@intCast(anchor), @intCast(other)));
            };
        }
        const markers = row.markers[0..row.width];
        if (mode == .candidate) {
            // Sibling candidates share a start position and never see each other.
            try std.testing.expectEqual(row.positions[@intCast(markers[0])], row.positions[@intCast(markers[1])]);
            try std.testing.expect(!row.visible(@intCast(markers[0]), @intCast(markers[1])));
            try std.testing.expect(row.visible(@intCast(markers[1]), @intCast(row.anchors[0])));
        } else {
            try std.testing.expect(row.visible(@intCast(markers[0]), @intCast(markers[1])));
        }
        // A tight physical budget splits questions across rows; each repeats the trunk.
        cfg.packing.max_packed_len = trunk + (row.ids.len - trunk) / 2;
        const split = try tree.build(a, tok, cfg, state_text, &questions, null);
        try std.testing.expect(split.len >= 2);
        var seen: usize = 0;
        for (split) |part| {
            try std.testing.expectEqualSlices(i64, row.ids[0..trunk], part.ids[0..trunk]);
            for (part.question_index, 0..) |index, i| try std.testing.expectEqual(seen + i, index);
            seen += part.questions();
        }
        try std.testing.expectEqual(questions.len, seen);
        // A state that leaves no logical room for a branch is rejected, not truncated.
        cfg.max_len = trunk + 2;
        try std.testing.expectError(error.ExtractionTextLimitExceeded, tree.build(a, tok, cfg, state_text, &questions, null));
    }
}

test "laya tree style override builds a joint branch under a candidate-mode config" {
    // Two-stage choice (LAYA.md, roadmap 2b) needs a candidate-packed
    // checkpoint to also serve one joint (question-style) branch over a
    // shortlist of finalists. `build`'s style override must reproduce
    // exactly what a pure question-mode config would build for the same
    // questions, byte for byte.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const candidate_cfg = model.Config{ .max_len = 128, .head_max_len = 48, .packing = .{ .mode = .candidate, .max_packed_len = 512 } };
    const question_cfg = model.Config{ .max_len = 128, .head_max_len = 48, .packing = .{ .mode = .question, .max_packed_len = 512 } };
    const joint = (try tree.build(a, tok, candidate_cfg, state_text, &questions, .question))[0];
    const plain = (try tree.build(a, tok, question_cfg, state_text, &questions, null))[0];
    try std.testing.expectEqualSlices(i64, plain.ids, joint.ids);
    try std.testing.expectEqualSlices(i64, plain.positions, joint.positions);
    try std.testing.expectEqualSlices(i64, plain.segments, joint.segments);
    try std.testing.expectEqualSlices(i64, plain.parents, joint.parents);
    try std.testing.expectEqualSlices(i64, plain.kinds, joint.kinds);
    try std.testing.expectEqualSlices(i64, plain.anchors, joint.anchors);
    try std.testing.expectEqualSlices(i64, plain.markers, joint.markers);
    try std.testing.expectEqual(plain.width, joint.width);
    // The default style still follows the config's own mode.
    const default_style = (try tree.build(a, tok, candidate_cfg, state_text, &questions, null))[0];
    try std.testing.expect(!std.mem.eql(i64, plain.ids, default_style.ids));
}

test "laya tree validation rejects cycles, crossed markers, and trunk kinds" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const cfg = model.Config{ .max_len = 128, .head_max_len = 48, .packing = .{ .mode = .question, .max_packed_len = 512 } };
    const row = (try tree.build(a, words.tokenizer(), cfg, state_text, &questions, null))[0];
    try tree.validate(row, cfg.max_len, cfg.packing.max_packed_len, cfg.maxOptions());
    var bad = try tree.own(a, row);
    @constCast(bad.parents)[1] = 2;
    try std.testing.expectError(error.InvalidLayaPackedRow, tree.validate(bad, cfg.max_len, cfg.packing.max_packed_len, cfg.maxOptions()));
    bad = try tree.own(a, row);
    // A marker of question 0 moved into question 1's branch.
    @constCast(bad.markers)[0] = row.markers[row.width];
    try std.testing.expectError(error.InvalidLayaPackedRow, tree.validate(bad, cfg.max_len, cfg.packing.max_packed_len, cfg.maxOptions()));
    bad = try tree.own(a, row);
    @constCast(bad.kinds)[0] = 0;
    try std.testing.expectError(error.InvalidLayaPackedRow, tree.validate(bad, cfg.max_len, cfg.packing.max_packed_len, cfg.maxOptions()));
    bad = try tree.own(a, row);
    @constCast(bad.positions)[1] = @intCast(cfg.max_len);
    try std.testing.expectError(error.InvalidLayaPackedRow, tree.validate(bad, cfg.max_len, cfg.packing.max_packed_len, cfg.maxOptions()));
}

test "laya packed encoder on a one-segment tree reproduces the unpacked encoder" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a, "{\"mode\":\"question\"}");
    defer fixture.deinit(a);
    const cb = try factory.getComputeBackend(fixture.session, a);
    defer cb.deinit();
    const n = 40;
    var ids: [n]i64 = undefined;
    var positions: [n]i64 = undefined;
    var segments: [n]i64 = undefined;
    var kinds: [n]i64 = undefined;
    const mask = @as([n]i64, @splat(1));
    for (&ids, &positions, &segments, &kinds, 0..) |*id, *p, *s, *k, i| {
        id.* = @intCast((i * 7 + 3) % synthetic.vocab_size);
        p.* = @intCast(i);
        s.* = 0;
        k.* = tree.trunk_kind;
    }
    const row = tree.Row{ .ids = &ids, .positions = &positions, .segments = &segments, .parents = &.{-1}, .kinds = &kinds, .anchors = &.{}, .markers = &.{}, .question_index = &.{}, .width = 0 };
    const segments_row = try packedSegments(a, row);
    defer freeSegments(a, segments_row);
    const expected_ct = try modern.forwardCT(&cb, a, fixture.encoder, &ids, &mask, 1, n);
    defer cb.free(expected_ct);
    const actual_ct = try modern.forwardPackedCT(&cb, a, fixture.encoder, &ids, segments_row);
    defer cb.free(actual_ct);
    const expected = try cb.toFloat32(expected_ct, a);
    defer a.free(expected);
    const actual = try cb.toFloat32(actual_ct, a);
    defer a.free(actual);
    const worst = try maxError(expected, actual);
    std.debug.print("Laya packed one-segment encoder max error={d}\n", .{worst});
    try std.testing.expect(worst < 1e-5);
}

fn runRow(a: std.mem.Allocator, fixture: *const Fixture, row: tree.Row) ![2][]f32 {
    const cb = try factory.getComputeBackend(fixture.session, a);
    defer cb.deinit();
    const outputs = try packed_arch.forwardRow(&cb, a, fixture.encoder, fixture.cfg, row, null);
    defer {
        for (outputs) |*output| output.deinit();
        a.free(outputs);
    }
    return .{ try a.dupe(f32, outputs[0].asFloat32()), try a.dupe(f32, outputs[1].asFloat32()) };
}

fn packedSegments(a: std.mem.Allocator, row: tree.Row) !modern.Packed {
    return .{ .positions = row.positions, .ranges = try tree.ranges(a, row, 0), .key_positions = try tree.positions32(a, row.positions) };
}

fn freeSegments(a: std.mem.Allocator, row: modern.Packed) void {
    a.free(row.ranges);
    a.free(row.key_positions);
}

fn trunkEncoding(a: std.mem.Allocator, fixture: *const Fixture, row: tree.Row) ![]f32 {
    const cb = try factory.getComputeBackend(fixture.session, a);
    defer cb.deinit();
    const segments_row = try packedSegments(a, row);
    defer freeSegments(a, segments_row);
    const encoded = try modern.forwardPackedCT(&cb, a, fixture.encoder, row.ids, segments_row);
    defer cb.free(encoded);
    const all = try cb.toFloat32(encoded, a);
    defer a.free(all);
    return a.dupe(f32, all[0 .. @as(usize, @intCast(row.anchors[0])) * synthetic.hidden]);
}

test "laya segment attention equals dense tree-masked attention on the session backend" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    var fixture = try Fixture.init(std.testing.allocator, "{\"mode\":\"candidate\"}");
    defer fixture.deinit(std.testing.allocator);
    const row = (try tree.build(a, tok, fixture.cfg, state_text, &questions, null))[0];
    const n = row.ids.len;
    const heads = 2;
    const hd = 32;
    const H = heads * hd;
    var prng = std.Random.DefaultPrng.init(5);
    const values = try a.alloc(f32, 3 * n * H);
    for (values) |*x| x.* = prng.random().floatNorm(f32);
    const cb = try factory.getComputeBackend(fixture.session, std.testing.allocator);
    defer cb.deinit();
    const shape = [_]i32{ @intCast(n), H };
    var worst: f32 = 0;
    for ([_]?usize{ null, 4 }) |window| {
        // Dense reference: the full row with the tree bias.
        const bias_values = try tree.bias(a, row, window);
        const bias = try cb.fromFloat32Shape(bias_values, &.{ @intCast(n), @intCast(n) });
        defer cb.free(bias);
        const q = try cb.fromFloat32Shape(values[0 .. n * H], &shape);
        defer cb.free(q);
        const k = try cb.fromFloat32Shape(values[n * H ..][0 .. n * H], &shape);
        defer cb.free(k);
        const v = try cb.fromFloat32Shape(values[2 * n * H ..][0 .. n * H], &shape);
        defer cb.free(v);
        const mask = try a.alloc(i64, n);
        @memset(mask, 1);
        const dense_ct = try cb.scaledDotProductAttention(q, k, v, mask, bias, 1, n, heads, hd);
        defer cb.free(dense_ct);
        const dense = try cb.toFloat32(dense_ct, a);
        // Every query, then only the branch queries against all keys.
        for ([_]usize{ 0, @intCast(row.anchors[0]) }) |first| {
            const queries = n - first;
            const q_rows = try cb.fromFloat32Shape(values[first * H ..][0 .. queries * H], &.{ @intCast(queries), H });
            defer cb.free(q_rows);
            const packed_row: modern.Packed = .{ .positions = row.positions[first..], .ranges = try tree.ranges(a, row, first), .key_positions = try tree.positions32(a, row.positions) };
            const out_ct = try modern.packedAttention(&cb, a, q_rows, k, v, packed_row, if (window) |w| @intCast(w) else std.math.maxInt(u32), queries, n, heads, hd);
            defer cb.free(out_ct);
            const out = try cb.toFloat32(out_ct, a);
            worst = @max(worst, try maxError(dense[first * H ..], out));
        }
    }
    std.debug.print("Laya segment attention ({s}) vs dense max error={d}\n", .{ @tagName(fixture.session.backend()), worst });
    try std.testing.expect(worst < 1e-5);
}

test "laya packed questions are isolated and share one exact trunk encoding" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    for ([_][]const u8{ "{\"mode\":\"question\"}", "{\"mode\":\"candidate\"}" }) |packing| {
        var fixture = try Fixture.init(std.testing.allocator, packing);
        defer fixture.deinit(std.testing.allocator);
        const all = (try tree.build(a, tok, fixture.cfg, state_text, &questions, null))[0];
        const together = try runRow(a, &fixture, all);
        const shared = try trunkEncoding(a, &fixture, all);
        var worst: f32 = 0;
        var trunk_worst: f32 = 0;
        for (questions, 0..) |q, qi| {
            const alone = (try tree.build(a, tok, fixture.cfg, state_text, &.{q}, null))[0];
            const single = try runRow(a, &fixture, alone);
            const labels = q.labels.len;
            worst = @max(worst, try maxError(single[0][0..labels], together[0][qi * all.width ..][0..labels]));
            worst = @max(worst, try maxError(single[1], together[1][qi * fixture.cfg.n_act ..][0..fixture.cfg.n_act]));
            trunk_worst = @max(trunk_worst, try maxError(shared, try trunkEncoding(a, &fixture, alone)));
        }
        std.debug.print("Laya packed {s}: isolation max error={d}, trunk max error={d}\n", .{ packing, worst, trunk_worst });
        try std.testing.expect(worst < 1e-5);
        try std.testing.expect(trunk_worst < 1e-5);
        // Decisions depend on the state: a different trunk changes the logits.
        const other = (try tree.build(a, tok, fixture.cfg, "hello world", &questions, null))[0];
        const moved = try runRow(a, &fixture, other);
        try std.testing.expect(try maxError(together[0], moved[0]) > 1e-4);
    }
}

// A batched row of several states runs as one call while cold; on the CPU,
// once any of its trunks is cached it runs per tree, reusing hot trunks, with
// the same decisions (Metal keeps the batch). Also prints the latency of a
// fully hot batch both ways.
test "laya packed multi-state rows reuse cached trunks and match the batched forward" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    var fixture = try Fixture.init(std.testing.allocator, "{\"mode\":\"question\"}");
    defer fixture.deinit(std.testing.allocator);
    const texts = [_][]const u8{ state_text, "hello world how are you today my friend", "the quick brown fox jumps over the lazy dog again and again", "please escalate this ticket because the customer is very upset" };
    var rows: [texts.len]tree.Row = undefined;
    for (&rows, texts) |*row, text| row.* = (try tree.build(a, tok, fixture.cfg, text, &questions, null))[0];
    const merged = try tree.coalesce(a, &rows);
    const batched = try runRow(a, &fixture, merged.row);
    var cache = @import("../architectures/laya_trunk_cache.zig").Cache.init(std.testing.allocator, 64 << 20);
    cache.min_tokens = 1;
    cache.setPrecision(.f32);
    defer cache.deinit();
    const cb = try factory.getComputeBackend(fixture.session, a);
    defer cb.deinit();
    const Run = struct {
        fn once(alloc: std.mem.Allocator, backend: anytype, fx: *const Fixture, row: tree.Row, store: ?*@import("../architectures/laya_trunk_cache.zig").Cache) ![]f32 {
            const outputs = try packed_arch.forwardRow(backend, alloc, fx.encoder, fx.cfg, row, store);
            defer {
                for (outputs) |*output| output.deinit();
                alloc.free(outputs);
            }
            return alloc.dupe(f32, outputs[0].asFloat32());
        }
    };
    // Cold: one batched call, nothing cached.
    try std.testing.expect(try maxError(batched[0], try Run.once(a, &cb, &fixture, merged.row, &cache)) < 1e-5);
    try std.testing.expectEqual(@as(usize, 0), cache.snapshot().entries);
    // Warm one state. On the CPU the batch now runs per tree, hitting that
    // trunk and filling the rest; on Metal it stays one batched call.
    _ = try Run.once(a, &cb, &fixture, rows[1], &cache);
    try std.testing.expect(try maxError(batched[0], try Run.once(a, &cb, &fixture, merged.row, &cache)) < 1e-5);
    const stats = cache.snapshot();
    if (cb.kind() == .metal) {
        try std.testing.expectEqual(@as(usize, 1), stats.entries);
    } else {
        try std.testing.expectEqual(texts.len, stats.entries);
        try std.testing.expect(stats.hits >= 1);
    }
    // Fully hot: per-tree cached vs one uncached batched call.
    const repeats = 5;
    const platform = @import("antfly_platform");
    const began = platform.time.monotonicNs();
    for (0..repeats) |_| _ = try Run.once(a, &cb, &fixture, merged.row, null);
    const middle = platform.time.monotonicNs();
    for (0..repeats) |_| _ = try Run.once(a, &cb, &fixture, merged.row, &cache);
    const uncached_ns = middle - began;
    const cached_ns = platform.time.monotonicNs() - middle;
    std.debug.print("Laya multi-state hot batch: batched uncached {d} us, per-tree cached {d} us\n", .{ uncached_ns / repeats / 1000, cached_ns / repeats / 1000 });
}

// Question-first positions (`packing.question_first`, LAYA.md): branches
// start at position 0 and the trunk after the question budget; questions stay
// isolated, and the trunk cache reuses the shifted trunk exactly.
test "laya question-first positions put every question before the state" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    for ([_][]const u8{ "{\"mode\":\"question\",\"question_first\":true}", "{\"mode\":\"candidate\",\"question_first\":true}" }) |packing| {
        var fixture = try Fixture.init(std.testing.allocator, packing);
        defer fixture.deinit(std.testing.allocator);
        const all = (try tree.build(a, tok, fixture.cfg, state_text, &questions, null))[0];
        var trunk: usize = 0;
        while (all.segments[trunk] == 0) trunk += 1;
        const offset: i64 = @intCast(tree.trunkOffset(fixture.cfg, trunk));
        for (0..trunk) |i| try std.testing.expectEqual(offset + @as(i64, @intCast(i)), all.positions[i]);
        for (all.anchors) |anchor| try std.testing.expectEqual(@as(i64, 0), all.positions[@intCast(anchor)]);
        for (all.positions[trunk..]) |p| try std.testing.expect(p < offset);
        const together = try runRow(a, &fixture, all);
        var worst: f32 = 0;
        for (questions, 0..) |q, qi| {
            const single = try runRow(a, &fixture, (try tree.build(a, tok, fixture.cfg, state_text, &.{q}, null))[0]);
            worst = @max(worst, try maxError(single[0][0..q.labels.len], together[0][qi * all.width ..][0..q.labels.len]));
        }
        try std.testing.expect(worst < 1e-5);
        // Moving the questions ahead of the state changes the model's view.
        var plain_cfg = fixture.cfg;
        plain_cfg.packing.question_first = false;
        const plain = try runRow(a, &fixture, (try tree.build(a, tok, plain_cfg, state_text, &questions, null))[0]);
        try std.testing.expect(try maxError(plain[0], together[0]) > 1e-4);
        // The trunk cache encodes the state at its shifted positions: a miss
        // fills it, a hit reuses it, and both match the uncached forward.
        var cache = @import("../architectures/laya_trunk_cache.zig").Cache.init(std.testing.allocator, 64 << 20);
        cache.min_tokens = 1;
        cache.setPrecision(.f32);
        defer cache.deinit();
        const cb = try factory.getComputeBackend(fixture.session, a);
        defer cb.deinit();
        var cache_error: f32 = 0;
        for (0..2) |_| {
            const cached = try packed_arch.forwardRow(&cb, a, fixture.encoder, fixture.cfg, all, &cache);
            defer {
                for (cached) |*output| output.deinit();
                a.free(cached);
            }
            cache_error = @max(cache_error, try maxError(together[0], cached[0].asFloat32()));
        }
        const stats = cache.snapshot();
        try std.testing.expectEqual(@as(u64, 1), stats.misses);
        try std.testing.expectEqual(@as(u64, 1), stats.hits);
        std.debug.print("Laya question-first {s}: isolation max error={d}, cached vs uncached={d}\n", .{ packing, worst, cache_error });
        try std.testing.expect(cache_error < 1e-5);
    }
}

// Per-question upper layers (`packing.fuse_layers`, LAYA.md): one tree per
// question, so questions stay isolated; fusing the whole stack is exactly the
// question-aware trunk on each question alone; fusing changes the model.
test "laya per-question upper layers isolate questions and reduce to the question-aware trunk" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const stack = synthetic.layers + 2;
    var aware = try Fixture.init(std.testing.allocator, "{\"mode\":\"question\",\"trunk_sees\":\"questions\"}");
    defer aware.deinit(std.testing.allocator);
    var plain = try Fixture.init(std.testing.allocator, "{\"mode\":\"question\"}");
    defer plain.deinit(std.testing.allocator);
    const unfused = try runRow(a, &plain, (try tree.build(a, tok, plain.cfg, state_text, &questions, null))[0]);
    for ([_]usize{ 2, 3, stack }) |fuse| {
        var fixture = try Fixture.init(std.testing.allocator, try std.fmt.allocPrint(a, "{{\"mode\":\"question\",\"fuse_layers\":{d}}}", .{fuse}));
        defer fixture.deinit(std.testing.allocator);
        const rows = try tree.build(a, tok, fixture.cfg, state_text, &questions, null);
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        const all = rows[0];
        try std.testing.expect(all.fused);
        try std.testing.expectEqual(questions.len, tree.treeCount(all));
        try tree.validate(all, fixture.cfg.max_len, fixture.cfg.packing.max_packed_len, fixture.cfg.maxOptions());
        // Both layer sets' ranges agree with their visibility.
        for ([_]tree.Row{ all, all.upper() }) |view| {
            const bounds = try tree.ranges(a, view, 0);
            for (0..view.ids.len) |q| for (0..view.ids.len) |k| {
                var in_range = false;
                for (0..3) |slot| in_range = in_range or (k >= bounds[q * 6 + 2 * slot] and k < bounds[q * 6 + 2 * slot + 1]);
                try std.testing.expectEqual(view.visible(q, k), in_range);
            };
        }
        const together = try runRow(a, &fixture, all);
        var isolation: f32 = 0;
        var reduction: f32 = 0;
        var moved: f32 = 0;
        for (questions, 0..) |q, qi| {
            const labels = q.labels.len;
            const got = together[0][qi * all.width ..][0..labels];
            const single = try runRow(a, &fixture, (try tree.build(a, tok, fixture.cfg, state_text, &.{q}, null))[0]);
            isolation = @max(isolation, try maxError(single[0][0..labels], got));
            moved = @max(moved, try maxError(unfused[0][qi * all.width ..][0..labels], got));
            if (fuse == stack) {
                const alone = (try tree.build(a, tok, aware.cfg, state_text, &.{q}, null))[0];
                reduction = @max(reduction, try maxError((try runRow(a, &aware, alone))[0][0..labels], got));
            }
        }
        std.debug.print("Laya fuse_layers={d}: isolation max error={d}, vs question-aware trunk={d}, vs unfused={d}\n", .{ fuse, isolation, reduction, moved });
        try std.testing.expect(isolation < 1e-5);
        try std.testing.expect(reduction < 1e-5);
        try std.testing.expect(moved > 1e-4);
    }
}

// Multi-row batching (LAYA.md, "Segment attention"): several rows, from
// different states, run as one physical row in one session call.
extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern fn unsetenv(name: [*:0]const u8) c_int;

test "laya multi-row coalescing isolates independent states and matches running them alone" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const states = [_][]const u8{
        state_text,
        "hello world this is a different and much shorter state",
        "a third state about invoices refunds and shipping delays",
    };
    for ([_][]const u8{ "{\"mode\":\"question\"}", "{\"mode\":\"candidate\"}" }) |packing| {
        var fixture = try Fixture.init(std.testing.allocator, packing);
        defer fixture.deinit(std.testing.allocator);
        var sub_rows: [states.len]tree.Row = undefined;
        var alone: [states.len][2][]f32 = undefined;
        for (&sub_rows, &alone, states) |*row, *result, text| {
            row.* = (try tree.build(a, tok, fixture.cfg, text, &questions, null))[0];
            result.* = try runRow(a, &fixture, row.*);
        }
        const merged = try tree.coalesce(a, &sub_rows);
        defer merged.deinit(a);
        try std.testing.expectEqual(@as(usize, states.len), tree.treeCount(merged.row));
        try tree.validate(merged.row, fixture.cfg.max_len, fixture.cfg.packing.max_packed_len, fixture.cfg.maxOptions());
        // No token of one state's tree is visible from another's.
        var offsets: [states.len]usize = undefined;
        var running: usize = 0;
        for (&offsets, sub_rows) |*offset, row| {
            offset.* = running;
            running += row.ids.len;
        }
        for (sub_rows, 0..) |row, ri| for (0..row.ids.len) |i| for (sub_rows, 0..) |other, rj| {
            if (ri == rj) continue;
            for (0..other.ids.len) |k| try std.testing.expect(!merged.row.visible(offsets[ri] + i, offsets[rj] + k));
        };
        // A batched call over all three states equals each row run alone.
        const together = try runRow(a, &fixture, merged.row);
        var worst: f32 = 0;
        for (merged.row.question_index, merged.owners, 0..) |local, owner, k| {
            const q = questions[local];
            const labels = q.labels.len;
            worst = @max(worst, try maxError(alone[owner][0][local * sub_rows[owner].width ..][0..labels], together[0][k * merged.row.width ..][0..labels]));
            worst = @max(worst, try maxError(alone[owner][1][local * fixture.cfg.n_act ..][0..fixture.cfg.n_act], together[1][k * fixture.cfg.n_act ..][0..fixture.cfg.n_act]));
        }
        std.debug.print("Laya multi-row batching {s}: isolation and exactness max error={d}\n", .{ packing, worst });
        try std.testing.expect(worst < 1e-5);
    }
}

test "laya multi-row coalescing holds exactly with a dozen near-identical states" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    var fixture = try Fixture.init(std.testing.allocator, "{\"mode\":\"question\"}");
    defer fixture.deinit(std.testing.allocator);
    const state_count = 12;
    var sub_rows: [state_count]tree.Row = undefined;
    var alone: [state_count][2][]f32 = undefined;
    for (&sub_rows, &alone, 0..) |*row, *result, i| {
        const text = try std.fmt.allocPrint(a, "{s} case number {d}", .{ state_text, i });
        row.* = (try tree.build(a, tok, fixture.cfg, text, questions[0..2], null))[0];
        result.* = try runRow(a, &fixture, row.*);
    }
    const merged = try tree.coalesce(a, &sub_rows);
    defer merged.deinit(a);
    try std.testing.expectEqual(@as(usize, state_count), tree.treeCount(merged.row));
    // This directly exercises forwardRow, bypassing the session's own
    // max_packed_len budget (pipelines/laya.zig enforces that when grouping
    // rows into batches); validate the row's structure against its own size.
    try tree.validate(merged.row, fixture.cfg.max_len, merged.row.ids.len, fixture.cfg.maxOptions());
    const together = try runRow(a, &fixture, merged.row);
    var worst: f32 = 0;
    for (merged.row.question_index, merged.owners, 0..) |local, owner, k| {
        const q = questions[0..2][local];
        const labels = q.labels.len;
        worst = @max(worst, try maxError(alone[owner][0][local * sub_rows[owner].width ..][0..labels], together[0][k * merged.row.width ..][0..labels]));
        worst = @max(worst, try maxError(alone[owner][1][local * fixture.cfg.n_act ..][0..fixture.cfg.n_act], together[1][k * fixture.cfg.n_act ..][0..fixture.cfg.n_act]));
    }
    std.debug.print("Laya multi-row batching, {d} near-identical states: max error={d}\n", .{ state_count, worst });
    try std.testing.expect(worst < 1e-5);
}

test "laya packed pipeline batches many small states into fewer session calls" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    var fixture = try Fixture.init(std.testing.allocator, "{\"mode\":\"question\"}");
    defer fixture.deinit(std.testing.allocator);
    const state_count = 12;
    var tasks: std.ArrayListUnmanaged(pipeline.Task) = .empty;
    for (0..state_count) |i| {
        const text = try std.fmt.allocPrint(a, "{s} case number {d}", .{ state_text, i });
        for (questions[0..2]) |q| try tasks.append(a, .{ .text = text, .question = q });
    }
    const result = try pipeline.execute(a, fixture.session, tok, fixture.cfg, tasks.items, null);
    try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_LAYA_PACKED_BATCH", "0", 1));
    const unbatched = try pipeline.execute(a, fixture.session, tok, fixture.cfg, tasks.items, null);
    try std.testing.expectEqual(@as(c_int, 0), unsetenv("ANTFLY_LAYA_PACKED_BATCH"));
    std.debug.print("Laya multi-row batching: {d} states, batched chunks={d}, unbatched chunks={d}\n", .{ state_count, result.execution_chunks, unbatched.execution_chunks });
    // Every state fits its own row well under the budget, so disabling
    // batching falls back to one call per state; batching shares fewer calls
    // across them without changing tokens processed or any decision.
    try std.testing.expectEqual(@as(usize, state_count), unbatched.execution_chunks);
    try std.testing.expect(result.execution_chunks < unbatched.execution_chunks);
    try std.testing.expectEqual(unbatched.prompt_tokens, result.prompt_tokens);
    var worst_prob: f32 = 0;
    var worst_act: f32 = 0;
    for (result.decisions, unbatched.decisions) |left, right| {
        worst_prob = @max(worst_prob, try maxError(left.probabilities, right.probabilities));
        worst_act = @max(worst_act, @abs(left.act_probability.? - right.act_probability.?));
    }
    std.debug.print("Laya multi-row batching: {d} states, worst probability error={d}, worst act error={d}\n", .{ state_count, worst_prob, worst_act });
    try std.testing.expect(worst_prob < 1e-5);
    try std.testing.expect(worst_act < 1e-5);
}

test "laya packed trunk cache reuses the state exactly across rows and requests" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const trunk_cache = @import("../architectures/laya_trunk_cache.zig");
    for ([_][]const u8{ "{\"mode\":\"question\"}", "{\"mode\":\"candidate\"}" }) |packing| for ([_]trunk_cache.Precision{ .f32, .f16 }) |precision| {
        var fixture = try Fixture.init(std.testing.allocator, packing);
        defer fixture.deinit(std.testing.allocator);
        var cache = trunk_cache.Cache.init(std.testing.allocator, 64 * 1024 * 1024);
        defer cache.deinit();
        // The synthetic state is short; cache it anyway.
        cache.min_tokens = 1;
        cache.setPrecision(precision);
        var worst: f32 = 0;
        // Miss (fills the cache), then hits with different question sets.
        for ([_][]const pipeline.Question{ &questions, questions[1..], questions[0..1] }) |subset| {
            const row = (try tree.build(a, tok, fixture.cfg, state_text, subset, null))[0];
            const full = try runRow(a, &fixture, row);
            // One compute backend at a time: Metal sessions share one provider.
            const cb = try factory.getComputeBackend(fixture.session, std.testing.allocator);
            defer cb.deinit();
            const cached = try packed_arch.forwardRow(&cb, a, fixture.encoder, fixture.cfg, row, &cache);
            worst = @max(worst, try maxError(full[0], cached[0].asFloat32()));
            worst = @max(worst, try maxError(full[1], cached[1].asFloat32()));
        }
        const stats = cache.snapshot();
        std.debug.print("Laya packed {s} {s}: cached vs full max error={d}, hits={d} misses={d} bytes={d}\n", .{ packing, @tagName(precision), worst, stats.hits, stats.misses, stats.bytes });
        // f32 entries are exact; f16 entries round the trunk keys and values.
        try std.testing.expect(worst < if (precision == .f32) @as(f32, 1e-5) else 5e-3);
        try std.testing.expectEqual(@as(u64, 2), stats.hits);
        try std.testing.expectEqual(@as(u64, 1), stats.misses);
    };
}

test "laya packed session caches the trunk across pipeline requests" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    var fixture = try Fixture.init(std.testing.allocator, "{\"mode\":\"question\"}");
    defer fixture.deinit(std.testing.allocator);
    factory.setLayaTrunkCacheLimit(fixture.session, 64 * 1024 * 1024, 1);
    factory.setLayaTrunkCachePrecision(fixture.session, .f32);
    const first = try pipeline.execute(a, fixture.session, tok, fixture.cfg, &.{.{ .text = state_text, .question = questions[0] }}, null);
    const second = try pipeline.execute(a, fixture.session, tok, fixture.cfg, &.{ .{ .text = state_text, .question = questions[1] }, .{ .text = state_text, .question = questions[0] } }, null);
    const stats = factory.layaTrunkCacheStats(fixture.session).?;
    try std.testing.expectEqual(@as(u64, 1), stats.misses);
    try std.testing.expectEqual(@as(u64, 1), stats.hits);
    try std.testing.expect(try maxError(first.decisions[0].probabilities, second.decisions[1].probabilities) < 1e-5);
    // A disabled cache recomputes the trunk and returns the same decisions.
    factory.setLayaTrunkCacheLimit(fixture.session, 0, 0);
    const uncached = try pipeline.execute(a, fixture.session, tok, fixture.cfg, &.{.{ .text = state_text, .question = questions[0] }}, null);
    try std.testing.expectEqual(stats.hits, factory.layaTrunkCacheStats(fixture.session).?.hits);
    try std.testing.expect(try maxError(first.decisions[0].probabilities, uncached.decisions[0].probabilities) < 1e-5);
}

test "laya packed pipeline groups shared states and preserves request order" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    var fixture = try Fixture.init(std.testing.allocator, "{\"mode\":\"question\"}");
    defer fixture.deinit(std.testing.allocator);
    const tasks = [_]pipeline.Task{
        .{ .text = state_text, .question = questions[0] },
        .{ .text = "hello world", .question = questions[1] },
        .{ .text = state_text, .question = questions[2] },
        .{ .text = state_text, .question = questions[1] },
    };
    const result = try pipeline.execute(a, fixture.session, tok, fixture.cfg, &tasks, null);
    // Two states, each one row well under the 512-token budget: multi-row
    // batching (LAYA.md, "Segment attention") shares one session call.
    try std.testing.expectEqual(@as(usize, 1), result.execution_chunks);
    var unpacked_tokens: usize = 0;
    for (tasks, result.decisions) |task, decision| {
        try std.testing.expectEqualStrings(task.question.name, decision.name);
        const alone = try pipeline.execute(a, fixture.session, tok, fixture.cfg, &.{task}, null);
        unpacked_tokens += alone.prompt_tokens;
        try std.testing.expect(try maxError(alone.decisions[0].probabilities, decision.probabilities) < 1e-5);
        try std.testing.expectApproxEqAbs(alone.decisions[0].act_probability.?, decision.act_probability.?, 1e-5);
    }
    // The shared state is encoded once instead of once per question.
    try std.testing.expect(result.prompt_tokens < unpacked_tokens);
    // Splitting the same request across rows does not change any decision.
    var split_cfg = fixture.cfg;
    split_cfg.packing.max_packed_len = 64;
    const split = try pipeline.execute(a, fixture.session, tok, split_cfg, &tasks, null);
    try std.testing.expect(split.execution_chunks > result.execution_chunks);
    for (split.decisions, result.decisions) |left, right| try std.testing.expect(try maxError(left.probabilities, right.probabilities) < 1e-5);
}

test "laya two-stage choice blends a joint shortlist back into the stage-1 distribution" {
    // LAYA.md, "Two-stage choice (roadmap 2b)". Reproduces stage 1 (the plain
    // candidate row) and stage 2 (a joint branch over the two highest-probability
    // options) independently, then checks `pipeline.execute`'s blended result
    // against the mixture formula by hand.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    var fixture = try Fixture.init(std.testing.allocator, "{\"mode\":\"candidate\",\"two_stage\":{\"top_k\":2}}");
    defer fixture.deinit(std.testing.allocator);
    const q = questions[0]; // "tool": choice, 3 labels.
    try std.testing.expect(q.labels.len > fixture.cfg.packing.two_stage.top_k);

    const stage1_row = (try tree.build(a, tok, fixture.cfg, state_text, &.{q}, null))[0];
    const stage1_raw = try runRow(a, &fixture, stage1_row);
    const stage1 = try pipeline.decode(a, fixture.cfg, q, stage1_raw[0][0..q.labels.len], stage1_raw[1][0..fixture.cfg.n_act]);

    // The two highest stage-1 probabilities are the finalists, ascending by index.
    var order = [_]usize{ 0, 1, 2 };
    std.mem.sort(usize, &order, stage1.probabilities, struct {
        fn more(probs: []const f32, l: usize, r: usize) bool {
            return probs[l] > probs[r];
        }
    }.more);
    var finalists = order[0..2].*;
    std.mem.sort(usize, &finalists, {}, std.sort.asc(usize));

    const labels = try a.alloc([]const u8, 2);
    const descriptions = try a.alloc([]const u8, 2);
    for (finalists, labels, descriptions) |idx, *l, *d| {
        l.* = q.labels[idx];
        d.* = q.descriptions[idx];
    }
    const shortlist = pipeline.Question{ .name = q.name, .kind = q.kind, .instruction = q.instruction, .labels = labels, .descriptions = descriptions };
    const stage2_row = (try tree.build(a, tok, fixture.cfg, state_text, &.{shortlist}, .question))[0];
    const stage2_raw = try runRow(a, &fixture, stage2_row);
    const stage2 = try pipeline.decode(a, fixture.cfg, shortlist, stage2_raw[0][0..2], stage2_raw[1][0..fixture.cfg.n_act]);

    var mass: f32 = 0;
    for (finalists) |idx| mass += stage1.probabilities[idx];
    const expected = try a.dupe(f32, stage1.probabilities);
    for (finalists, 0..) |idx, j| expected[idx] = mass * stage2.probabilities[j];

    const result = try pipeline.execute(a, fixture.session, tok, fixture.cfg, &.{.{ .text = state_text, .question = q }}, null);
    try std.testing.expectEqual(@as(usize, 1), result.decisions.len);
    const worst = try maxError(expected, result.decisions[0].probabilities);
    std.debug.print("Laya two-stage choice: blended vs manually-computed max error={d}, mass captured={d}\n", .{ worst, mass });
    try std.testing.expect(worst < 1e-5);
    var sum: f32 = 0;
    for (result.decisions[0].probabilities) |p| sum += p;
    try std.testing.expectApproxEqAbs(@as(f32, 1), sum, 1e-5);
    // Each input token counts once: stage 2 adds its joint branch, not the
    // state stage 1 already counted.
    var stage2_branch: usize = 0;
    for (stage2_row.kinds) |kind| stage2_branch += @intFromBool(kind != tree.trunk_kind);
    try std.testing.expectEqual(stage1_row.ids.len + stage2_branch, result.prompt_tokens);

    // With top_k covering every option, stage 2 never runs and nothing changes.
    var single_stage_cfg = fixture.cfg;
    single_stage_cfg.packing.two_stage.top_k = q.labels.len;
    const single_stage = try pipeline.execute(a, fixture.session, tok, single_stage_cfg, &.{.{ .text = state_text, .question = q }}, null);
    try std.testing.expect(try maxError(stage1.probabilities, single_stage.decisions[0].probabilities) < 1e-5);
}

test "laya packed session rejects rows that break the tree contract" {
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a, "{\"mode\":\"question\"}");
    defer fixture.deinit(a);
    // Model loading skips resident Metal admission for packed checkpoints.
    try std.testing.expect(factory.isPackedLayaModel(a, fixture.path));
    var unpacked = try Fixture.init(a, null);
    defer unpacked.deinit(a);
    try std.testing.expect(!factory.isPackedLayaModel(a, unpacked.path));
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var inputs = [_]Tensor{
        try Tensor.initInt64(a, "input_ids", &.{ 1, 4 }, &.{ 2, 5, 3, 4 }),
        try Tensor.initInt64(a, "position_ids", &.{ 1, 4 }, &.{ 0, 1, 2, 2 }),
        try Tensor.initInt64(a, "token_segment", &.{ 1, 4 }, &.{ 0, 0, 1, 1 }),
        try Tensor.initInt64(a, "segment_parent", &.{ 1, 2 }, &.{ -1, 0 }),
        try Tensor.initInt64(a, "token_qtype", &.{ 1, 4 }, &.{ -1, -1, 0, 0 }),
        // The first marker is a trunk token, which cannot belong to a question.
        try Tensor.initInt64(a, "marker_pos", &.{ 1, 2 }, &.{ 1, 3 }),
        try Tensor.initInt64(a, "anchor_pos", &.{ 1, 1 }, &.{2}),
    };
    defer for (&inputs) |*input| input.deinit();
    try std.testing.expectError(error.InvalidLayaPackedRow, fixture.session.run(&inputs, a));
}

// Cost of shared-state questions, packed vs one sequence per question.
// Packed decisions are meaningless here (the weights are not fine-tuned for
// packing); only latency and processed tokens are measured. Set
// ANTFLY_LAYA_PACKED_BENCH to a prepared Laya directory and build ReleaseFast.
test "laya packed benchmark shared-state cost against unpacked" {
    const platform = @import("antfly_platform");
    const source = platform.env.getenv("ANTFLY_LAYA_PACKED_BENCH") orelse return error.SkipZigTest;
    const hf = @import("inference_hf_tokenizer");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const packed_dir = try tmp.dir.realPathFileAlloc(native_platform.testing.io, ".", s);
    var src = try std.Io.Dir.cwd().openDir(native_platform.testing.io, source, .{ .iterate = true });
    defer src.close(native_platform.testing.io);
    var it = src.iterate();
    while (try it.next(native_platform.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        const from = try std.fs.path.join(s, &.{ source, entry.name });
        if (std.mem.eql(u8, entry.name, "config.json")) {
            const bytes = try c_file.readFile(s, from);
            const parsed = try std.json.parseFromSlice(std.json.Value, s, bytes, .{});
            var object: std.json.ObjectMap = .empty;
            try object.put(s, "mode", .{ .string = "question" });
            try object.put(s, "max_packed_len", .{ .integer = 8192 });
            try parsed.value.object.getPtr("laya").?.object.put(s, "packing", .{ .object = object });
            try tmp.dir.writeFile(native_platform.testing.io, .{ .sub_path = entry.name, .data = try std.json.Stringify.valueAlloc(s, parsed.value, .{}) });
        } else try src.copyFile(entry.name, tmp.dir, entry.name, native_platform.testing.io, .{});
    }
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, try c_file.readFileFromDir(s, source, "tokenizer.json"));
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    const sentence = "The customer reports that invoice 4471 from Acme Logistics was charged twice this month, and the second charge overlaps with a refund that support promised last week. ";
    const bench_questions = [_]pipeline.Question{
        .{ .name = "q", .kind = .choice, .instruction = "which team should handle this ticket?", .labels = &.{ "billing", "shipping", "technical", "sales" }, .descriptions = &.{ "", "", "", "" } },
        .{ .name = "q", .kind = .noul, .instruction = "is a refund required?", .labels = &.{ "false", "true" }, .descriptions = &.{ "", "" } },
        .{ .name = "q", .kind = .score, .instruction = "how urgent is the ticket?", .labels = &.{ "low", "medium", "high" }, .descriptions = &.{ "", "", "" } },
    };
    const backend = @import("../util/laya_test_support.zig");
    var unpacked = try backend.createSession(a, source);
    defer unpacked.close();
    var packed_session = try backend.createSession(a, packed_dir);
    defer packed_session.close();
    const unpacked_cfg = factory.getLayaConfig(unpacked).?;
    const packed_cfg = factory.getLayaConfig(packed_session).?;
    const samples: usize = 5;
    const trunk_cache_min = @import("../architectures/laya_trunk_cache.zig").default_min_tokens;
    for ([_]usize{ 1, 4, 12 }) |sentences| {
        const text = try s.alloc(u8, sentence.len * sentences);
        for (0..sentences) |i| @memcpy(text[i * sentence.len ..][0..sentence.len], sentence);
        for ([_]usize{ 1, 2, 4, 8, 16, 64 }) |count| {
            const tasks = try s.alloc(pipeline.Task, count);
            for (tasks, 0..) |*task, i| task.* = .{ .text = text, .question = bench_questions[i % bench_questions.len] };
            var medians: [2]u64 = undefined;
            var tokens: [2]usize = undefined;
            for ([_]Session{ unpacked, packed_session }, [_]model.Config{ unpacked_cfg, packed_cfg }, 0..) |session, cfg, which| {
                var times: [samples + 1]u64 = undefined;
                for (&times) |*t| {
                    var request = std.heap.ArenaAllocator.init(a);
                    defer request.deinit();
                    const began = platform.time.monotonicNs();
                    const result = try pipeline.executeWithScratch(request.allocator(), a, session, tok, cfg, tasks, null, null);
                    t.* = platform.time.monotonicNs() - began;
                    tokens[which] = result.prompt_tokens;
                }
                std.mem.sort(u64, times[1..], {}, std.sort.asc(u64));
                medians[which] = times[1 + samples / 2];
            }
            // The packed timings above hit the trunk cache after the first
            // request. Measure the uncached packed cost separately.
            factory.setLayaTrunkCacheLimit(packed_session, 0, trunk_cache_min);
            var uncached: [samples + 1]u64 = undefined;
            for (&uncached) |*t| {
                var request = std.heap.ArenaAllocator.init(a);
                defer request.deinit();
                const began = platform.time.monotonicNs();
                _ = try pipeline.executeWithScratch(request.allocator(), a, packed_session, tok, packed_cfg, tasks, null, null);
                t.* = platform.time.monotonicNs() - began;
            }
            factory.setLayaTrunkCacheLimit(packed_session, 1024 * 1024 * 1024, trunk_cache_min);
            std.mem.sort(u64, uncached[1..], {}, std.sort.asc(u64));
            std.debug.print("LAYA_PACKED_BENCH {{\"backend\":\"{s}\",\"state_sentences\":{d},\"questions\":{d},\"unpacked_ms\":{d:.1},\"packed_ms\":{d:.1},\"packed_cached_ms\":{d:.1},\"unpacked_tokens\":{d},\"packed_tokens\":{d}}}\n", .{
                @tagName(unpacked.backend()),              sentences,                                                count,
                @as(f64, @floatFromInt(medians[0])) / 1e6, @as(f64, @floatFromInt(uncached[1 + samples / 2])) / 1e6, @as(f64, @floatFromInt(medians[1])) / 1e6,
                tokens[0],                                 tokens[1],
            });
        }
    }
    // Many-states case (step 1b'): a request with many distinct short states
    // instead of many questions about one state. Multi-row batching (LAYA.md,
    // "Segment attention") amortizes per-call overhead across states; compare
    // against ANTFLY_LAYA_PACKED_BATCH=0, which falls back to one call per row.
    const many_state_questions = 4;
    for ([_]usize{ 16, 64 }) |state_count| {
        const tasks = try s.alloc(pipeline.Task, state_count * many_state_questions);
        for (0..state_count) |si| {
            const text = try std.fmt.allocPrint(s, "{s} case {d}", .{ sentence, si });
            for (0..many_state_questions) |qi| tasks[si * many_state_questions + qi] = .{ .text = text, .question = bench_questions[qi % bench_questions.len] };
        }
        var unpacked_time: [samples + 1]u64 = undefined;
        var unpacked_tokens: usize = 0;
        for (&unpacked_time) |*t| {
            var request = std.heap.ArenaAllocator.init(a);
            defer request.deinit();
            const began = platform.time.monotonicNs();
            const result = try pipeline.executeWithScratch(request.allocator(), a, unpacked, tok, unpacked_cfg, tasks, null, null);
            t.* = platform.time.monotonicNs() - began;
            unpacked_tokens = result.prompt_tokens;
        }
        std.mem.sort(u64, unpacked_time[1..], {}, std.sort.asc(u64));
        var batched_time: [samples + 1]u64 = undefined;
        var batched_chunks: usize = 0;
        var batched_tokens: usize = 0;
        for (&batched_time) |*t| {
            var request = std.heap.ArenaAllocator.init(a);
            defer request.deinit();
            const began = platform.time.monotonicNs();
            const result = try pipeline.executeWithScratch(request.allocator(), a, packed_session, tok, packed_cfg, tasks, null, null);
            t.* = platform.time.monotonicNs() - began;
            batched_chunks = result.execution_chunks;
            batched_tokens = result.prompt_tokens;
        }
        std.mem.sort(u64, batched_time[1..], {}, std.sort.asc(u64));
        try std.testing.expectEqual(@as(c_int, 0), setenv("ANTFLY_LAYA_PACKED_BATCH", "0", 1));
        var unbatched_time: [samples + 1]u64 = undefined;
        var unbatched_chunks: usize = 0;
        for (&unbatched_time) |*t| {
            var request = std.heap.ArenaAllocator.init(a);
            defer request.deinit();
            const began = platform.time.monotonicNs();
            const result = try pipeline.executeWithScratch(request.allocator(), a, packed_session, tok, packed_cfg, tasks, null, null);
            t.* = platform.time.monotonicNs() - began;
            unbatched_chunks = result.execution_chunks;
        }
        std.mem.sort(u64, unbatched_time[1..], {}, std.sort.asc(u64));
        try std.testing.expectEqual(@as(c_int, 0), unsetenv("ANTFLY_LAYA_PACKED_BATCH"));
        std.debug.print("LAYA_PACKED_MANY_STATES {{\"backend\":\"{s}\",\"states\":{d},\"questions_per_state\":{d},\"unpacked_ms\":{d:.1},\"packed_batched_ms\":{d:.1},\"packed_unbatched_ms\":{d:.1},\"batched_chunks\":{d},\"unbatched_chunks\":{d},\"unpacked_tokens\":{d},\"packed_tokens\":{d}}}\n", .{
            @tagName(unpacked.backend()),                                 state_count,
            many_state_questions,                                         @as(f64, @floatFromInt(unpacked_time[1 + samples / 2])) / 1e6,
            @as(f64, @floatFromInt(batched_time[1 + samples / 2])) / 1e6, @as(f64, @floatFromInt(unbatched_time[1 + samples / 2])) / 1e6,
            batched_chunks,                                               unbatched_chunks,
            unpacked_tokens,                                              batched_tokens,
        });
    }
}
