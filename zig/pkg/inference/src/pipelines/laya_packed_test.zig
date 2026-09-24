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

//! Tree-packing invariants on a seeded synthetic checkpoint (LAYA.md,
//! "Verification"). These need no fixtures; PyTorch agreement for the same
//! layout is `laya_packed_parity_test.zig`.
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
    tmp: std.testing.TmpDir,
    path: [:0]const u8,
    session: Session,
    cfg: model.Config,
    encoder: modern.Config,

    fn init(a: std.mem.Allocator, packing: ?[]const u8) !Fixture {
        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
        errdefer a.free(path);
        try synthetic.writeModel(a, std.testing.io, path, packing, 128, 717);
        // ANTFLY_LAYA_BACKEND=metal runs the same invariants on Metal.
        const session = try @import("../util/laya_test_support.zig").createSession(a, path);
        errdefer session.close();
        const bytes = try c_file.readFileFromDir(a, path, "config.json");
        defer a.free(bytes);
        const encoder = try modern.parseConfig(a, bytes);
        return .{ .tmp = tmp, .path = path, .session = session, .cfg = factory.getLayaConfig(session).?, .encoder = encoder };
    }
    fn deinit(self: *Fixture, a: std.mem.Allocator) void {
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
        const rows = try tree.build(a, tok, cfg, state_text, &questions);
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
            try std.testing.expectEqual(@as(i64, @intFromEnum(questions[qi].kind)), row.kinds[@intCast(anchor)]);
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
        const split = try tree.build(a, tok, cfg, state_text, &questions);
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
        try std.testing.expectError(error.ExtractionTextLimitExceeded, tree.build(a, tok, cfg, state_text, &questions));
    }
}

test "laya tree validation rejects cycles, crossed markers, and trunk kinds" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const cfg = model.Config{ .max_len = 128, .head_max_len = 48, .packing = .{ .mode = .question, .max_packed_len = 512 } };
    const row = (try tree.build(a, words.tokenizer(), cfg, state_text, &questions))[0];
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
    const mask = [_]i64{1} ** n;
    for (&ids, &positions, &segments, &kinds, 0..) |*id, *p, *s, *k, i| {
        id.* = @intCast((i * 7 + 3) % synthetic.vocab_size);
        p.* = @intCast(i);
        s.* = 0;
        k.* = tree.trunk_kind;
    }
    const row = tree.Row{ .ids = &ids, .positions = &positions, .segments = &segments, .parents = &.{-1}, .kinds = &kinds, .anchors = &.{}, .markers = &.{}, .question_index = &.{}, .width = 0 };
    const global_values = try tree.bias(a, row, null);
    defer a.free(global_values);
    const local_values = try tree.bias(a, row, fixture.encoder.local_attention_window / 2);
    defer a.free(local_values);
    const shape = [_]i32{ n, n };
    const global = try cb.fromFloat32Shape(global_values, &shape);
    defer cb.free(global);
    const local = try cb.fromFloat32Shape(local_values, &shape);
    defer cb.free(local);
    const expected_ct = try modern.forwardCT(&cb, a, fixture.encoder, &ids, &mask, 1, n);
    defer cb.free(expected_ct);
    const actual_ct = try modern.forwardPackedCT(&cb, a, fixture.encoder, &ids, .{ .positions = &positions, .global_bias = global, .local_bias = local });
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

fn trunkEncoding(a: std.mem.Allocator, fixture: *const Fixture, row: tree.Row) ![]f32 {
    const cb = try factory.getComputeBackend(fixture.session, a);
    defer cb.deinit();
    const n = row.ids.len;
    const shape = [_]i32{ @intCast(n), @intCast(n) };
    const global_values = try tree.bias(a, row, null);
    defer a.free(global_values);
    const local_values = try tree.bias(a, row, fixture.encoder.local_attention_window / 2);
    defer a.free(local_values);
    const global = try cb.fromFloat32Shape(global_values, &shape);
    defer cb.free(global);
    const local = try cb.fromFloat32Shape(local_values, &shape);
    defer cb.free(local);
    const encoded = try modern.forwardPackedCT(&cb, a, fixture.encoder, row.ids, .{ .positions = row.positions, .global_bias = global, .local_bias = local });
    defer cb.free(encoded);
    const all = try cb.toFloat32(encoded, a);
    defer a.free(all);
    return a.dupe(f32, all[0 .. @as(usize, @intCast(row.anchors[0])) * synthetic.hidden]);
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
        const all = (try tree.build(a, tok, fixture.cfg, state_text, &questions))[0];
        const together = try runRow(a, &fixture, all);
        const shared = try trunkEncoding(a, &fixture, all);
        var worst: f32 = 0;
        var trunk_worst: f32 = 0;
        for (questions, 0..) |q, qi| {
            const alone = (try tree.build(a, tok, fixture.cfg, state_text, &.{q}))[0];
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
        const other = (try tree.build(a, tok, fixture.cfg, "hello world", &questions))[0];
        const moved = try runRow(a, &fixture, other);
        try std.testing.expect(try maxError(together[0], moved[0]) > 1e-4);
    }
}

test "laya packed trunk cache reuses the state exactly across rows and requests" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    const trunk_cache = @import("../architectures/laya_trunk_cache.zig");
    for ([_][]const u8{ "{\"mode\":\"question\"}", "{\"mode\":\"candidate\"}" }) |packing| {
        var fixture = try Fixture.init(std.testing.allocator, packing);
        defer fixture.deinit(std.testing.allocator);
        var cache = trunk_cache.Cache.init(std.testing.allocator, 64 * 1024 * 1024);
        defer cache.deinit();
        // The synthetic state is short; cache it anyway.
        cache.min_tokens = 1;
        var worst: f32 = 0;
        // Miss (fills the cache), then hits with different question sets.
        for ([_][]const pipeline.Question{ &questions, questions[1..], questions[0..1] }) |subset| {
            const row = (try tree.build(a, tok, fixture.cfg, state_text, subset))[0];
            const full = try runRow(a, &fixture, row);
            // One compute backend at a time: Metal sessions share one provider.
            const cb = try factory.getComputeBackend(fixture.session, std.testing.allocator);
            defer cb.deinit();
            const cached = try packed_arch.forwardRow(&cb, a, fixture.encoder, fixture.cfg, row, &cache);
            worst = @max(worst, try maxError(full[0], cached[0].asFloat32()));
            worst = @max(worst, try maxError(full[1], cached[1].asFloat32()));
        }
        const stats = cache.snapshot();
        std.debug.print("Laya packed {s}: cached vs full max error={d}, hits={d} misses={d} bytes={d}\n", .{ packing, worst, stats.hits, stats.misses, stats.bytes });
        try std.testing.expect(worst < 1e-5);
        try std.testing.expectEqual(@as(u64, 2), stats.hits);
        try std.testing.expectEqual(@as(u64, 1), stats.misses);
    }
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
    try std.testing.expectEqual(@as(usize, 2), result.execution_chunks);
    var unpacked_tokens: usize = 0;
    for (tasks, result.decisions) |task, decision| {
        try std.testing.expectEqualStrings(task.question.name, decision.name);
        const alone = try pipeline.execute(a, fixture.session, tok, fixture.cfg, &.{task}, null);
        unpacked_tokens += alone.prompt_tokens;
        try std.testing.expect(try maxError(alone.decisions[0].probabilities, decision.probabilities) < 1e-5);
        try std.testing.expectApproxEqAbs(alone.decisions[0].act_probability, decision.act_probability, 1e-5);
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const packed_dir = try tmp.dir.realPathFileAlloc(std.testing.io, ".", s);
    var src = try std.Io.Dir.cwd().openDir(std.testing.io, source, .{ .iterate = true });
    defer src.close(std.testing.io);
    var it = src.iterate();
    while (try it.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        const from = try std.fs.path.join(s, &.{ source, entry.name });
        if (std.mem.eql(u8, entry.name, "config.json")) {
            const bytes = try c_file.readFile(s, from);
            const parsed = try std.json.parseFromSlice(std.json.Value, s, bytes, .{});
            var object: std.json.ObjectMap = .empty;
            try object.put(s, "mode", .{ .string = "question" });
            try object.put(s, "max_packed_len", .{ .integer = 8192 });
            try parsed.value.object.getPtr("laya").?.object.put(s, "packing", .{ .object = object });
            try tmp.dir.writeFile(std.testing.io, .{ .sub_path = entry.name, .data = try std.json.Stringify.valueAlloc(s, parsed.value, .{}) });
        } else try src.copyFile(entry.name, tmp.dir, entry.name, std.testing.io, .{});
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
        for ([_]usize{ 1, 2, 4, 8, 16 }) |count| {
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
}
