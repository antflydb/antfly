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

//! Zig tree packing and packed inference against the independent PyTorch
//! oracle `scripts/laya/laya_packed_reference.py`.
const std = @import("std");
const c_file = @import("../util/c_file.zig");
const factory = @import("../architectures/session_factory.zig");
const hf = @import("inference_hf_tokenizer");
const pipeline = @import("laya.zig");
const tree = @import("laya_tree.zig");
const test_support = @import("../util/laya_test_support.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;

const Row = struct {
    ids: []const i64,
    positions: []const i64,
    segments: []const i64,
    parents: []const i64,
    kinds: []const i64,
    anchors: []const i64,
    markers: []const []const i64,
    width: usize,
};
const Case = struct { state: []const u8, row: Row, logits: []const []const f32, action_logits: []const []const f32 };
const Reference = struct {
    modes: struct { question: []const Case, candidate: []const Case },
    questions: []const struct { name: []const u8, t: []const u8, ins: []const u8, labels: []const []const u8, desc: []const []const u8 },
};

test "laya packed rows and decisions match the independent PyTorch oracle" {
    const root = try test_support.fixture("ANTFLY_LAYA_REFERENCE");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    const bytes = c_file.readFile(s, try std.fmt.allocPrint(s, "{s}/packed_reference.json", .{root})) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    const ref = (try std.json.parseFromSlice(Reference, s, bytes, .{ .ignore_unknown_fields = true })).value;
    const questions = try s.alloc(pipeline.Question, ref.questions.len);
    for (questions, ref.questions) |*q, r| q.* = .{ .name = r.name, .kind = std.meta.stringToEnum(@import("../models/laya.zig").QuestionType, r.t).?, .instruction = r.ins, .labels = r.labels, .descriptions = r.desc };
    var worst: f32 = 0;
    inline for (.{ "question", "candidate" }) |mode| {
        const model_path = try std.fmt.allocPrint(s, "{s}/packed-{s}", .{ root, mode });
        var session = try test_support.createSession(a, model_path);
        defer session.close();
        const cfg = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
        const tokenizer = try hf.HfTokenizer.loadFromBytes(a, try c_file.readFileFromDir(s, model_path, "tokenizer.json"));
        const tok = tokenizer.tokenizer();
        defer tok.deinitTokenizer();
        for (@field(ref.modes, mode)) |case| {
            const rows = try tree.build(s, tok, cfg, case.state, questions);
            try std.testing.expectEqual(@as(usize, 1), rows.len);
            const row = rows[0];
            // The packer is a second implementation of the same layout.
            try std.testing.expectEqualSlices(i64, case.row.ids, row.ids);
            try std.testing.expectEqualSlices(i64, case.row.positions, row.positions);
            try std.testing.expectEqualSlices(i64, case.row.segments, row.segments);
            try std.testing.expectEqualSlices(i64, case.row.parents, row.parents);
            try std.testing.expectEqualSlices(i64, case.row.kinds, row.kinds);
            try std.testing.expectEqualSlices(i64, case.row.anchors, row.anchors);
            try std.testing.expectEqual(case.row.width, row.width);
            for (case.row.markers, 0..) |expected, qi| try std.testing.expectEqualSlices(i64, expected, row.markers[qi * row.width ..][0..row.width]);
            // Full rows, then the exact (f32) state cache: a miss followed by a hit.
            factory.setLayaTrunkCachePrecision(session, .f32);
            for ([_]usize{ @import("../architectures/laya_trunk_cache.zig").default_min_tokens, 1, 1 }) |min_tokens| {
                factory.setLayaTrunkCacheLimit(session, 64 * 1024 * 1024, min_tokens);
                const result = try pipeline.execute(s, session, tok, cfg, &.{
                    .{ .text = case.state, .question = questions[0] },
                    .{ .text = case.state, .question = questions[1] },
                    .{ .text = case.state, .question = questions[2] },
                }, null);
                try std.testing.expectEqual(@as(usize, 1), result.execution_chunks);
                for (result.decisions, case.logits, case.action_logits, questions) |decision, logits, actions, q| {
                    // The oracle emits raw logits; compare calibrated probabilities.
                    const expected = try pipeline.decode(s, cfg, q, logits[0..q.labels.len], actions);
                    for (expected.probabilities, decision.probabilities) |want, got| worst = @max(worst, @abs(want - got));
                    worst = @max(worst, @abs(expected.act_probability - decision.act_probability));
                }
            }
            const stats = factory.layaTrunkCacheStats(session).?;
            try std.testing.expect(stats.hits >= 1);
        }
    }
    std.debug.print("Laya packed oracle backend max probability error={d}\n", .{worst});
    try std.testing.expect(worst < 5e-5);
}
