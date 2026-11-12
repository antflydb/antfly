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

//! GLiDE prompt and token parity against the Python reference capture.
//!
//! `ANTFLY_GLIDE_REFERENCE` names a directory of reference captures
//! (`fix04_cpu_fp32.json`, `fix08_cpu_fp32.json`, `fix2b_cpu_fp32.json`,
//! `trunc04_tokens.json`; absent captures are skipped): each holds the
//! reference `requests` (Fastino format), the checkpoint `model_dir`, and
//! per prompt the exact `text` and token `ids`, in `decide_batch` order.
//! `ANTFLY_GLIDE_MODELS_DIR` optionally replaces the recorded checkpoint
//! parent directory. Skipped without the variable unless
//! `ANTFLY_GLIDE_REQUIRE_TESTS` is set.

const std = @import("std");
const platform = @import("antfly_platform");
const c_file = @import("../util/c_file.zig");
const hf = @import("inference_hf_tokenizer");
const glide = @import("../models/glide.zig");
const prompt = @import("glide_prompt.zig");

const Capture = struct {
    model_dir: []const u8,
    requests: std.json.Value,
    prompts: []const struct { text: []const u8, ids: []const i64, n: usize },
};

/// `trunc04_tokens.json` holds over-long encoder prompts (head/tail
/// truncation) with whitespace runs that hit ModernBERT's added space tokens.
pub const fixture_names = [_][]const u8{ "fix04_cpu_fp32.json", "fix08_cpu_fp32.json", "fix2b_cpu_fp32.json", "trunc04_tokens.json" };

pub fn referenceDir() ![]const u8 {
    return platform.env.getenv("ANTFLY_GLIDE_REFERENCE") orelse {
        if (platform.env.getenvBoolDefault("ANTFLY_GLIDE_REQUIRE_TESTS", false)) return error.MissingGlideFixture;
        return error.SkipZigTest;
    };
}

/// The checkpoint directory a capture was recorded against.
pub fn captureModelDir(a: std.mem.Allocator, recorded: []const u8) ![]const u8 {
    const override = platform.env.getenv("ANTFLY_GLIDE_MODELS_DIR") orelse return a.dupe(u8, recorded);
    return std.fs.path.join(a, &.{ override, std.fs.path.basename(recorded) });
}

test "GLiDE prompts and token ids match the Python reference exactly" {
    const root = try referenceDir();
    const gpa = std.testing.allocator;
    var checked: usize = 0;
    for (fixture_names) |name| {
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        const a = arena.allocator();
        const path = try std.fs.path.join(a, &.{ root, name });
        if (!c_file.fileExists(a, path)) continue;
        const bytes = try c_file.readFileMax(a, path, 256 * 1024 * 1024);
        const capture = try std.json.parseFromSliceLeaky(Capture, a, bytes, .{ .ignore_unknown_fields = true });
        const model_dir = try captureModelDir(a, capture.model_dir);
        var config = try glide.loadDecisionConfig(a, model_dir);
        defer config.deinit(a);
        const tokenizer_bytes = try c_file.readFileFromDir(a, model_dir, "tokenizer.json");
        const tokenizer = try hf.HfTokenizer.loadFromBytes(gpa, tokenizer_bytes);
        const tok = tokenizer.tokenizer();
        defer tok.deinitTokenizer();

        if (capture.requests != .array) return error.InvalidGlideFixture;
        var index: usize = 0;
        var text_ok: usize = 0;
        var ids_ok: usize = 0;
        for (capture.requests.array.items) |request| {
            const item = try prompt.parseSystemOne(a, request);
            for (item.questions) |question| {
                if (index >= capture.prompts.len) return error.InvalidGlideFixture;
                const expected = capture.prompts[index];
                const text = try prompt.render(a, config.codes, item.state, question);
                if (std.mem.eql(u8, text, expected.text)) text_ok += 1 else std.debug.print("{s} prompt {d}: text differs\n", .{ name, index });
                try std.testing.expectEqual(expected.n, question.options.len);
                const ids = try prompt.encode(a, tok, &config, expected.text);
                var first: usize = 0;
                while (first < @min(ids.len, expected.ids.len) and ids[first] == expected.ids[first]) first += 1;
                if (first == ids.len and ids.len == expected.ids.len) ids_ok += 1 else {
                    std.debug.print("{s} prompt {d}: ids differ at {d} (len {d} vs {d})\n", .{ name, index, first, ids.len, expected.ids.len });
                }
                index += 1;
            }
        }
        std.debug.print("GLiDE parity {s}: text {d}/{d}, ids {d}/{d}\n", .{ name, text_ok, index, ids_ok, index });
        try std.testing.expectEqual(capture.prompts.len, index);
        try std.testing.expectEqual(index, text_ok);
        try std.testing.expectEqual(index, ids_ok);
        checked += 1;
    }
    if (checked == 0) return error.SkipZigTest;
}

test "GLiDE encoder wrap truncates head and tail around special tokens" {
    // A tiny whole-word vocabulary: every word is one id.
    const json =
        \\{"model":{"type":"WordPiece","unk_token":"[UNK]","continuing_subword_prefix":"##","max_input_chars_per_word":100,
        \\  "vocab":{"[CLS]":0,"[SEP]":1,"a":2,"b":3,"c":4,"d":5,"e":6,"f":7,"g":8,"h":9,"[UNK]":10}},
        \\ "added_tokens":[{"id":0,"content":"[CLS]","special":true},{"id":1,"content":"[SEP]","special":true},{"id":10,"content":"[UNK]","special":true}],
        \\ "post_processor":{"type":"BertProcessing","cls":["[CLS]",0],"sep":["[SEP]",1]}}
    ;
    const gpa = std.testing.allocator;
    const tokenizer = try hf.HfTokenizer.loadFromBytes(gpa, json);
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    var config = glide.DecisionConfig{ .backbone = .encoder, .pooling = .last, .max_len = 7, .trunc_head = 2, .temperature = 1, .codes = &.{} };
    const short = try prompt.encode(gpa, tok, &config, "a b c");
    defer gpa.free(short);
    try std.testing.expectEqualSlices(i32, &.{ 0, 2, 3, 4, 1 }, short);
    const long = try prompt.encode(gpa, tok, &config, "a b c d e f g h");
    defer gpa.free(long);
    // max_len 7 keeps 5 content ids: the first 2 and the last 3.
    try std.testing.expectEqualSlices(i32, &.{ 0, 2, 3, 7, 8, 9, 1 }, long);
    config.backbone = .decoder;
    config.max_len = 16;
    const decoded = try prompt.encode(gpa, tok, &config, "a b c d e f g h");
    defer gpa.free(decoded);
    try std.testing.expectEqual(@as(usize, 8), decoded.len);
    config.max_len = 15;
    try std.testing.expectError(error.GlidePromptTooLong, prompt.encode(gpa, tok, &config, "a b c d e f g h"));
}
