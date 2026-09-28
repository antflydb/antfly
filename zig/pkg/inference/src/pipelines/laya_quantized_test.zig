// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Q8_0 Laya linears (`laya.weight_quantization`, LAYA.md step 1d) against
//! dense serving of the same checkpoint on the session backend.
const std = @import("std");
const c_file = @import("../util/c_file.zig");
const factory = @import("../architectures/session_factory.zig");
const hf = @import("inference_hf_tokenizer");
const pipeline = @import("laya.zig");
const test_support = @import("../util/laya_test_support.zig");

/// Copy a model directory, setting `laya.weight_quantization` in its config.
fn quantizedCopy(a: std.mem.Allocator, io: std.Io, source: []const u8, target: []const u8) !void {
    try std.Io.Dir.cwd().createDir(io, target, .default_dir);
    for ([_][]const u8{ "model.safetensors", "model_manifest.json", "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json", "rl_agent_config.json", "config.json" }) |name| {
        const bytes = c_file.readFileFromDir(a, source, name) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer a.free(bytes);
        var contents = bytes;
        var edited: ?[]u8 = null;
        defer if (edited) |e| a.free(e);
        if (std.mem.eql(u8, name, "config.json")) {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
            defer parsed.deinit();
            try parsed.value.object.getPtr("laya").?.object.put(parsed.arena.allocator(), "weight_quantization", .{ .string = "q8_0" });
            var out = std.Io.Writer.Allocating.init(a);
            defer out.deinit();
            try std.json.Stringify.value(parsed.value, .{}, &out.writer);
            edited = try out.toOwnedSlice();
            contents = edited.?;
        }
        const path = try std.fs.path.join(a, &.{ target, name });
        defer a.free(path);
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true });
        defer file.close(io);
        var buffer: [4096]u8 = undefined;
        var w = file.writer(io, &buffer);
        try w.interface.writeAll(contents);
        try w.interface.flush();
    }
}

test "laya q8_0 linear weights keep every decision and stay close to dense serving" {
    const root = try test_support.fixture("ANTFLY_LAYA_REFERENCE");
    const a = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const dense_path = try std.fmt.allocPrint(s, "{s}/model", .{root});
    const quantized_path = try std.fs.path.join(s, &.{ try temp.dir.realPathFileAlloc(io, ".", s), "q8" });
    try quantizedCopy(a, io, dense_path, quantized_path);

    const reference = try std.json.parseFromSlice(struct { states: []const []const u8 }, s, try c_file.readFile(s, try std.fmt.allocPrint(s, "{s}/reference.json", .{root})), .{ .ignore_unknown_fields = true });
    const questions = [_]pipeline.Question{
        .{ .name = "tool", .kind = .choice, .instruction = "which tool is needed?", .labels = &.{ "search", "fetch", "none" }, .descriptions = &.{ "", "", "" } },
        .{ .name = "urgency", .kind = .score, .instruction = "urgency?", .labels = &.{ "low", "medium", "high" }, .descriptions = &.{ "", "", "" } },
        .{ .name = "needed", .kind = .noul, .instruction = "is search needed?", .labels = &.{ "false", "true" }, .descriptions = &.{ "", "" } },
    };
    var tasks: std.ArrayListUnmanaged(pipeline.Task) = .empty;
    for (reference.value.states) |state| for (questions) |q| try tasks.append(s, .{ .text = state, .question = q });

    const tokenizer_bytes = try c_file.readFileFromDir(s, dense_path, "tokenizer.json");
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();

    var decisions: [2][]const pipeline.Decision = undefined;
    for ([_][]const u8{ dense_path, quantized_path }, &decisions) |path, *out| {
        var session = try test_support.createSession(a, path);
        defer session.close();
        const config = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
        out.* = (try pipeline.execute(s, session, tok, config, tasks.items, null)).decisions;
    }
    var worst: f32 = 0;
    for (decisions[0], decisions[1]) |dense, quantized| {
        try std.testing.expectEqualStrings(dense.label, quantized.label);
        for (dense.probabilities, quantized.probabilities) |want, got| worst = @max(worst, @abs(want - got));
    }
    std.debug.print("Laya q8_0 vs dense: decisions={d} labels identical, max probability error={d:.7}\n", .{ tasks.items.len, worst });
    // Quantization must be in effect (not silently dense) and stay small.
    try std.testing.expect(worst > 0);
    try std.testing.expect(worst <= 2e-2);
}
