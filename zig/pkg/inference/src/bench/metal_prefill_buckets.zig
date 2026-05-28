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

fn getEnvVarOwned(allocator: std.mem.Allocator, comptime name: [:0]const u8) !?[]u8 {
    const value = std.c.getenv(name) orelse return null;
    return try allocator.dupe(u8, std.mem.span(value));
}

fn envOrDefault(allocator: std.mem.Allocator, comptime name: [:0]const u8, default_value: []const u8) ![]const u8 {
    return (try getEnvVarOwned(allocator, name)) orelse default_value;
}

fn defaultGemma4ModelPath(allocator: std.mem.Allocator) ![]u8 {
    if (try getEnvVarOwned(allocator, "TERMITE_GEMMA4_MODEL")) |value| return value;
    const home = (try getEnvVarOwned(allocator, "HOME")) orelse return error.HomeNotSet;
    defer allocator.free(home);
    return std.fs.path.join(allocator, &.{ home, ".termite/models/ggml-org/gemma-4-e2b-it-gguf" });
}

fn makePrompt(allocator: std.mem.Allocator, words: usize) ![]u8 {
    var out = std.ArrayList(u8).empty;
    try out.appendSlice(allocator, "hi");
    var i: usize = 1;
    while (i < words) : (i += 1) {
        try out.appendSlice(allocator, " benchmark");
    }
    return out.toOwnedSlice(allocator);
}

fn runBucket(
    io: std.Io,
    allocator: std.mem.Allocator,
    termite_bin: []const u8,
    model: []const u8,
    backend: []const u8,
    decode_count: []const u8,
    label: []const u8,
    target_prompt_tokens: usize,
    words: usize,
) !void {
    const prompt = try makePrompt(allocator, words);
    defer allocator.free(prompt);

    std.debug.print("\n== {s} target_prompt_tokens={d} prompt_words={d} decode_count={s} ==\n", .{ label, target_prompt_tokens, words, decode_count });

    var child = try std.process.spawn(io, .{
        .argv = &.{
            termite_bin,
            "generate",
            model,
            prompt,
            "--backend",
            backend,
            "--max-tokens",
            decode_count,
            "--print-token-ids",
            "--print-timing",
        },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    });
    const term = try child.wait(io);
    switch (term) {
        .exited => |code| if (code != 0) return error.BenchmarkCommandFailed,
        else => return error.BenchmarkCommandFailed,
    }
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const termite_bin = try envOrDefault(allocator, "TERMITE_BIN", "./zig-out/bin/termite");
    const model = try defaultGemma4ModelPath(allocator);
    defer allocator.free(model);
    const backend = try envOrDefault(allocator, "TERMITE_BENCH_BACKEND", "metal");
    const prefill_decode_count = try envOrDefault(allocator, "TERMITE_BENCH_PREFILL_DECODE_COUNT", "4");
    const decode_count = try envOrDefault(allocator, "TERMITE_BENCH_DECODE_COUNT", "16");

    try runBucket(io, allocator, termite_bin, model, backend, prefill_decode_count, "pp10", 10, 1);
    try runBucket(io, allocator, termite_bin, model, backend, prefill_decode_count, "pp128", 128, 119);
    try runBucket(io, allocator, termite_bin, model, backend, prefill_decode_count, "pp512", 512, 503);
    try runBucket(io, allocator, termite_bin, model, backend, decode_count, "tg16", 10, 1);
}
