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

//! Bounded replayable mutation smoke tests plus coverage-guided fuzz entrypoints.
const std = @import("std");
const regex = @import("antfly_regex").search;
const Budget = @import("antfly_regex").Budget;
const syntax = @import("antfly_syntax");
const compress = @import("antfly_compress");
const Dictionary = compress.brotli.SharedDictionary;
const seeds = [_][]const u8{
    "\x91\x00\x00\x01\x00\x00\x01\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x61\x6e\x74\x66\x6c\x79\x7a\x69\x67\x7a\x61\x67\x63\x75\x73\x74\x6f\x6d\x73\x65\x61\x72\x63\x68\x62\x6f\x75\x6e\x64\x65\x64\x63\x6f\x64\x65\x63\x61\x62\x63\x64\x65\x66\x67\x68\x69\x6a\x6b\x6c\x6d\x6e\x6f\x70\x71\x72\x73\x74\x75\x76\x77\x78\x79\x31\x32\x33\x34\x35\x36\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50\x51\x52\x53\x54\x55\x56\x57\x58\x59\x31\x32\x33\x34\x35\x36\x01\x05\x00\x01\x3c\x01\x3e\x00\x18\x02\x02\x00\x02\x02\x01\x02\x02\x02\x02\x02\x03\x02\x02\x04\x02\x02\x05\x02\x02\x06\x02\x02\x07\x02\x02\x08\x02\x02\x09\x02\x02\x0a\x02\x02\x0b\x02\x02\x0c\x02\x02\x0d\x02\x02\x0e\x02\x02\x0f\x02\x02\x10\x02\x02\x11\x02\x02\x12\x02\x02\x13\x02\x02\x14\x02\x02\x15\x02\x02\x16\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00\x01\x00\x00\x00",
    "\x91\x00\x00\x01\x00\x00\x01\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x61\x6e\x74\x66\x6c\x79\x7a\x69\x67\x7a\x61\x67\x63\x75\x73\x74\x6f\x6d\x73\x65\x61\x72\x63\x68\x62\x6f\x75\x6e\x64\x65\x64\x63\x6f\x64\x65\x63\x61\x62\x63\x64\x65\x66\x67\x68\x69\x6a\x6b\x6c\x6d\x6e\x6f\x70\x71\x72\x73\x74\x75\x76\x77\x78\x79\x31\x32\x33\x34\x35\x36\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50\x51\x52\x53\x54\x55\x56\x57\x58\x59\x31\x32\x33\x34\x35\x36\x01\x05\x00\x01\x3c\x01\x3e\x00\x18\x02\x00\x02\x02\x01\x02\x02\x02\x02\x02\x03\x02\x02\x04\x02\x02\x05\x02\x02\x06\x02\x02\x07\x02\x02\x08\x02\x02\x09\x02\x02\x0a\x02\x02\x0b\x02\x02\x0c\x02\x02\x0d\x02\x02\x0e\x02\x02\x0f\x02\x02\x10\x02\x02\x11\x02\x02\x12\x02\x02\x13\x02\x02\x14\x02\x02\x15\x02\x02\x16\x02\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x00\x00\x00\x01\x00\x00\x00",
    "(?<n>a|é)+\\k<n>",
    "(?<=a{1,3})(b)",
    "(a(?1)?b)",
    "a(*MARK:x)(*SKIP:x)b|c",
    "[a-z]+",
    "\xff\x00[",
    "def f(x):\n    return f(x-1)\n",
    "package p\nfunc f(){ f() }\n",
    "fn f() { /* nested /* c */ */ f(); }",
    "\x91\x00\x00\x00\x00\x01\x00\x00\x00\x00",
    "\x37\xa4\x30\xec\x01\x00\x00\x00",
};
const corpus: [seeds.len][]const u8 = blk: {
    var encoded: [seeds.len][]const u8 = undefined;
    for (seeds, 0..) |seed, i| {
        const len: u32 = seed.len;
        const prefix: [4]u8 = @bitCast(std.mem.nativeToLittle(u32, len));
        encoded[i] = prefix ++ seed;
    }
    break :blk encoded;
};
fn regexOne(bytes: []const u8) !void {
    {
        var memory = Budget{ .backing = std.testing.allocator, .limit = 1024 * 1024 };
        defer std.debug.assert(memory.live == 0);
        const a = memory.allocator();
        if (@import("antfly_regex").PreparedPattern.init(a, bytes[0..@min(bytes.len, 128)])) |compiled| {
            var prepared = compiled;
            defer prepared.deinit();
            var reference = prepared;
            reference.fast = .{};
            const subject = bytes[0..@min(bytes.len, 128)];
            try std.testing.expectEqual(try reference.matches(a, subject), try prepared.matches(a, subject));
        } else |_| {}
    }
    const split = @min(bytes.len, 16);
    if (!regex.literal.mayContainOrByte(bytes[split..], bytes[0..split], 0)) {
        try std.testing.expect(std.mem.indexOfScalar(u8, bytes[split..], 0) == null);
        try std.testing.expect(regex.literal.findExact(bytes[split..], bytes[0..split], 0) == null);
    }
    try regexCheck(bytes[0..@min(bytes.len, 128)], if (bytes.len > 128) bytes[128..] else "aaab éé xyz\n");
    const patterns = [_][]const u8{ "((a)(b))\\K(c)?", "(?<n>[a-z]+)\\k<n>", "(?<=(a))b|é+", "(a|ab)*?b", "(?i)([a-z])([0-9])", "(a(?1)?b)" };
    try regexCheck(patterns[if (bytes.len == 0) 0 else bytes[0] % patterns.len], bytes[0..@min(bytes.len, 128)]);
}
fn regexCheck(expression: []const u8, subject: []const u8) !void {
    var budget = Budget{ .backing = std.testing.allocator, .limit = 1024 * 1024 };
    defer std.debug.assert(budget.live == 0);
    const a = budget.allocator();
    var p = regex.BacktrackingPattern.init(a, expression, .{}) catch return;
    defer p.deinit();
    var fast = try regex.BacktrackingRunner.init(a, &p);
    defer fast.deinit();
    // A borrowed pattern view also disables candidate skipping in the scalar
    // oracle. The original pattern retains ownership of its arena and JIT.
    var unfiltered = p;
    unfiltered.candidate_prefix = "";
    var vm = try regex.BacktrackingRunner.init(a, &unfiltered);
    defer vm.deinit();
    fast.max_steps = 4096;
    vm.max_steps = 4096;
    vm.jit_enabled = false;
    vm.ascii_repeat_enabled = false;
    for (0..@min(subject.len + 1, 16)) |offset| {
        const actual = fast.find(subject, offset) catch |err| {
            try std.testing.expectError(err, vm.find(subject, offset));
            try std.testing.expectEqual(vm.steps, fast.steps);
            try std.testing.expectEqualStrings(vm.mark orelse "", fast.mark orelse "");
            continue;
        };
        const expected = try vm.find(subject, offset);
        try std.testing.expectEqual(expected != null, actual != null);
        if (actual) |match| {
            try std.testing.expectEqual(expected.?.span, match.span);
            try std.testing.expectEqualSlices(?regex.Span, expected.?.captures, match.captures);
        }
        // Skipping an impossible start performs no VM work. A successful
        // candidate and every budget error retain identical state accounting.
        if (actual != null or p.candidate_prefix.len == 0) try std.testing.expectEqual(vm.steps, fast.steps);
        try std.testing.expectEqualStrings(vm.mark orelse "", fast.mark orelse "");
    }
    var regular = regex.Pattern.init(a, expression, .{}) catch return;
    defer regular.deinit();
    var runner = regex.Runner.init(a, &regular) catch return;
    defer runner.deinit();
    if (runner.find(subject, 0)) |match| {
        try std.testing.expect(match.span.start <= match.span.end and match.span.end <= subject.len);
    }
}
fn parserOne(bytes: []const u8) !void {
    for ([_]*const syntax.grammar.Grammar{ syntax.python, syntax.go, syntax.rust }) |grammar| {
        var tree = syntax.grammar.parseOptions(std.testing.allocator, grammar, bytes, .{ .memory_limit = 512 * 1024 }) catch |err| switch (err) {
            error.ParseBudgetExceeded, error.OutOfMemory => continue,
            else => return err,
        };
        defer tree.deinit();
        for (tree.nodes) |node| try std.testing.expect(node.start <= node.end and node.end <= bytes.len);
    }
    for ([_]syntax.ast.Lang{ .js, .ts }) |lang| {
        var budget = Budget{ .backing = std.testing.allocator, .limit = 512 * 1024 };
        defer std.debug.assert(budget.live == 0);
        var tree = syntax.parse(budget.allocator(), bytes, .{ .lang = lang }) catch continue;
        defer tree.deinit();
    }
}
fn dictionaryOne(bytes: []const u8) !void {
    var budget = Budget{ .backing = std.testing.allocator, .limit = 512 * 1024 };
    defer std.debug.assert(budget.live == 0);
    const a = budget.allocator();
    for ([_]compress.brotli.DictionaryFormat{ .brotli, .@"brotli-google", .raw, .auto }) |format| {
        var dictionary = Dictionary.init(a, bytes, format) catch continue;
        defer dictionary.deinit();
        const Words = std.meta.Child(@TypeOf(dictionary.words));
        const Transforms = std.meta.Child(@TypeOf(dictionary.transforms));
        const empty_words: Words = .{ .data = &.{}, .bits = @splat(0), .offsets = @splat(0) };
        const empty_transforms: Transforms = .{ .strings = &.{}, .offsets = &.{}, .triplets = &.{} };
        var transformed: [4096]u8 = undefined;
        for (4..32) |length| {
            const resolved = dictionary.resolve(length, if (bytes.len > 1) bytes[1] else 0, @intCast(if (bytes.len > 2) bytes[2] % 64 else 0), &empty_words, &empty_transforms) catch continue;
            const result = resolved.transform.apply(resolved.word, &transformed) catch continue;
            try std.testing.expect(result.len <= transformed.len);
        }

        var input = std.Io.Reader.fixed(&.{0x3b});
        var output: std.Io.Writer.Allocating = .init(a);
        defer output.deinit();
        compress.brotli.decodeToOptions(a, &input, &output.writer, 4096, .{ .max_window_bytes = 8192, .prepared_dictionary = &dictionary }) catch {};
        try std.testing.expect(output.written().len <= 4096);
    }
    zstd: {
        const dictionary = compress.ZstdDecoder.Dictionary.init(a, bytes) catch break :zstd;
        defer dictionary.deinit(a);
    }
    const decoded = compress.lz4.decodeOptions(a, bytes, 4096, .{ .dictionary = bytes }) catch return;
    defer a.free(decoded);
    try std.testing.expect(decoded.len <= 4096);
}
fn fuzzRegex(_: void, smith: *std.testing.Smith) !void {
    var buffer: [256]u8 = undefined;
    try regexOne(buffer[0..smith.slice(&buffer)]);
}
fn fuzzParser(_: void, smith: *std.testing.Smith) !void {
    var buffer: [512]u8 = undefined;
    try parserOne(buffer[0..smith.slice(&buffer)]);
}
fn fuzzDictionary(_: void, smith: *std.testing.Smith) !void {
    var buffer: [512]u8 = undefined;
    try dictionaryOne(buffer[0..smith.slice(&buffer)]);
}
test "coverage fuzz regex VM and JIT" {
    try std.testing.fuzz({}, fuzzRegex, .{ .corpus = &corpus });
}
test "coverage fuzz pinned parsers" {
    try std.testing.fuzz({}, fuzzParser, .{ .corpus = &corpus });
}
test "coverage fuzz dictionaries" {
    try std.testing.fuzz({}, fuzzDictionary, .{ .corpus = &corpus });
}
test "deterministic malformed-input mutation replay" {
    var rng = std.Random.DefaultPrng.init(1151);
    const random = rng.random();
    var buffer: [512]u8 = undefined;
    for (0..1024) |iteration| {
        const seed = seeds[iteration % seeds.len];
        @memcpy(buffer[0..seed.len], seed);
        var len = seed.len;
        switch (iteration % 4) {
            0 => len = random.uintLessThan(usize, len + 1),
            1 => {
                buffer[random.uintLessThan(usize, len)] = random.int(u8);
            },
            2 => {
                const extra = random.uintLessThan(usize, 64);
                random.bytes(buffer[len..][0..extra]);
                len += extra;
            },
            else => {
                len = random.uintLessThan(usize, buffer.len + 1);
                random.bytes(buffer[0..len]);
            },
        }
        try regexOne(buffer[0..len]);
        try parserOne(buffer[0..len]);
        try dictionaryOne(buffer[0..len]);
    }
}

fn allocateRegex(a: std.mem.Allocator) !void {
    var pattern = try regex.BacktrackingPattern.init(a, "(?<n>a|ab)+\\k<n>", .{});
    defer pattern.deinit();
    var runner = try regex.BacktrackingRunner.init(a, &pattern);
    defer runner.deinit();
    _ = try runner.find("abab", 0);
}
fn allocateParser(a: std.mem.Allocator) !void {
    var tree = try syntax.grammar.parse(a, syntax.go, "package p\nfunc f() {}\n");
    defer tree.deinit();
}
fn allocateDictionary(a: std.mem.Allocator) !void {
    var dictionary = try Dictionary.init(a, seeds[0], .brotli);
    defer dictionary.deinit();
}
test "allocation failure cleanup across regex parser and dictionary" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocateRegex, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocateParser, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocateDictionary, .{});
}
