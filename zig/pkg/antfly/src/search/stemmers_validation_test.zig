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

//! Stemmer validation tests using official Snowball test vocabularies.
//! Test data from: https://github.com/snowballstem/snowball-data
//!
//! Each test allows a maximum failure percentage. As stemmers improve,
//! thresholds should be tightened toward 0%.

const std = @import("std");
const stemmers = @import("stemmers.zig");
const stopwords = @import("stopwords.zig");
const Language = stopwords.Language;

fn validateLanguage(
    alloc: std.mem.Allocator,
    comptime lang: Language,
    comptime voc_path: []const u8,
    comptime out_path: []const u8,
    comptime max_fail_pct: f64,
) !void {
    const voc_data = @embedFile(voc_path);
    const out_data = @embedFile(out_path);

    var voc_iter = std.mem.splitScalar(u8, voc_data, '\n');
    var out_iter = std.mem.splitScalar(u8, out_data, '\n');

    var line_num: usize = 0;
    var failures: usize = 0;
    const max_print = 50;

    while (true) {
        const word = voc_iter.next() orelse break;
        const expected = out_iter.next() orelse break;

        if (word.len == 0) continue;
        line_num += 1;

        const actual = try stemmers.stem(alloc, word, lang);
        const is_owned = actual.ptr != word.ptr;
        defer if (is_owned) alloc.free(actual);

        if (!std.mem.eql(u8, actual, expected)) {
            failures += 1;
            if (failures <= max_print) {
                std.debug.print("  FAIL line {d}: \"{s}\" -> \"{s}\" (expected \"{s}\")\n", .{
                    line_num, word, actual, expected,
                });
            }
        }
    }

    const fail_pct: f64 = if (line_num > 0)
        @as(f64, @floatFromInt(failures)) * 100.0 / @as(f64, @floatFromInt(line_num))
    else
        0.0;

    if (failures > 0) {
        std.debug.print("{s}: {d}/{d} failures ({d:.1}%, threshold {d:.0}%)\n", .{
            @tagName(lang), failures, line_num, fail_pct, max_fail_pct,
        });
    }

    if (fail_pct > max_fail_pct) {
        return error.StemmerValidationFailed;
    }
}

// All stemmers now use Snowball compiler-generated code and match the
// reference output exactly (0% failure rate).

test "snowball: german" {
    try validateLanguage(std.testing.allocator, .german, "testdata/snowball/german/voc.txt", "testdata/snowball/german/output.txt", 0);
}

test "snowball: french" {
    try validateLanguage(std.testing.allocator, .french, "testdata/snowball/french/voc.txt", "testdata/snowball/french/output.txt", 0);
}

test "snowball: spanish" {
    try validateLanguage(std.testing.allocator, .spanish, "testdata/snowball/spanish/voc.txt", "testdata/snowball/spanish/output.txt", 0);
}

test "snowball: italian" {
    try validateLanguage(std.testing.allocator, .italian, "testdata/snowball/italian/voc.txt", "testdata/snowball/italian/output.txt", 0);
}

test "snowball: portuguese" {
    try validateLanguage(std.testing.allocator, .portuguese, "testdata/snowball/portuguese/voc.txt", "testdata/snowball/portuguese/output.txt", 0);
}

test "snowball: dutch" {
    try validateLanguage(std.testing.allocator, .dutch, "testdata/snowball/dutch/voc.txt", "testdata/snowball/dutch/output.txt", 0);
}

test "snowball: swedish" {
    try validateLanguage(std.testing.allocator, .swedish, "testdata/snowball/swedish/voc.txt", "testdata/snowball/swedish/output.txt", 0);
}

test "snowball: norwegian" {
    try validateLanguage(std.testing.allocator, .norwegian, "testdata/snowball/norwegian/voc.txt", "testdata/snowball/norwegian/output.txt", 0);
}

test "snowball: danish" {
    try validateLanguage(std.testing.allocator, .danish, "testdata/snowball/danish/voc.txt", "testdata/snowball/danish/output.txt", 0);
}

test "snowball: finnish" {
    try validateLanguage(std.testing.allocator, .finnish, "testdata/snowball/finnish/voc.txt", "testdata/snowball/finnish/output.txt", 0);
}
