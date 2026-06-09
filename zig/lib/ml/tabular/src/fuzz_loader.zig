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

//! Standalone fuzz target for `tabular_model.json` loading.
//!
//! Drives the IR loader against arbitrary input via the standard Zig
//! `fuzz_test` machinery. Wired into the root build as `fuzz-tabular-loader`.
//!
//! Run interactively:
//!     zig test src/fuzz_loader.zig -fno-emit-bin --fuzz
//!
//! Reproducer:
//!     The fuzzer prints a base64 corpus on failure; pipe it back through
//!     `parseAndCatch` from a normal `zig test` to reproduce.

const std = @import("std");
const loader = @import("loader.zig");

fn parseAndCatch(input: []const u8) void {
    var loaded = loader.parseFromSlice(std.testing.allocator, input) catch return;
    loaded.deinit();
}

test "fuzz: arbitrary input must not panic" {
    const Context = struct {
        fn run(_: @This(), smith: *std.testing.Smith) anyerror!void {
            if (smith.in) |bytes| parseAndCatch(bytes);
        }
    };
    try std.testing.fuzz(Context{}, Context.run, .{});
}

test "regression: empty input is rejected, not crashed" {
    parseAndCatch("");
    parseAndCatch("{");
    parseAndCatch("{\"schema_version\":1}");
    // Bytes designed to exercise the unicode-decoding path.
    parseAndCatch("\xff\xfe\xff\xfe");
}
