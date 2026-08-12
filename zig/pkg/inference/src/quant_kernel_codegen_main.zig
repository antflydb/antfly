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
const codegen = @import("native_quant_kernel_codegen.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_iter.next();

    var args_buf: [2][]const u8 = undefined;
    var args_len: usize = 0;
    while (args_iter.next()) |arg| {
        if (args_len >= args_buf.len) return error.InvalidArguments;
        args_buf[args_len] = arg;
        args_len += 1;
    }

    try codegen.main(allocator, init.io, args_buf[0..args_len]);
}
