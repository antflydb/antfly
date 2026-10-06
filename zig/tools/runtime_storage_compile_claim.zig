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

//! Evaluate the production reservation rather than parsing its source layout.
const std = @import("std");
const memory = @import("runtime_memory");

pub fn main() !void {
    const linux = try std.zig.system.resolveTargetQuery(std.Io.Threaded.global_single_threaded.io(), .{
        .cpu_arch = .x86_64,
        .cpu_model = .baseline,
        .os_tag = .linux,
        .abi = .gnu,
    });
    var required: usize = 0;
    // CI builds both measured CPU releases and conservative test profiles.
    for ([_]std.builtin.OptimizeMode{ .Debug, .ReleaseFast, .ReleaseSafe }) |optimize| {
        for ([_]bool{ false, true }) |cpu_inference| {
            required = @max(required, memory.runtimeCompileMaxRss(.storage_kernel, .{
                .host = linux,
                .target = linux,
                .optimize = optimize,
                .strip = true,
                .cpu_inference = cpu_inference,
            }));
        }
    }
    std.debug.print("{d}\n", .{required});
}
