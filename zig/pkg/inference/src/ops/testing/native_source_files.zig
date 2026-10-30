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

const std = @import("std");
const metal_renderer = @import("../../graph/quant_kernel_metal_renderer.zig");
const cuda_renderer = @import("../../graph/quant_kernel_cuda_renderer.zig");

pub fn read(path: []const u8) ![]u8 {
    const allocator = std.testing.allocator;
    const stored = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(1 << 20));
    errdefer allocator.free(stored);
    if (try metal_renderer.expandNativeSourceFile(allocator, stored)) |full| {
        allocator.free(stored);
        return full;
    }
    if (try cuda_renderer.expandNativeSourceFile(allocator, stored)) |full| {
        allocator.free(stored);
        return full;
    }
    return stored;
}
