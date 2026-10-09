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
const Bitmap = @import("../encoding/roaring.zig").RoaringBitmap;
/// Same-transaction native identity resolution. A missing block proof requests
/// the legacy point-lookup path; it must never be interpreted as an empty set.
pub const Lookup = struct {
    ptr: *anyopaque,
    one: *const fn (*anyopaque, []const u8) anyerror!?u32,
    block: *const fn (*anyopaque, std.mem.Allocator, []const u8, u32, *const Bitmap, *Bitmap) anyerror!bool,
};

/// Owned masks in one pinned sparse generation. A null include admits every
/// ordinal; an empty include admits none. Exclusions never require constructing
/// the potentially archive-sized complement.
pub const Selection = struct {
    include: ?Bitmap = null,
    exclude: ?Bitmap = null,
    pub fn deinit(self: *Selection) void {
        if (self.include) |*bitmap| bitmap.deinit();
        if (self.exclude) |*bitmap| bitmap.deinit();
        self.* = undefined;
    }
};
