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
const format = @import("format.zig");

pub const Catalog = struct {
    file: *const format.File,

    pub fn init(file: *const format.File) Catalog {
        return .{ .file = file };
    }

    pub fn count(self: Catalog) usize {
        return self.file.tensors.len;
    }

    pub fn find(self: Catalog, name: []const u8) ?*const format.TensorInfo {
        for (self.file.tensors) |*tensor| {
            if (std.mem.eql(u8, tensor.name, name)) return tensor;
        }
        return null;
    }
};

test "catalog finds tensor by name" {
    var dims = [_]u64{ 4, 8 };
    var tensors = [_]format.TensorInfo{.{
        .name = "test.weight",
        .dimensions = &dims,
        .tensor_type = .{ .known = .F16 },
        .offset = 0,
        .data_offset = 64,
    }};
    const file = format.File{
        .header = .{ .version = 3, .tensor_count = 1, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = 32,
        .data_region_offset = 64,
    };
    const catalog = Catalog.init(&file);
    try std.testing.expectEqual(@as(usize, 1), catalog.count());
    try std.testing.expect(catalog.find("test.weight") != null);
    try std.testing.expect(catalog.find("missing") == null);
}
