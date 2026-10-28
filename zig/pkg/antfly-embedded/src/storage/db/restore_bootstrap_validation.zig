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
const Port = @import("../../common/read_admission.zig").Port;

pub fn invalidName(name: []const u8, control: ?Port) !bool {
    const original = control orelse return std.mem.indexOfAny(u8, name, "/\\\x00") != null;
    var offset: usize = 0;
    while (offset < name.len) {
        try original.checkpoint();
        const end = offset + @min(@as(usize, 4096), name.len - offset);
        if (std.mem.indexOfAny(u8, name[offset..end], "/\\\x00") != null) return true;
        offset = end;
    }
    return false;
}

pub fn validUtf8(text: []const u8, control: ?Port) !bool {
    const original = control orelse return std.unicode.utf8ValidateSlice(text);
    var offset: usize = 0;
    var next_check: usize = 0;
    while (offset < text.len) {
        if (offset >= next_check) {
            try original.checkpoint();
            next_check = offset +| 4096;
        }
        const length = std.unicode.utf8ByteSequenceLength(text[offset]) catch return false;
        if (length > text.len - offset) return false;
        _ = std.unicode.utf8Decode(text[offset..][0..length]) catch return false;
        offset += length;
    }
    try original.checkpoint();
    return true;
}

pub fn less(left: []const u8, right: []const u8, control: ?Port) !bool {
    const original = control orelse return std.mem.order(u8, left, right) == .lt;
    const common = @min(left.len, right.len);
    var offset: usize = 0;
    while (offset < common) {
        try original.checkpoint();
        const end = offset + @min(@as(usize, 4096), common - offset);
        switch (std.mem.order(u8, left[offset..end], right[offset..end])) {
            .lt => return true,
            .gt => return false,
            .eq => {},
        }
        offset = end;
    }
    return left.len < right.len;
}
