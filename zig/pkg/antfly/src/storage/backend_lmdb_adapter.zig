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
const backend_adapter = @import("backend_adapter.zig");
const lmdb = @import("lmdb.zig");

fn entryOf(entry: lmdb.Entry) backend_adapter.Entry {
    return .{
        .key = entry.key,
        .value = entry.value,
    };
}

fn close(cursor: *lmdb.Cursor) void {
    cursor.close();
}

fn first(cursor: *lmdb.Cursor) !backend_adapter.Entry {
    return entryOf(try cursor.first());
}

fn last(cursor: *lmdb.Cursor) !backend_adapter.Entry {
    return entryOf(try cursor.getEntry(.last));
}

fn next(cursor: *lmdb.Cursor) !backend_adapter.Entry {
    return entryOf(try cursor.next());
}

fn prev(cursor: *lmdb.Cursor) !backend_adapter.Entry {
    return entryOf(try cursor.getEntry(.prev));
}

fn seekAtOrAfter(cursor: *lmdb.Cursor, key: []const u8) !backend_adapter.Entry {
    return entryOf(try cursor.seekRange(key));
}

fn seekAtOrBefore(cursor: *lmdb.Cursor, key: []const u8) !backend_adapter.Entry {
    const entry = cursor.seekRange(key) catch |err| switch (err) {
        lmdb.Error.NotFound => return entryOf(try cursor.getEntry(.last)),
        else => return err,
    };
    if (std.mem.order(u8, entry.key, key) == .gt) {
        return entryOf(try cursor.getEntry(.prev));
    }
    return entryOf(entry);
}

pub const Cursor = backend_adapter.Cursor(lmdb.Cursor, .{
    .close = close,
    .first = first,
    .last = last,
    .next = next,
    .prev = prev,
    .seek_at_or_after = seekAtOrAfter,
    .seek_at_or_before = seekAtOrBefore,
});
