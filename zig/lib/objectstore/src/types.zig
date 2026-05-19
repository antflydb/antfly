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
const Allocator = std.mem.Allocator;

pub const ObjectMetadata = struct {
    bucket: []u8,
    key: []u8,
    etag: ?[]u8 = null,
    version_id: ?[]u8 = null,
    content_length: u64,
    content_type: ?[]u8 = null,
    last_modified_unix_ms: ?i64 = null,

    pub fn deinit(self: *ObjectMetadata, alloc: Allocator) void {
        alloc.free(self.bucket);
        alloc.free(self.key);
        if (self.etag) |value| alloc.free(value);
        if (self.version_id) |value| alloc.free(value);
        if (self.content_type) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const PutOptions = struct {
    content_type: ?[]const u8 = null,
    if_match_etag: ?[]const u8 = null,
    if_none_match: bool = false,
};

pub const GetOptions = struct {
    version_id: ?[]const u8 = null,
    range: ?ByteRange = null,
    if_match_etag: ?[]const u8 = null,
    part_number: ?u32 = null,
};

pub const DeleteOptions = struct {
    version_id: ?[]const u8 = null,
    if_match_etag: ?[]const u8 = null,
};

pub const ListOptions = struct {
    prefix: []const u8 = "",
    recursive: bool = true,
    delimiter: []const u8 = "/",
    start_after: ?[]const u8 = null,
    continuation_token: ?[]const u8 = null,
    max_keys: u32 = 1000,
};

pub const ByteRange = struct {
    offset: u64,
    length: ?u64 = null,
};

pub const PutResult = struct {
    etag: ?[]u8 = null,
    version_id: ?[]u8 = null,

    pub fn deinit(self: *PutResult, alloc: Allocator) void {
        if (self.etag) |value| alloc.free(value);
        if (self.version_id) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const GetResult = struct {
    body: []u8,
    metadata: ObjectMetadata,

    pub fn deinit(self: *GetResult, alloc: Allocator) void {
        alloc.free(self.body);
        self.metadata.deinit(alloc);
        self.* = undefined;
    }
};

pub const ListEntry = struct {
    key: []u8,
    etag: ?[]u8 = null,
    size: u64,
    last_modified_unix_ms: ?i64 = null,

    pub fn deinit(self: *ListEntry, alloc: Allocator) void {
        alloc.free(self.key);
        if (self.etag) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const ObjectPart = struct {
    part_number: u32,
    size: u64,
    etag: ?[]u8 = null,

    pub fn deinit(self: *ObjectPart, alloc: Allocator) void {
        if (self.etag) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const ObjectAttributes = struct {
    etag: ?[]u8 = null,
    version_id: ?[]u8 = null,
    content_length: u64,
    content_type: ?[]u8 = null,
    parts: []ObjectPart,

    pub fn deinit(self: *ObjectAttributes, alloc: Allocator) void {
        if (self.etag) |value| alloc.free(value);
        if (self.version_id) |value| alloc.free(value);
        if (self.content_type) |value| alloc.free(value);
        for (self.parts) |*part| part.deinit(alloc);
        alloc.free(self.parts);
        self.* = undefined;
    }
};

pub const ListResult = struct {
    entries: []ListEntry,
    common_prefixes: [][]u8 = &.{},
    next_continuation_token: ?[]u8 = null,

    pub fn deinit(self: *ListResult, alloc: Allocator) void {
        for (self.entries) |*entry| entry.deinit(alloc);
        alloc.free(self.entries);
        for (self.common_prefixes) |prefix| alloc.free(prefix);
        alloc.free(self.common_prefixes);
        if (self.next_continuation_token) |value| alloc.free(value);
        self.* = undefined;
    }
};

test "object metadata owns strings" {
    const alloc = std.testing.allocator;
    var meta = ObjectMetadata{
        .bucket = try alloc.dupe(u8, "bucket"),
        .key = try alloc.dupe(u8, "key"),
        .etag = try alloc.dupe(u8, "etag"),
        .content_length = 1,
    };
    meta.deinit(alloc);
}

test "object attributes own part metadata" {
    const alloc = std.testing.allocator;
    var attrs = ObjectAttributes{
        .etag = try alloc.dupe(u8, "etag"),
        .version_id = try alloc.dupe(u8, "v1"),
        .content_length = 42,
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .parts = try alloc.alloc(ObjectPart, 1),
    };
    attrs.parts[0] = .{
        .part_number = 1,
        .size = 42,
        .etag = try alloc.dupe(u8, "part-etag"),
    };
    attrs.deinit(alloc);
}
