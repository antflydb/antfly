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

/// Borrowed transport-neutral cancellation source for one object operation.
/// The callback and context must remain valid until the synchronous operation
/// returns. Providers adapt this directly to their HTTP transport so an active
/// request can be interrupted instead of merely discarded after completion.
pub const CancellationToken = struct {
    ptr: *const anyopaque,
    is_cancelled_fn: *const fn (*const anyopaque) bool,

    pub fn fromAtomic(signal: *const std.atomic.Value(bool)) CancellationToken {
        return .{
            .ptr = signal,
            .is_cancelled_fn = struct {
                fn call(raw: *const anyopaque) bool {
                    const value: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
                    return value.load(.acquire);
                }
            }.call,
        };
    }

    pub fn fromCallback(
        ptr: ?*const anyopaque,
        is_cancelled_fn: ?*const fn (*const anyopaque) bool,
    ) ?CancellationToken {
        return .{
            .ptr = ptr orelse return null,
            .is_cancelled_fn = is_cancelled_fn orelse return null,
        };
    }

    pub fn isCancelled(self: CancellationToken) bool {
        return self.is_cancelled_fn(self.ptr);
    }

    pub fn check(self: CancellationToken) !void {
        if (self.isCancelled()) return error.Canceled;
    }
};

pub const ObjectChecksumAlgorithm = enum {
    crc32_base64,
    crc32c_base64,
    crc64nvme_base64,
    sha1_base64,
    sha256_hex,
    sha256_base64,
    sha512_base64,
    md5_base64,
    xxhash64_base64,
    xxhash3_base64,
    xxhash128_base64,
};

pub const ObjectChecksumType = enum {
    full_object,
    composite,
    unknown,
};

pub const ObjectChecksum = struct {
    algorithm: ObjectChecksumAlgorithm,
    value: []u8,
    checksum_type: ObjectChecksumType = .full_object,
    /// Number of source parts represented by a composite checksum, when the
    /// provider exposes it separately from the encoded digest.
    part_count: ?u32 = null,

    pub fn clone(self: ObjectChecksum, alloc: Allocator) !ObjectChecksum {
        return .{
            .algorithm = self.algorithm,
            .value = try alloc.dupe(u8, self.value),
            .checksum_type = self.checksum_type,
            .part_count = self.part_count,
        };
    }

    pub fn deinit(self: *ObjectChecksum, alloc: Allocator) void {
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const ObjectChecksumScope = enum {
    object,
    response_body,
};

pub const ObjectMetadata = struct {
    bucket: []u8,
    key: []u8,
    etag: ?[]u8 = null,
    version_id: ?[]u8 = null,
    checksum: ?ObjectChecksum = null,
    checksum_scope: ObjectChecksumScope = .object,
    content_length: u64,
    content_type: ?[]u8 = null,
    last_modified_unix_ms: ?i64 = null,

    pub fn deinit(self: *ObjectMetadata, alloc: Allocator) void {
        alloc.free(self.bucket);
        alloc.free(self.key);
        if (self.etag) |value| alloc.free(value);
        if (self.version_id) |value| alloc.free(value);
        if (self.checksum) |*value| value.deinit(alloc);
        if (self.content_type) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const PutOptions = struct {
    content_type: ?[]const u8 = null,
    if_match_etag: ?[]const u8 = null,
    if_none_match: bool = false,
    /// Optional full-object SHA-256 supplied by callers that already hashed
    /// the body. S3-compatible implementations persist this as provider
    /// checksum metadata so later integrity checks remain metadata-only.
    checksum_sha256_base64: ?[]const u8 = null,
    /// Optional caller-computed SHA-256 in lowercase hexadecimal form. GCS JSON
    /// uploads persist this as informational custom metadata only. Providers do
    /// not validate custom metadata against object bytes, so consumers must not
    /// treat it as an authenticated `ObjectChecksum`.
    checksum_sha256_hex: ?[]const u8 = null,
    /// Borrowed for the duration of the upload. Remote providers interrupt
    /// their active transport; local providers check between bounded chunks
    /// and never publish a partially written object.
    cancellation: ?CancellationToken = null,
};

pub const GetOptions = struct {
    version_id: ?[]const u8 = null,
    range: ?ByteRange = null,
    /// Strong conditional read. Implementations must return
    /// `error.PreconditionFailed` rather than data from another object version.
    if_match_etag: ?[]const u8 = null,
    part_number: ?u32 = null,
    /// Avoid a separate provider metadata request when the caller only needs
    /// the response body and response-derived metadata. Defaults to false so
    /// existing callers retain complete metadata semantics.
    skip_metadata_probe: bool = false,
    /// Bound bytes buffered by transports for this request. Implementations
    /// must enforce this before returning a response body.
    max_response_bytes: ?usize = null,
    /// Borrowed for the duration of this operation. Remote providers interrupt
    /// their active transport; local providers check between bounded chunks.
    cancellation: ?CancellationToken = null,
};

pub const StatOptions = struct {
    cancellation: ?CancellationToken = null,
};

pub const DeleteOptions = struct {
    version_id: ?[]const u8 = null,
    if_match_etag: ?[]const u8 = null,
};

pub const ListOptions = struct {
    prefix: []const u8 = "",
    recursive: bool = true,
    delimiter: []const u8 = "/",
    /// Exclusive lexicographic lower bound. Providers with an inclusive native
    /// primitive (notably GCS `startOffset`) must normalize their first page.
    start_after: ?[]const u8 = null,
    /// Opaque provider cursor. Mutually exclusive with `start_after`.
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
        .checksum = .{
            .algorithm = .sha256_hex,
            .value = try alloc.dupe(u8, "abcd"),
            .checksum_type = .full_object,
        },
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
