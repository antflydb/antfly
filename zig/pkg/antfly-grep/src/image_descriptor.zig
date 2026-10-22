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

//! Bounded verified facts, never image bytes or permission grants. A record
//! becomes reusable only after a current authorized source identity matches.
const std = @import("std");
const media = @import("antfly_source_evidence");
const Options = @import("options.zig").Options;
const Cache = @import("artifact_cache.zig").Cache;
const A = std.mem.Allocator;
pub const Context = struct { endpoint: []const u8, token: []const u8, table: []const u8, id: []const u8 };
pub const Record = struct {
    identity: []const u8,
    sha256: [32]u8,
    mime_type: []const u8,
    width: u32,
    height: u32,
    bytes: usize,
    pub fn image(self: Record) media.Image {
        return .{ .data_url = "", .sha256 = self.sha256, .mime_type = self.mime_type, .width = self.width, .height = self.height, .bytes = self.bytes };
    }
};
pub fn enabled(options: *const Options) bool {
    return !options.no_cache and options.cache_ttl_seconds != 0 and options.cache_dir != null;
}
fn cache(options: *const Options) ?Cache {
    if (!enabled(options)) return null;
    return .{ .directory = options.cache_dir.?, .namespace = "verified-image-v1", .buckets = 256, .ways = 4, .limit = 1024 };
}
fn key(context: Context, field: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&@import("grep_cache_options").image_validator_digest);
    for ([_][]const u8{ context.endpoint, context.token, context.table, context.id, field }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, part.len, .little);
        hash.update(&length);
        hash.update(part);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
pub fn validIdentity(bytes: []const u8) bool {
    if (bytes.len != 68 or !std.mem.startsWith(u8, bytes, "sv1-")) return false;
    for (bytes[4..]) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return false;
    return true;
}
pub fn load(a: A, io: std.Io, options: *const Options, context: Context, unit: media.Unit) ?Record {
    const bucket = cache(options) orelse return null;
    const bytes = bucket.load(a, io, key(context, unit.asset.field)) orelse return null;
    defer a.free(bytes);
    const record = std.json.parseFromSliceLeaky(Record, a, bytes, .{ .allocate = .alloc_always }) catch return null;
    if (!validIdentity(record.identity) or record.bytes != unit.asset.byte_length or record.bytes > options.max_image_bytes or record.width == 0 or record.height == 0 or @as(u64, record.width) * record.height > options.max_image_pixels or !std.mem.eql(u8, record.mime_type, unit.asset.mime_type) or !std.mem.eql(u8, &std.fmt.bytesToHex(record.sha256, .lower), unit.asset.sha256)) return null;
    return record;
}
pub fn store(a: A, io: std.Io, options: *const Options, context: Context, unit: media.Unit, image: media.Image, identity: []const u8) void {
    const out = cache(options) orelse return;
    if (!validIdentity(identity) or image.data_url.len == 0) return;
    const record: Record = .{ .identity = identity, .sha256 = image.sha256, .mime_type = image.mime_type, .width = image.width, .height = image.height, .bytes = image.bytes };
    const bytes = std.json.Stringify.valueAlloc(a, record, .{}) catch return;
    defer a.free(bytes);
    out.store(a, io, key(context, unit.asset.field), bytes);
}
