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

//! Deletable dependency summaries. Four SHA-256-derived positions use packed
//! counting pages; rare four-bit overflows have exact sparse counters. A hit
//! always requires an exact dependency lookup, so collisions cannot reject or
//! admit policy. Empty pages and overflow records are reclaimed immediately.
const std = @import("std");
const records = @import("resolver_authority.zig");
const A = std.mem.Allocator;
pub const page_bytes = 1024;
pub const page_count = 128;
pub const byte_count = page_bytes * page_count;
const positions = byte_count * 2;
pub fn position(digest: *const [32]u8, index: usize) u32 {
    return std.mem.readInt(u32, digest[index * 4 ..][0..4], .little) & (positions - 1);
}
pub fn contains(bytes: []const u8, digest: *const [32]u8) bool {
    std.debug.assert(bytes.len == byte_count);
    for (0..4) |index| {
        const bit = position(digest, index);
        if (((bytes[bit / 2] >> @as(u3, @intCast((bit % 2) * 4))) & 15) == 0) return false;
    }
    return true;
}
pub const Index = struct {
    a: A,
    backend: records.Backend,
    root: []const u8,
    /// At most four native writes, independent of component size. Inputs are
    /// existing SHA-256 digests, shared with exact root dependency postings.
    pub fn update(self: Index, root_digest: []const u8, digest: *const [32]u8, add: bool) !void {
        var temporary: [16 * 1024]u8 = undefined;
        var frame: std.heap.FixedBufferAllocator = .init(&temporary);
        for (0..4) |index| {
            frame.reset();
            const a = frame.allocator();
            const bit = position(digest, index);
            const page = bit / (page_bytes * 2);
            const offset = (bit / 2) % page_bytes;
            const shift: u3 = @intCast((bit % 2) * 4);
            const key = try std.fmt.allocPrint(a, "{s}dep-count/{s}/{x:0>2}", .{ self.root, root_digest, page });
            var bytes: [page_bytes]u8 = @splat(0);
            if (try self.backend.get(self.backend.ptr, a, key)) |raw| {
                if (raw.len != bytes.len) return error.InvalidCatalogRecord;
                @memcpy(&bytes, raw);
            }
            const count = (bytes[offset] >> shift) & 15;
            var next: u8 = count;
            if (count == 15) {
                const overflow_key = try std.fmt.allocPrint(a, "{s}dep-overflow/{s}/{x:0>5}", .{ self.root, root_digest, bit });
                const raw = try self.backend.get(self.backend.ptr, a, overflow_key);
                const overflow = if (raw) |value| try std.json.parseFromSliceLeaky(u32, a, value, .{}) else 0;
                if (add or overflow != 0) {
                    const value = if (add) try std.math.add(u32, overflow, 1) else overflow - 1;
                    const owned_key = try self.a.dupe(u8, overflow_key);
                    if (value == 0) try self.backend.remove(self.backend.ptr, self.a, owned_key) else try self.backend.put(self.backend.ptr, self.a, owned_key, try std.json.Stringify.valueAlloc(self.a, value, .{}));
                    continue;
                }
                next = 14;
            } else if (add) next += 1 else {
                if (count == 0) return error.InvalidCatalogRecord;
                next -= 1;
            }
            bytes[offset] = (bytes[offset] & ~(@as(u8, 15) << shift)) | (next << shift);
            const owned_key = try self.a.dupe(u8, key);
            if (std.mem.allEqual(u8, &bytes, 0)) try self.backend.remove(self.backend.ptr, self.a, owned_key) else try self.backend.put(self.backend.ptr, self.a, owned_key, try self.a.dupe(u8, &bytes));
        }
    }
    /// One bounded range read loads only nonempty pages. Empty summaries need
    /// no counter allocation or clearing; the caller reuses its root scratch.
    pub fn load(self: Index, a: A, root_digest: []const u8) !?[]u8 {
        const prefix = try std.fmt.allocPrint(a, "{s}dep-count/{s}/", .{ self.root, root_digest });
        const page = try self.backend.scan(self.backend.ptr, a, prefix, null, page_count);
        if (page.rows.len > page_count) return error.InvalidCatalogRecord;
        if (page.next) |next| {
            if ((try self.backend.scan(self.backend.ptr, a, prefix, next, 1)).rows.len != 0) return error.InvalidCatalogRecord;
        }
        if (page.rows.len == 0) return null;
        const output = try a.alloc(u8, byte_count);
        @memset(output, 0);
        for (page.rows) |row| {
            const suffix = row.key[prefix.len..];
            if (suffix.len != 2 or row.value.len != page_bytes) return error.InvalidCatalogRecord;
            const number = std.fmt.parseInt(usize, suffix, 16) catch return error.InvalidCatalogRecord;
            if (number >= page_count) return error.InvalidCatalogRecord;
            @memcpy(output[number * page_bytes ..][0..page_bytes], row.value);
        }
        return output;
    }
};
