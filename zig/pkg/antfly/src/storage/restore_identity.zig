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

//! Catalog binding for an imported generation, without physical storage types.
const std = @import("std");

pub const Identity = struct {
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,
    artifact_sha256: []const u8,
    native_manifest_size_bytes: u64 = 0,
    native_manifest_sha256: []const u8 = "",

    pub fn eql(self: Identity, other: Identity) bool {
        if (self.native_manifest_size_bytes != other.native_manifest_size_bytes) return false;
        inline for (string_fields) |field| {
            if (!std.mem.eql(u8, @field(self, field), @field(other, field))) return false;
        }
        return true;
    }

    pub fn clone(self: Identity, alloc: std.mem.Allocator) !Identity {
        var result = self;
        var copied: usize = 0;
        errdefer inline for (string_fields, 0..) |field, index| {
            if (index < copied) alloc.free(@field(result, field));
        };
        inline for (string_fields) |field| {
            @field(result, field) = try alloc.dupe(u8, @field(self, field));
            copied += 1;
        }
        return result;
    }

    pub fn deinit(self: *Identity, alloc: std.mem.Allocator) void {
        inline for (string_fields) |field| alloc.free(@field(self, field));
        self.* = undefined;
    }

    const string_fields = .{ "backup_id", "location", "snapshot_path", "artifact_sha256", "native_manifest_sha256" };
};
