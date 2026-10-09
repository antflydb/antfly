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

pub const types = @import("types.zig");
pub const metadata = @import("metadata.zig");
pub const managed = @import("managed.zig");
pub const rest = @import("rest.zig");
pub const parquet_writer = @import("parquet_writer.zig");
pub const avro_writer = @import("avro_writer.zig");
pub const row_commit = @import("row_commit.zig");
const std = @import("std");
pub const Catalog = union(enum) {
    managed: managed.Managed,
    rest: rest.Rest,
    pub fn load(self: *const Catalog, a: std.mem.Allocator) !types.Table {
        return switch (self.*) {
            .managed => |*v| v.load(a),
            .rest => |*v| v.load(a),
        };
    }
    pub fn create(self: *const Catalog, a: std.mem.Allocator, id: []const u8, request: []const u8, timestamp: i64) !types.Table {
        return switch (self.*) {
            .managed => |*v| v.create(a, id, request, timestamp),
            .rest => |*v| v.create(a, id, request, timestamp),
        };
    }
    pub fn commit(self: *const Catalog, a: std.mem.Allocator, request: types.Commit) !types.Table {
        return switch (self.*) {
            .managed => |*v| v.commit(a, request),
            .rest => |*v| v.commit(a, request),
        };
    }
    pub fn resolve(self: *const Catalog, a: std.mem.Allocator, id: []const u8, hash: []const u8) !types.Outcome {
        return switch (self.*) {
            .managed => |*v| v.resolve(a, id, hash),
            .rest => |*v| v.resolve(a, id, hash),
        };
    }
};
test {
    _ = parquet_writer;
    _ = row_commit;
    _ = @import("tests.zig");
}

pub const compaction = @import("compaction.zig");
