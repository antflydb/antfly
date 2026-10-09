// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

pub const types = @import("types.zig");
pub const metadata = @import("metadata.zig");
pub const managed = @import("managed.zig");
pub const rest = @import("rest.zig");
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
    _ = @import("tests.zig");
}
