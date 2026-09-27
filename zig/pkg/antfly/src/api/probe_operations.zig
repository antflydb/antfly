// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Typed Kubernetes probe operations. Route placement and JSON encoding are
//! transport concerns; readiness evaluation is supplied explicitly.

const std = @import("std");
const operation = @import("operation.zig");

pub const Status = enum {
    ok,
    ready,
    not_ready,
};

pub const ReadinessSource = struct {
    ptr: *anyopaque,
    check_fn: *const fn (*anyopaque) anyerror!void,

    pub fn check(self: ReadinessSource) !void {
        try self.check_fn(self.ptr);
    }
};

pub const Operations = struct {
    readiness: ReadinessSource,

    pub fn health(_: Operations, request: operation.RequestContext) operation.ApiError!Status {
        try request.ensureActive();
        return .ok;
    }

    pub fn ready(self: Operations, request: operation.RequestContext) operation.ApiError!Status {
        try request.ensureActive();
        self.readiness.check() catch return .not_ready;
        return .ready;
    }
};

test "probe operations distinguish health from readiness" {
    const Source = struct {
        ready: bool,

        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.ready) return error.NotReady;
        }
    };
    var source = Source{ .ready = false };
    const probes = Operations{ .readiness = .{ .ptr = &source, .check_fn = Source.check } };
    try std.testing.expectEqual(Status.ok, try probes.health(.{}));
    try std.testing.expectEqual(Status.not_ready, try probes.ready(.{}));
    source.ready = true;
    try std.testing.expectEqual(Status.ready, try probes.ready(.{}));
}
