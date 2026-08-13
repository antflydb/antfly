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

const std = @import("std");
const cancellation_mod = @import("../../common/cancellation.zig");
const http_routes = @import("http_routes.zig");

const Allocator = std.mem.Allocator;

pub const HttpRequest = struct {
    method: http_routes.HttpMethod,
    path: []const u8,
    body: []const u8 = "",
    /// Borrowed from the listener and valid only while `handle` is running.
    /// Application work must not retain this callback beyond the request.
    cancellation: cancellation_mod.CancellationToken = .none,

    pub fn ensureActive(self: HttpRequest) !void {
        return self.cancellation.check();
    }
};

pub const HttpResponse = struct {
    status: u16,
    content_type: []u8,
    body: []u8,
    retry_after_seconds: ?u32 = null,

    pub fn deinit(self: *HttpResponse, alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.body);
        self.* = undefined;
    }
};
