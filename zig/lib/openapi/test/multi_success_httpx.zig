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

//! Owned in-memory HTTP response fixture; no listener or network required.
const std = @import("std");
pub const Client = struct {
    pub fn get(_: *Client, _: []const u8, _: anytype) !Response {
        return Response.init(202, "{\"name\":\"documents\"}");
    }
};
pub const Response = struct {
    status: struct { code: u16 },
    body: ?[]const u8,
    allocator: std.mem.Allocator,

    pub fn init(code: u16, body: ?[]const u8) !Response {
        return .{
            .status = .{ .code = code },
            .body = if (body) |bytes| try std.testing.allocator.dupe(u8, bytes) else null,
            .allocator = std.testing.allocator,
        };
    }
    pub fn ok(self: Response) bool {
        return self.status.code >= 200 and self.status.code < 300;
    }
    pub fn deinit(self: *Response) void {
        if (self.body) |body| self.allocator.free(body);
        self.body = null;
    }
};
