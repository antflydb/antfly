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

//! Retained, C-layout byte channel for task-scoped document persistence.
//! The host authorizes one run/principal before issuing this capability. Calls
//! exchange bounded JSON control metadata and raw payload bytes. Every response
//! remains owned by its producer until its explicit destructor runs.
const error_abi = @import("runtime_error_abi.zig");
const Bytes = @import("runtime_http_abi.zig").Bytes;
pub const Response = extern struct {
    handle: ?*anyopaque = null,
    metadata: Bytes = .{ .ptr = "", .len = 0 },
    body: Bytes = .{ .ptr = "", .len = 0 },
};
pub const View = extern struct {
    context: ?*anyopaque = null,
    retain_context: ?*const fn (*anyopaque) callconv(.c) u8 = null,
    release_context: ?*const fn (*anyopaque) callconv(.c) void = null,
    invoke: ?*const fn (*anyopaque, Bytes, Bytes, *Response) callconv(.c) error_abi.Status = null,
    destroy_response: ?*const fn (*anyopaque, *anyopaque) callconv(.c) void = null,
    pub fn configured(self: View) bool {
        return self.context != null and self.retain_context != null and self.release_context != null and self.invoke != null and self.destroy_response != null;
    }
    pub fn retain(self: View) !void {
        if (!self.configured()) return error.DocumentStorageNotConfigured;
        if (self.retain_context.?(self.context.?) == 0) return error.DocumentRunLeaseLost;
    }
    pub fn release(self: View) void {
        self.release_context.?(self.context.?);
    }
    pub fn destroy(self: View, response: Response) void {
        if (response.handle) |handle| self.destroy_response.?(self.context.?, handle);
    }
};
