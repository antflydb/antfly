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

//! Request-local physical transfer accounting and strict readback policy.
//! Scopes nest on the submitting thread; unrelated contexts/threads cannot
//! contaminate a request with process-global provider counters.
pub const Audit = struct {
    upload_bytes: u64 = 0,
    download_bytes: u64 = 0,
    host_access_bytes: u64 = 0,
    host_accesses: u64 = 0,
    upload_calls: u64 = 0,
    download_calls: u64 = 0,
    forbid_device_host_access: bool = false,
    parent: ?*Audit = null,

    pub fn recordStats(self: *const Audit, stats: *@import("stats.zig").ExecutionStats) void {
        stats.actual_upload_calls = self.upload_calls;
        stats.actual_upload_bytes = self.upload_bytes;
        stats.actual_download_calls = self.download_calls;
        stats.actual_download_bytes = self.download_bytes;
        stats.device_host_accesses = self.host_accesses;
        stats.device_host_access_bytes = self.host_access_bytes;
    }
};

pub fn validateBackend(kind: anytype) !void {
    switch (kind) {
        .native, .metal, .cuda, .wasm => {},
        else => return error.UnsupportedResidencyAudit,
    }
}
threadlocal var current: ?*Audit = null;

pub fn begin(audit: *Audit) !void {
    var ancestor = current;
    while (ancestor) |value| : (ancestor = value.parent) if (value == audit) return error.TransferAuditAlreadyActive;
    audit.parent = current;
    current = audit;
}
pub fn end() void {
    const active = current orelse return;
    current = active.parent;
    active.parent = null;
}
pub fn checkDeviceHostAccess() error{DeviceReadbackForbidden}!void {
    var ancestor = current;
    while (ancestor) |audit| : (ancestor = audit.parent) if (audit.forbid_device_host_access) return error.DeviceReadbackForbidden;
}
pub fn hostAccess(bytes: usize) !void {
    try checkDeviceHostAccess();
    var ancestor = current;
    while (ancestor) |audit| : (ancestor = audit.parent) {
        audit.host_accesses += 1;
        audit.host_access_bytes += bytes;
    }
}
pub fn upload(bytes: usize) void {
    var ancestor = current;
    while (ancestor) |audit| : (ancestor = audit.parent) {
        audit.upload_calls += 1;
        audit.upload_bytes += bytes;
    }
}
pub fn download(bytes: usize) void {
    var ancestor = current;
    while (ancestor) |audit| : (ancestor = audit.parent) {
        audit.download_calls += 1;
        audit.download_bytes += bytes;
    }
}

test "transfer audit nests counters and cannot weaken an outer strict scope" {
    const std = @import("std");
    var outer = Audit{ .forbid_device_host_access = true };
    try begin(&outer);
    defer end();
    var inner = Audit{};
    try begin(&inner);
    upload(32);
    try std.testing.expectError(error.DeviceReadbackForbidden, checkDeviceHostAccess());
    try std.testing.expectError(error.TransferAuditAlreadyActive, begin(&outer));
    end();
    try std.testing.expectEqual(@as(u64, 32), outer.upload_bytes);
    try std.testing.expectEqual(@as(u64, 32), inner.upload_bytes);
    try std.testing.expectError(error.DeviceReadbackForbidden, hostAccess(16));
    try std.testing.expectEqual(@as(u64, 0), outer.host_accesses);
}
