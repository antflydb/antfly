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

//! A separately compiled owner using the production implementations. Export
//! only the context operations needed by the real client adapter, plus a
//! test-only snapshot lease operation; no callbacks or cache logic are faked.
pub const antfly_sources = @import("source_owner_storage.zig");
const owner = @import("capi/server_owner.zig");
const abi = @import("kernel_owner_abi");
const scraping = @import("antfly_scraping");

comptime {
    @export(&owner.storageOwnerContextCreate, .{ .name = "antfly_storage_context_create" });
    @export(&owner.storageOwnerContextDestroy, .{ .name = "antfly_storage_context_destroy" });
    @export(&owner.storageOwnerContextConfigureRemoteContentSource, .{ .name = "antfly_storage_context_configure_remote_content_source" });
    @export(&owner.storageOwnerContextConfigureRemoteContentSecurity, .{ .name = "antfly_storage_context_configure_remote_content_security" });
}

export fn remote_content_owner_test_acquire(context: ?*anyopaque, out: *scraping.RemoteContentConfig.Snapshot) callconv(.c) abi.Status {
    const handle = owner.asStorageOwnerContext(context) orelse return .invalid_argument;
    const facade = handle.remoteContent() orelse return .invalid_argument;
    out.* = facade.acquire() catch |err| return switch (err) {
        error.OutOfMemory => .out_of_memory,
        error.Timeout => .timeout,
        error.Canceled => .cancelled,
        else => .resource_temporarily_unavailable,
    };
    return .ok;
}
