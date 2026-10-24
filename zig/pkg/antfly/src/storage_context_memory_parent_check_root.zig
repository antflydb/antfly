// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Analyze the actual exported storage context constructor and destructor.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
const provider = @import("capi/server_owner.zig");
const services = @import("storage/kernel_runtime_services.zig");
export fn antfly_check_storage_context_memory_parent(request: *const services.Request, out_context: *?*anyopaque) callconv(.c) u8 {
    if (provider.storageOwnerContextCreateWithRuntime(request, out_context) != .ok) return 0;
    var parent: services.memory.Parent = undefined;
    if (!provider.storageOwnerContextMemoryParent(out_context.*, &parent)) return 0;
    return @intFromBool(provider.storageOwnerContextDestroy(out_context.*) == .ok);
}

// Include the actual private embedded helper and its resource-owning teardown;
// the regular storage context constructor can otherwise bypass this branch.
export fn antfly_check_private_embedded_io(allocator: *const services.memory.Allocator, path: services.memory.Bytes) callconv(.c) u8 {
    if (!allocator.valid()) return 0;
    const public = @import("antfly_local_sources").capi_db;
    const handles = @import("antfly_local_sources").capi_handles;
    const handle = public.openLiteHandleAllocWithRuntime(allocator.asStd(), path.slice(), .{
        .externally_locked = true,
        .open_mode = .writer,
        .profile = .hosted,
    }, false, null, null) catch return 0;
    handles.closeHandle(handle);
    return 1;
}
