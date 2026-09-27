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

const std = @import("std");
const lsm_backend = @import("lsm_backend/mod.zig");
const object_storage = @import("object_storage.zig");

const Allocator = std.mem.Allocator;

/// Shared host bundle that can provide both engine-level file storage and
/// higher-level object/blob storage from one embedding context.
pub const HostEnvironment = struct {
    storage: lsm_backend.HostStorage,
    object_storage: object_storage.HostObjectStorage,

    pub fn initSharedContext(
        allocator: Allocator,
        ptr: *anyopaque,
        storage_vtable: *const lsm_backend.Storage.VTable,
        object_vtable: *const object_storage.ObjectStorage.VTable,
    ) HostEnvironment {
        return .{
            .storage = lsm_backend.HostStorage.init(ptr, storage_vtable),
            .object_storage = object_storage.HostObjectStorage.init(allocator, ptr, object_vtable),
        };
    }

    pub fn initSplit(
        allocator: Allocator,
        storage_ptr: *anyopaque,
        storage_vtable: *const lsm_backend.Storage.VTable,
        object_ptr: *anyopaque,
        object_vtable: *const object_storage.ObjectStorage.VTable,
    ) HostEnvironment {
        return .{
            .storage = lsm_backend.HostStorage.init(storage_ptr, storage_vtable),
            .object_storage = object_storage.HostObjectStorage.init(allocator, object_ptr, object_vtable),
        };
    }
};
