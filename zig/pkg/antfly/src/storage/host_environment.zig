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
