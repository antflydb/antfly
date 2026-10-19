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

pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = @import("local_test_sources.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
test {
    _ = @import("storage/kernel_wal_owner.zig");
    _ = @import("storage/hot_standby/primary.zig");
    _ = @import("storage/hot_standby/replication_log.zig");
    _ = @import("antfly_local_sources").storage_wal;
    _ = @import("antfly_local_sources").storage_docstore;
    _ = @import("antfly_local_sources").storage_db_durable_outbox_store;
    _ = @import("antfly_local_sources").section_typed_doc_values;
}

comptime {
    @export(&@import("storage/kernel_wal_owner.zig").ownedReadBegin, .{ .name = "antfly_storage_wal_owned_read_begin" });
    @export(&@import("storage/kernel_wal_owner.zig").ownedReadStep, .{ .name = "antfly_storage_wal_owned_read_step" });
    @export(&@import("storage/kernel_wal_owner.zig").ownedReadChunk, .{ .name = "antfly_storage_wal_owned_read_chunk" });
    @export(&@import("storage/kernel_wal_owner.zig").ownedReadDestroy, .{ .name = "antfly_storage_wal_owned_read_destroy" });

    @export(&@import("storage/kernel_wal_owner.zig").iterate, .{ .name = "antfly_storage_wal_iterate" });
    @export(&@import("storage/kernel_wal_owner.zig").readWithAllocator, .{ .name = "antfly_storage_wal_read_with_allocator" });
    @export(&@import("storage/kernel_wal_owner.zig").iterateWithAllocator, .{ .name = "antfly_storage_wal_iterate_with_allocator" });
    @export(&@import("storage/kernel_wal_owner.zig").append, .{ .name = "antfly_storage_wal_append" });
    @export(&@import("storage/kernel_wal_owner.zig").openWithAllocator, .{ .name = "antfly_storage_wal_open_with_allocator" });
    @export(&@import("storage/kernel_wal_owner.zig").close, .{ .name = "antfly_storage_wal_close" });
    @export(&@import("storage/kernel_wal_owner.zig").recoveryPosition, .{ .name = "antfly_storage_wal_recovery_position" });
    @export(&@import("storage/kernel_wal_owner.zig").appendIfRecoveryPosition, .{ .name = "antfly_storage_wal_append_if_recovery_position" });
    @export(&@import("storage/kernel_wal_owner.zig").read, .{ .name = "antfly_storage_wal_read" });
    @export(&@import("capi/server_owner.zig").storageOwnerBufferDestroy, .{ .name = "antfly_storage_owner_buffer_destroy" });
    @export(&@import("storage/kernel_wal_owner.zig").readWithAdmission, .{ .name = "antfly_storage_wal_read_with_admission" });
    @export(&@import("storage/kernel_wal_owner.zig").lastLsnWithAdmission, .{ .name = "antfly_storage_wal_last_lsn_with_admission" });
}
