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

//! Analyze the actual non-test node caller and opaque API boundary without
//! emitting/linking a storage binary or performing full qualification.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
const DataServer = @import("data/runtime.zig").DataServer;
export fn antfly_check_remote_attempt_bootstrap(raw: *anyopaque) callconv(.c) u8 {
    const server: *DataServer = @ptrCast(@alignCast(raw));
    server.runRemoteAttemptBootstrapRound() catch return 0;
    return 1;
}

// Force the actual provider factory and retained quorum wait callback, not just
// an interface call through a runtime-supplied function pointer.
export fn antfly_check_frozen_read_provider(raw: *anyopaque, proof: *const @import("antfly_local_sources").storage_read_consistency.ReadSafetyBarrier.FrozenProof, control: *const @import("antfly_local_sources").common_read_admission.Port) callconv(.c) u8 {
    const server: *DataServer = @ptrCast(@alignCast(raw));
    server.dataReadSafetyBarrier().validateFrozenWithAdmission(1, proof.*, null, .none, control.*) catch return 0;
    return 1;
}

export fn antfly_check_retained_snapshot_provider(raw: *anyopaque, proof: *const @import("antfly_local_sources").storage_read_consistency.ReadSafetyBarrier.FrozenProof, control: *const @import("antfly_local_sources").common_read_admission.Port) callconv(.c) u8 {
    const server: *DataServer = @ptrCast(@alignCast(raw));
    server.dataReadSafetyBarrier().validateRetainedSnapshotWithAdmission(1, proof.*, null, .none, control.*) catch return 0;
    return 1;
}

// Compile the production bounded node callback as well as the typed factory.
export fn antfly_check_api_memory_try_reserve(raw: *anyopaque, bytes: usize) callconv(.c) c_int {
    const server: *DataServer = @ptrCast(@alignCast(raw));
    const accounting = server.apiMemoryAccounting();
    return @backingInt(accounting.try_reserve_fn.?(accounting.ptr, bytes));
}
