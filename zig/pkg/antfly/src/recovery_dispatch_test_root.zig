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

//! Finite recovery dispatch, owner lifecycle, and aggregate reservation tests.
pub const antfly_sources = @import("source_owner_physical.zig");
test {
    _ = @import("antfly_local_sources").storage_background_runtime;
    _ = @import("antfly_local_sources").storage_db_portable_activation_recovery;
    _ = @import("antfly_runtime_fs").threaded_io_limits;
}

test "recovery dispatch aggregate worker reservation" {
    const std = @import("std");
    const limits = @import("antfly_runtime_fs").threaded_io_limits;
    var profile: limits.BackendRuntimeLaneLimits = .{};
    try profile.validate();
    try std.testing.expectEqual(@as(u64, 286), profile.total());
    const fixed = profile.total() - profile.worker_capacity;
    profile.worker_capacity = @intCast(limits.backend_runtime_aggregate - fixed);
    try profile.validate();
    profile.worker_capacity += 1;
    try std.testing.expectError(error.InvalidBackendRuntimeLaneLimits, profile.validate());
}
