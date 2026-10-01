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

//! Linked-control hosted initial-FK owner integration composition.

const hosted_initial_fk_e2e = @import("api/hosted_initial_fk_e2e.zig");

pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
    _ = hosted_initial_fk_e2e;
    _ = @import("api/hosted_initial_fk_fault_e2e.zig");
    _ = @import("api/hosted_initial_fk_transfer_e2e.zig");
    _ = @import("api/hosted_initial_fk_offline_e2e.zig");
    _ = @import("api/hosted_initial_fk_capabilities_test.zig");
}
