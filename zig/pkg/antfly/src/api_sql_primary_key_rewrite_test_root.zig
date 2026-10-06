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

//! Linked-control owner composition for mounted SQL primary-key rewrites.

const sql_primary_key_rewrite_integration_test = @import("api/sql_primary_key_rewrite_integration_test.zig");

// The private online-merge donor port is only installed by the linked-control
// runtime. A physical/monolithic API root cannot exercise this protocol.
pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = sql_primary_key_rewrite_integration_test;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
