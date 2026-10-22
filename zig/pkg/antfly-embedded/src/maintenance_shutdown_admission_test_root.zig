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

//! Actual recurring maintenance callback and physical join ownership.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("storage/db/independent_maintenance.zig");
    _ = @import("common/maintenance_scheduler.zig");
    _ = @import("storage/db/graph_cleanup_owner.zig");
    _ = @import("storage/db/quarantine_recovery.zig");
    _ = @import("storage/db/native_query_warming.zig");
    _ = @import("storage/db/publication_recovery_owner.zig");
    _ = @import("storage/db/schema_reconcile_owner.zig");
    _ = @import("storage/db/portable_activation_recovery.zig");
    _ = @import("storage/db/group_source_owner.zig");
    _ = @import("common/cleanup_wait.zig");
    _ = @import("storage/db/maintenance/transaction_recovery_driver.zig");
}
