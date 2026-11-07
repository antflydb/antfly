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

//! Actual physical FK control receipts and schema commit/restart qualification.
pub const antfly_sources = @import("source_owner_embedded_db.zig");

test {
    _ = @import("storage/db/db.zig");
    _ = @import("storage/transactions.zig");
    _ = @import("storage/db/row_policy_schema_pair_test.zig");
    _ = @import("storage/db/relational_integrity_generation_admission.zig");
    _ = @import("storage/db/native_empty_generation.zig");
    _ = @import("api/backups.zig");
    _ = @import("metadata/backup_cohort.zig").consumer_tests;
}
