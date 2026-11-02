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

pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("storage/db/apply_rw_lock.zig");
    _ = @import("storage/db/mutation_admission.zig");
    _ = @import("storage/pressure_admission.zig");
    _ = @import("storage/resource_manager.zig");
    _ = @import("storage/db/mutation_commit.zig");
    _ = @import("storage/db/commit_integration.zig");
    _ = @import("storage/db/snapshot_admission.zig");
    _ = @import("api/mutation_admission_controls.zig");
    _ = @import("api/batch_execution_admission.zig");
    _ = @import("api/table_write_source.zig");
    _ = @import("storage/db/query/search_exec.zig");
}
