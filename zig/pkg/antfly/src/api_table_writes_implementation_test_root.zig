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

pub const antfly_sources = @import("source_owner_physical.zig");
pub const implementation_tests_only = true;
test {
    _ = @import("api/backup_cohort_physical_test.zig");
}
comptime {
    _ = @import("api/table_reads.zig").implementation_tests;
    _ = @import("metadata/table_provisioner.zig").implementation_tests;
    _ = @import("api/distributed_txn.zig").implementation_tests;
    _ = @import("api/provisioned_storage.zig").implementation_tests;
    _ = @import("api/table_writes.zig").implementation_tests;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
