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
