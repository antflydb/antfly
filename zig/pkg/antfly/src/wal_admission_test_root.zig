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

pub const antfly_sources = @import("source_owner_control.zig");
pub const consumer_tests_only = true;
comptime {
    @export(&@import("storage/kernel_wal_owner.zig").lastLsnWithAdmission, .{ .name = "antfly_storage_wal_last_lsn_with_admission" });
}
test {
    _ = @import("storage/kernel_wal_owner.zig");
    _ = @import("serverless/api/http_types.zig");
    _ = @import("api/table_writes.zig");
    _ = @import("api/http_server.zig");
    _ = @import("storage/restore_descriptor_admission.zig");
    _ = @import("storage/metadata_read_admission.zig");
    _ = @import("storage/transaction_execution_admission.zig");
    _ = @import("storage/transaction_participant_admission.zig");
    _ = @import("storage/transaction_begin_record_admission.zig");
    _ = @import("storage/restore_plan_codec.zig");
    _ = @import("storage/hot_standby/db_commit.zig");
    _ = @import("storage/hot_standby/owner_preflight_admission.zig");
}
