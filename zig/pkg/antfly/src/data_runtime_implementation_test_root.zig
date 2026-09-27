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
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");
comptime {
    _ = @import("data/runtime.zig").implementation_tests;
    _ = @import("storage/db/enrichment/enrichment_runtime.zig");
    _ = @import("storage/hot_standby/restore_terminal_ledger.zig");
    _ = @import("data/storage/shard_state_store.zig");
}
