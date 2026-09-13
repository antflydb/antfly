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

const runtime = @import("data/runtime.zig");
const raft_batch = @import("data/raft_batch.zig");
const runtime_status = @import("api/runtime_status.zig");
const indexes = @import("api/indexes.zig");
const table_writes = @import("antfly_source_root").antfly_sources.table_writes;

// The auth storage adapter deliberately receives storage through an injected
// module to avoid a production import cycle. Focused runtime tests expose the
// same narrow surface as root.zig.
pub const storage_backend_erased = @import("storage/backend_erased.zig");
pub const lsm_backend = @import("storage/lsm_backend.zig");

test {
    _ = runtime;
    _ = raft_batch;
    _ = runtime_status;
    _ = indexes;
    _ = table_writes;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_control.zig");

pub const consumer_tests_only = true;

pub const linked_owner_fixture = @import("api/linked_owner_test_fixture.zig");
