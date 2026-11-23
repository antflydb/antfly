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

test {
    _ = @import("antfly_local_sources").storage_db_replication_source_apply_store;
    _ = @import("antfly_local_sources").storage_db_replication_source_commit_participant;
    _ = @import("metadata/replication_digest.zig");
    _ = @import("metadata/replication_initialization_attestation.zig");
    _ = @import("system_catalog/replication/payload.zig");
    _ = @import("system_catalog/replication/snapshot_codec.zig");
    _ = @import("api/replication_subscription_capture.zig");
    _ = @import("system_catalog/prepared_transactions.zig");
    _ = @import("metadata/replication_subscription_work.zig");
}
