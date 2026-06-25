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

const std = @import("std");

const backend_types = @import("../../storage/backend_types.zig");
const db_mod = @import("../../storage/db/mod.zig");

pub const min_batch_ops: usize = 100;
pub const max_window_ops: usize = 25_000;
pub const max_hbc_leaf_splits_per_publish: usize = 256;

// Client-side bulk loads often arrive as serial HTTP chunks. Finish implicit
// dense bulk ingest windows on max ops or idle, not elapsed open time, so an
// active upload does not start HBC replay/publish work mid-stream.
pub const max_idle_ns: u64 = 2 * std.time.ns_per_s;

pub const finish_options: backend_types.BulkIngestFinishOptions = .{
    .compact = false,
    .flush = true,
    .max_deferred_l0_runs = 64,
    .max_deferred_hbc_leaf_splits_per_publish = max_hbc_leaf_splits_per_publish,
};

pub fn shouldDrainManagedDbAfterBatch(sync_level: db_mod.types.SyncLevel) bool {
    // Request latency for weak sync levels must not depend on derived replay.
    // Pending replay is durable in the journal and is resumed by later writes,
    // explicit catch-up, or bulk-session finish.
    return switch (sync_level) {
        .propose, .write, .enrichments => false,
        .full_text, .aknn, .full_index => false,
    };
}

pub fn shouldDrainCachedManagedDbAfterBatch(sync_level: db_mod.types.SyncLevel) bool {
    _ = sync_level;
    return false;
}

pub fn autoBulkIngestBatchOps(req: db_mod.types.BatchRequest) usize {
    _ = req;
    // Weak-sync writes are already durable in the primary store plus replay
    // journal. Opening a foreground HBC bulk session here suppresses dense
    // replay notifications for the entire active upload, so indexing only
    // becomes query-visible after the writer goes idle. Let the background
    // derived executor own dense bulk sessions and publish bounded windows.
    return 0;
}

pub fn autoBulkIngestGroupBatchOps(group: anytype, sync_level: db_mod.types.SyncLevel) usize {
    _ = group;
    _ = sync_level;
    return 0;
}

test "weak sync levels do not drain managed db after batch" {
    try std.testing.expect(!shouldDrainManagedDbAfterBatch(.propose));
    try std.testing.expect(!shouldDrainManagedDbAfterBatch(.write));
}
