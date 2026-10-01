// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");
pub const antfly_sources = @import("source_owner_physical.zig");
const native_snapshot = @import("raft/storage/native_snapshot.zig");
const file_snapshot_store = @import("raft/storage/file_snapshot_store.zig");

test "raft snapshot storage tests are reachable" {
    std.testing.refAllDecls(file_snapshot_store);
    std.testing.refAllDecls(native_snapshot);
}
