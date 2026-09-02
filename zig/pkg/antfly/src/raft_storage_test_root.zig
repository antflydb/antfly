// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");
const file_snapshot_store = @import("raft/storage/file_snapshot_store.zig");

test "raft snapshot storage tests are reachable" {
    std.testing.refAllDecls(file_snapshot_store);
}
