// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Focused backup inventory tests without the application archives.
pub const consumer_tests_only = true;
test {
    _ = @import("storage/db/native_backup_seal.zig");
    _ = @import("storage/db/native_backup.zig");
    _ = @import("storage/db/snapshot_staging.zig");
}
