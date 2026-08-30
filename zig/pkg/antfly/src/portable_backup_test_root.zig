// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const portable_backup = @import("storage/portable_backup.zig");
const backup_bundle = @import("storage/backup_bundle.zig");
const backup_bundle_io = @import("storage/backup_bundle_io.zig");
const backup_repository = @import("storage/backup_repository.zig");

test {
    std.testing.refAllDecls(portable_backup);
    std.testing.refAllDecls(backup_bundle);
    std.testing.refAllDecls(backup_bundle_io);
    std.testing.refAllDecls(backup_repository);
}
