// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const portable_backup = @import("storage/portable_backup.zig");

test {
    std.testing.refAllDecls(portable_backup);
}
