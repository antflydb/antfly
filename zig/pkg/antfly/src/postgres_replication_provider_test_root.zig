// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

pub const antfly_sources = @import("source_owner_physical.zig");

test {
    _ = @import("foreign/postgres_libpq.zig");
}
