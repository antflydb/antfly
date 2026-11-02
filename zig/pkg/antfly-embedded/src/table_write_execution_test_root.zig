// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Private callback contract tests, without physical storage or SQL ingress.
pub const antfly_sources = struct {};

test {
    @import("std").testing.refAllDecls(@import("api/table_write_source.zig").consumer_tests);
}
