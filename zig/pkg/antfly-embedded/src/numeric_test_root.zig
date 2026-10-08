// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Standalone exact-NUMERIC contracts and microbenchmarks. No runtime/server
//! graph is needed to validate the arithmetic or canonical row/key boundary.
test {
    _ = @import("sql/numeric_value.zig");
    _ = @import("sql/numeric_binary.zig");
    _ = @import("sql/numeric_key.zig");
}
