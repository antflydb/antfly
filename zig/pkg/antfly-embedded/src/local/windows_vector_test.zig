// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Focused root for vector-block publication and cleanup qualification.
pub const consumer_tests_only = true;

test {
    _ = @import("storage/vector_block_store.zig");
}
