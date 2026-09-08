// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Allocation-free, portable hashing primitives with target-gated acceleration.
pub const Crc32 = @import("crc32.zig").Crc32;

test {
    _ = @import("crc32.zig");
}
