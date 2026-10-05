// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2

test "evented enrichment executor initializes when supported" {
    try @import("enrichment_executor.zig").testEventedExecutor(true);
}
