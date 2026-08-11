// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Process-wide thread sizing constraints shared by partitioned runtime units.

/// Every independently generated Zig unit contributes its own signal-stack
/// TLS reservation. The production executable currently links five units and
/// therefore has roughly 1.25 MiB of static TLS on glibc. `pthread_create`
/// rejects requested stacks that cannot accommodate that process-wide TLS
/// layout, even when the worker itself needs very little stack space.
///
/// Keep explicitly bounded infrastructure threads above that floor with room
/// for libc bookkeeping and ordinary call frames. Larger workload-specific
/// defaults (for example HTTP request workers) remain unchanged.
pub const minimum_partitioned_stack_size: usize = 4 * 1024 * 1024;

test "partitioned thread stack floor covers current linked-unit TLS" {
    const std = @import("std");
    try std.testing.expect(minimum_partitioned_stack_size >= 2 * 1024 * 1024);
}
