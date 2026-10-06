// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

// The module root includes shared datetime primitives used by scalar binding.
pub const main = @import("sql/compiler_bench.zig").main;

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
