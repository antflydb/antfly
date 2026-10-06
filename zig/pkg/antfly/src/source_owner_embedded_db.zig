// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Browser/local DB profile; native owner configuration is not a browser dependency.
pub const physical_db = @import("antfly_local_sources").storage_db_db;
pub const selected_db = @import("antfly_local_sources").storage_db_mod;
pub const local_query = @import("antfly_local_sources").storage_local_query;

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
