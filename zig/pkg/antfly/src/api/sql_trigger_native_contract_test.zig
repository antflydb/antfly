// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Execute the common native receiver contracts from the API consumer test
//! root; dependency-module tests are not registered in this linked root.
const std = @import("std");
const native = @import("antfly_local_sources").sql_catalog.trigger_native_admission_tests;

test "trigger native preparation fences user authority owner body preimages and no-refire ordered replay" {
    try native.checkPreparationAuthority(std.testing.allocator);
}

test "trigger native v3 wire owns receipt program and proof and forbids public or downgraded admission" {
    try native.checkNativeWireOwnership(std.testing.allocator);
}

test "trigger native integrity-only FK participants retain no row receipts and uncaptured effects fail closed" {
    try @import("sql_trigger_raw.zig").checkIntegrityReceiptSelection(std.testing.allocator);
}

test "trigger native TRUNCATE metadata transport binds empty generation cohort custody and unknown admission" {
    try @import("../metadata/query_population_job.zig").checkTruncateTransport(std.testing.allocator);
}
