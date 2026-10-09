// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! This root owns physical backend sources, so filters cannot silently omit
//! tests declared in a foreign source catalog module.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    @import("std").testing.refAllDecls(@import("storage/lsm_backend/local_reader.zig"));
    @import("std").testing.refAllDecls(@import("storage/lite/docstore.zig"));
    @import("std").testing.refAllDecls(@import("storage/lsm_backend.zig"));
}
