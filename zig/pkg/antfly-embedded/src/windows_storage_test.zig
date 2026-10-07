// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// A small root for native/Windows storage I/O qualification without building
// the full application, inference stack, or unrelated package test runners.
test {
    _ = @import("storage/lsm_backend/storage_io.zig");
}
