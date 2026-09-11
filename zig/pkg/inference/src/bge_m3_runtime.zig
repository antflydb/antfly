// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

/// One module owns both the internal runtime and the production Node boundary.
/// Keeping them under one root prevents Zig from compiling shared backend files
/// as members of two distinct modules in the managed BGE-M3 benchmark.
pub const internal = @import("inference_internal.zig");
pub const server = @import("server/server.zig");
