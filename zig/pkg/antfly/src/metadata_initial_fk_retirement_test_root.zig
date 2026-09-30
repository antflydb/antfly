// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Focused compile root for the bounded hosted initial-FK retirement contract.
const contract = @import("metadata/fk_initial_retirement_contract.zig");
const wire = @import("metadata/fk_initial_retirement_wire.zig");

test {
    _ = contract;
    _ = wire;
}

pub const antfly_sources = @import("source_owner_physical.zig");
