// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Algebraic value, wire, and proof contracts used by distributed query
//! control. The physical index and planner implementations remain owned by
//! the compiled storage kernel.

pub const token = @import("token.zig");
pub const value = @import("value.zig");
pub const algebra = @import("algebra.zig");
pub const law = @import("law.zig");
pub const fact = @import("fact.zig");
pub const pathfact = @import("pathfact.zig");
pub const lexical = @import("lexical.zig");
pub const tensor = @import("tensor.zig");
pub const vector = @import("vector.zig");
pub const path = @import("path.zig");
pub const adaptive = @import("adaptive.zig");
pub const distributed = @import("distributed.zig");
pub const cylinder = @import("cylinder.zig");
pub const join = @import("join.zig");
pub const ir = @import("ir.zig");
pub const schema_capability = @import("schema_capability.zig");
pub const symbol = @import("symbol.zig");
pub const planner = @import("planner_control.zig");
pub const index = @import("index_control.zig");
