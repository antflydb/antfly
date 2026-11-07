// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
pub const Kind = enum(u32) { linear = 1, layer_norm = 2, rms_norm = 3 };
pub const Stamp = extern struct { kind: Kind, slot: u32, generation: u64 };
