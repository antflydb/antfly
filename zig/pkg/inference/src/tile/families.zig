// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Production instances use the qualified native schedules. The semantic tile
//! primitives remain shared by CPU, Metal, CUDA and the OpenCL test dialect.
const r = @import("region.zig");
pub const Region = r.Region;
pub const regions = @import("families/native.zig").regions;
