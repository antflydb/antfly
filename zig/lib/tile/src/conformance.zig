// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Portable operation fixtures consumed by CPU tests and GPU qualification.
const fexpr = @import("fexpr.zig");
pub const Programs = struct {
    pub const projection: @import("gemm.zig").Gemm = .{ .name = "contract_projection", .epilogue = fexpr.acc.plus(.bias).roundF16().siluOf().times(.gate).plus(.residual).roundBf16() };
    pub const rms: @import("norm.zig").RmsNorm = .{ .name = "contract_rms" };
    pub const mean: @import("norm.zig").MaskedMean = .{ .name = "contract_masked_mean" };
    pub const gather: @import("layout.zig").Copy = .{ .name = "contract_gather", .element = .u32, .gather = true };
    pub const copy: @import("layout.zig").Copy = .{ .name = "contract_copy", .element = .f16 };
};
