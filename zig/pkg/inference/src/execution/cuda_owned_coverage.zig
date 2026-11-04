// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! CUDA entrypoint retains its target-specific contract; shared slot/resource
//! accounting is also used by separately attested Metal Frame executors.
const ops = @import("../ops/ops.zig");
const device = @import("device_owned_coverage.zig");
pub fn services(backend: ops.ComputeBackend) ?*const @import("../ops/kernel_backend.zig").OwnedPayloadServices {
    if (backend.kind() != .cuda) return null;
    return device.services(backend);
}
pub const assess = device.assess;
