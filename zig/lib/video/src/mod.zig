// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
pub const sampling = @import("sampling.zig");
test {
    _ = sampling;
    _ = @import("sampling_test.zig");
}
