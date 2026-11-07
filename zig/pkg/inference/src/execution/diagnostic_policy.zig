// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One cold opt-in shared by managed model and standalone pipeline loaders.
//! No environment lookup occurs in a numerical kernel or publication callback.
pub const Policy = struct {
    enabled: bool = false,
    pub fn fromEnvironment() Policy {
        return .{ .enabled = @import("antfly_platform").env.getenvBool("ANTFLY_EXECUTION_DIAGNOSTICS") };
    }
};
