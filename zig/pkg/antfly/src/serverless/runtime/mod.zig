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

pub const manager = @import("manager.zig");
pub const bootstrap = @import("bootstrap.zig");

pub const RuntimeConfig = manager.RuntimeConfig;
pub const RuntimeRunStats = manager.RuntimeRunStats;
pub const ManagedRuntime = manager.ManagedRuntime;
pub const BootstrapConfig = bootstrap.BootstrapConfig;
pub const RuntimeStatus = bootstrap.RuntimeStatus;
pub const RuntimeRole = @import("../api/types.zig").RuntimeRole;
pub const OwnedStack = bootstrap.OwnedStack;
pub const validateBootstrapConfig = bootstrap.validateConfig;
pub const runtimeStatusAlloc = bootstrap.runtimeStatusAlloc;

test "serverless runtime module compiles" {
    _ = manager;
    _ = bootstrap;
    _ = RuntimeConfig;
    _ = RuntimeRunStats;
    _ = ManagedRuntime;
    _ = BootstrapConfig;
    _ = RuntimeStatus;
    _ = RuntimeRole;
    _ = OwnedStack;
    _ = validateBootstrapConfig;
    _ = runtimeStatusAlloc;
}
