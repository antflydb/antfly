// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Checked, by-value resource participant snapshots. No pointer to the caller's
//! temporary ABI frame is retained; only its explicitly borrowed physical owner.
const abi = @import("antfly_runtime_abi").runnable_abi;
const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

comptime {
    for (@typeInfo(native.GrantResources.Probe).@"enum".field_names) |name| {
        if (@backingInt(@field(native.GrantResources.Probe, name)) != @backingInt(@field(abi.ResourceBundle.Probe, name)))
            @compileError("resource probe ABI discriminant mismatch");
    }
    for (@typeInfo(native.GrantResources.Acquire).@"enum".field_names) |name| {
        if (@backingInt(@field(native.GrantResources.Acquire, name)) != @backingInt(@field(abi.ResourceBundle.Acquire, name)))
            @compileError("resource acquisition ABI discriminant mismatch");
    }
}
pub fn receive(raw: *const abi.ResourceBundle) !native.GrantResources {
    const value = try abi.ResourceBundle.read(raw);
    return .{ .context = value.context, .probe = value.probe, .acquire = value.acquire };
}
pub fn send(value: native.GrantResources) abi.ResourceBundle {
    return .{ .context = value.context, .probe = value.probe, .acquire = value.acquire };
}
