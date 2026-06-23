// Copyright 2026 Antfly, Inc.
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

const lite_capabilities = @import("storage/lite/capabilities.zig");

pub const capability_freestanding_build: u32 = 1 << 0;
pub const capability_hosted_profile: u32 = 1 << 1;
pub const capability_manual_maintenance: u32 = 1 << 2;
pub const capability_caller_supplied_artifacts: u32 = 1 << 3;
pub const capability_no_inference_configured_ok: u32 = 1 << 4;
pub const capability_remote_provider_available: u32 = 1 << 5;
pub const capability_local_inference_runtime: u32 = 1 << 6;
pub const capability_background_enrichment_runtime: u32 = 1 << 7;
pub const capability_ttl_cleanup_runtime: u32 = 1 << 8;
pub const capability_transaction_recovery_runtime: u32 = 1 << 9;

fn maskForHostedProfile() u32 {
    const caps = lite_capabilities.capabilitiesForProfile(.hosted);
    var mask: u32 = 0;
    if (caps.freestanding_build) mask |= capability_freestanding_build;
    if (caps.hosted_profile) mask |= capability_hosted_profile;
    if (caps.manual_maintenance) mask |= capability_manual_maintenance;
    if (caps.caller_supplied_artifacts) mask |= capability_caller_supplied_artifacts;
    if (caps.no_inference_configured_ok) mask |= capability_no_inference_configured_ok;
    if (caps.remote_inference_providers) mask |= capability_remote_provider_available;
    if (caps.local_inference_runtime) mask |= capability_local_inference_runtime;
    if (caps.background_enrichment_runtime) mask |= capability_background_enrichment_runtime;
    if (caps.ttl_cleanup_runtime) mask |= capability_ttl_cleanup_runtime;
    if (caps.transaction_recovery_runtime) mask |= capability_transaction_recovery_runtime;
    return mask;
}

export fn antfly_lite_wasm_hosted_capability_mask() u32 {
    return maskForHostedProfile();
}

export fn antfly_lite_wasm_supported_inference_mode_count() u32 {
    return @intCast(lite_capabilities.supported_inference_modes.len);
}

export fn antfly_lite_wasm_available_inference_mode_count() u32 {
    return @intCast(lite_capabilities.capabilitiesForProfile(.hosted).available_inference_modes.len);
}

test "lite wasm hosted profile is manual maintenance and freestanding" {
    const mask = maskForHostedProfile();
    try @import("std").testing.expect((mask & capability_hosted_profile) != 0);
    try @import("std").testing.expect((mask & capability_manual_maintenance) != 0);
    try @import("std").testing.expect((mask & capability_caller_supplied_artifacts) != 0);
    try @import("std").testing.expect((mask & capability_no_inference_configured_ok) != 0);
    try @import("std").testing.expect((mask & capability_local_inference_runtime) == 0);
    try @import("std").testing.expect((mask & capability_background_enrichment_runtime) == 0);
    try @import("std").testing.expect((mask & capability_ttl_cleanup_runtime) == 0);
    try @import("std").testing.expect((mask & capability_transaction_recovery_runtime) == 0);
}
