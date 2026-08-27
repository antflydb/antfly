// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const platform = @import("antfly_platform");

fn cachedEnvBool(comptime name: [*:0]const u8) bool {
    // Keep each flag's hot-path cache distinct. Referencing the comptime name
    // in the nested type prevents Zig from deduplicating the cache across
    // instantiations.
    const Cache = struct {
        const key = name;
        var value: ?bool = null;
    };
    if (Cache.value) |value| return value;
    const value = platform.env.getenvBool(Cache.key);
    Cache.value = value;
    return value;
}

pub fn highMemoryFastPathEnabledForValues(enable: bool, disable: bool) bool {
    return enable and !disable;
}

pub fn highMemoryFeatureEnabledForValues(
    umbrella: bool,
    enable: bool,
    disable: bool,
) bool {
    return (umbrella or enable) and !disable;
}

pub fn highMemoryFastPathEnabled() bool {
    return highMemoryFastPathEnabledForValues(
        cachedEnvBool("TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH"),
        cachedEnvBool("TERMITE_METAL_DISABLE_A4B_HIGH_MEMORY_FAST_PATH"),
    );
}

pub fn highMemoryFeatureEnabled(
    comptime enable_name: [*:0]const u8,
    comptime disable_name: [*:0]const u8,
) bool {
    return highMemoryFeatureEnabledForValues(
        highMemoryFastPathEnabled(),
        cachedEnvBool(enable_name),
        cachedEnvBool(disable_name),
    );
}

test "A4B high-memory feature flags preserve disable precedence" {
    try std.testing.expect(highMemoryFastPathEnabledForValues(true, false));
    try std.testing.expect(!highMemoryFastPathEnabledForValues(true, true));
    try std.testing.expect(highMemoryFeatureEnabledForValues(true, false, false));
    try std.testing.expect(highMemoryFeatureEnabledForValues(false, true, false));
    try std.testing.expect(!highMemoryFeatureEnabledForValues(true, true, true));
}
