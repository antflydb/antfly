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

const std = @import("std");
const platform = @import("antfly_platform");

test "public real clock uses the platform wall clock and can sleep" {
    const clock = platform.clock.Clock.real();
    try std.testing.expect(clock.isReal());
    try std.testing.expect(clock.nowRealtimeNs() > 0);
    clock.sleepMs(1);
    // Wall time can be adjusted while sleeping; it need not be monotonic.
    try std.testing.expect(clock.nowRealtimeMs() > 0);
}
