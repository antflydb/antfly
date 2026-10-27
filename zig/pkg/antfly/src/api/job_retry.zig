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
const retry_base_ms: u64 = 250;
const retry_max_ms: u64 = 30 * std.time.ms_per_s;

pub fn retryDelayMs(job_id: u64, attempt_id: u64) u64 {
    const shift: u6 = @intCast(@min(attempt_id, 16));
    const exponential = @min(retry_max_ms, retry_base_ms *| (@as(u64, 1) << shift));
    var hasher = std.hash.Wyhash.init(job_id);
    hasher.update(std.mem.asBytes(&attempt_id));
    const jitter_span = @max(@as(u64, 1), exponential / 4);
    return @min(retry_max_ms, exponential +| (hasher.final() % jitter_span));
}
