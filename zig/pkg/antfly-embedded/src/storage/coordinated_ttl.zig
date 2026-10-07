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

//! Storage-owned expiry observations; the API coordinator owns FK actions and
//! distributed commit. Borrowed slices remain valid only for the callback.
pub const Candidate = struct {
    key: []const u8,
    row_version: u64,
    ttl_timestamp_ns: u64,
    /// SHA256 of exact stored primary bytes: a TTL timestamp is not a mutation revision.
    expected_content_digest: [32]u8,
};
pub const Request = struct {
    table_id: u64,
    schema_version: u32,
    ttl_duration_ns: u64,
    ttl_field: []const u8,
    observed_at_unix_ns: u64,
    grace_period_ns: u64,
    candidates: []const Candidate,
};
pub const Port = struct {
    ptr: *anyopaque,
    expire_fn: *const fn (*anyopaque, Request) anyerror!u32,
    pub fn expire(self: Port, request: Request) !u32 {
        return self.expire_fn(self.ptr, request);
    }
};
