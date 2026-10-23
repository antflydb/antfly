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

pub const openapi = @import("antfly_internal_openapi");
pub const routes = @import("routes.zig");

pub const StandbyIdentifySystemResponse = openapi.StandbyIdentifySystemResponse;
pub const StandbyCreateReplicationSlotRequest = openapi.StandbyCreateReplicationSlotRequest;
pub const StandbyReplicationSlotResponse = openapi.StandbyReplicationSlotResponse;
pub const StandbyStartReplicationRequest = openapi.StandbyStartReplicationRequest;
pub const StandbyStartReplicationResponse = openapi.StandbyStartReplicationResponse;
pub const StandbyStatusUpdateRequest = openapi.StandbyStatusUpdateRequest;
pub const StandbyStatusUpdateResponse = openapi.StandbyStatusUpdateResponse;

test {
    _ = openapi;
    _ = routes;
}

// Deprecated aliases, remove after 0.4.
pub const HAIdentifySystemResponse = StandbyIdentifySystemResponse;
pub const HACreateReplicationSlotRequest = StandbyCreateReplicationSlotRequest;
pub const HAReplicationSlotResponse = StandbyReplicationSlotResponse;
pub const HAStartReplicationRequest = StandbyStartReplicationRequest;
pub const HAStartReplicationResponse = StandbyStartReplicationResponse;
pub const HAStandbyStatusUpdateRequest = StandbyStatusUpdateRequest;
pub const HAStandbyStatusUpdateResponse = StandbyStatusUpdateResponse;
