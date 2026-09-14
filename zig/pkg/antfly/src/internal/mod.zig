// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
