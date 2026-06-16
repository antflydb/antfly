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

pub const openapi = @import("antfly_admin_openapi");
pub const routes = @import("routes.zig");

pub const ReplicationSlotCreateRequest = openapi.ReplicationSlotCreateRequest;
pub const BaseBackupStartRequest = openapi.BaseBackupStartRequest;
pub const BaseBackupManifestPathRequest = openapi.BaseBackupManifestPathRequest;
pub const StandbyBootstrapRequest = openapi.StandbyBootstrapRequest;
pub const FenceAcquireRequest = openapi.FenceAcquireRequest;
pub const PromotionAssessRequest = openapi.PromotionAssessRequest;

test {
    _ = openapi;
    _ = routes;
}
