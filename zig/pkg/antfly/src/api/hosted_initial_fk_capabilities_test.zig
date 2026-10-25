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

//! Real linked services must never take the legacy/mock capability fallbacks.
const std = @import("std");

test "hosted initial FK real services and facade retain all required capabilities" {
    const services = @import("../metadata/service.zig");
    inline for (.{ services.MetadataService, services.MetadataHttpService }) |Service| {
        inline for (.{
            "projectedStore",
            "ensureLinearizableReadWithContext",
            "cachedCoordinatedDecoderReadiness",
            "ensureTableTopologyProtocolReadyWithContext",
            "ensureReconciliationPlacementReadCutWithContext",
            "preflightInitialPlacementForGroupWithContext",
            "captureProvisioningCatalog",
        }) |capability| {
            if (!@hasDecl(Service, capability)) @compileError("Hosted initial FK service is missing required capability: " ++ capability);
        }
    }
    const Facade = @import("../storage/metadata_raft_apply_client.zig").RaftApplyStore;
    inline for (.{
        "initialGroupReservation",
        "captureProvisioningCatalog",
        "fkInitialCreatePrepareJson",
        "preflightFkInitialCreateCommand",
        "fkInitialCreateStatusJson",
        "fkInitialCreateWorkJson",
        "fkInitialChildDecisionJson",
        "fkInitialParentDecisionJson",
        "fkGenerationTableLockedJson",
        "fkInitialRetirementTicketPageJson",
        "storeRootControlJson",
    }) |capability| {
        if (!@hasDecl(Facade, capability)) @compileError("Hosted initial FK storage facade is missing required capability: " ++ capability);
    }
    try std.testing.expect(true);
}
