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
