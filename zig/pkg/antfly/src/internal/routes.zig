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

pub const base = "/internal/v1";
pub const ha = base ++ "/ha";
pub const ha_replication = ha ++ "/replication";
pub const ha_replication_identify = ha_replication ++ "/identify";
pub const ha_replication_slots = ha_replication ++ "/slots";
pub const ha_replication_start = ha_replication ++ "/start";
pub const ha_replication_status = ha_replication ++ "/status";

test "internal routes define HA runtime replication paths" {
    try @import("std").testing.expectEqualStrings("/internal/v1/ha/replication/identify", ha_replication_identify);
    try @import("std").testing.expectEqualStrings("/internal/v1/ha/replication/slots", ha_replication_slots);
    try @import("std").testing.expectEqualStrings("/internal/v1/ha/replication/start", ha_replication_start);
    try @import("std").testing.expectEqualStrings("/internal/v1/ha/replication/status", ha_replication_status);
}
