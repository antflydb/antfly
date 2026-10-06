// Copyright 2026 Antfly, Inc.
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

//! API import facade for the durable metadata-owned cohort protocol.
const cohort = @import("../metadata/backup_cohort.zig");
pub const Phase = cohort.Phase;
pub const Result = cohort.Result;
pub const Owner = cohort.Owner;
pub const State = cohort.State;
pub const Observation = cohort.Observation;
pub const step = cohort.step;
pub const cancel = cohort.cancel;
