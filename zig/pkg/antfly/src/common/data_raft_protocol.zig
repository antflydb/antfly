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

/// Version 1 adds the internal `_timestamp_ns` field to data-Raft batch log
/// entries. Version 2 adds the fail-closed, durable activation barrier used to
/// turn that format on without retaining capability probes in the write path.
pub const batch_protocol_version: u16 = 2;
pub const batch_timestamp_protocol_version: u16 = 1;
pub const batch_activation_barrier_protocol_version: u16 = 2;
