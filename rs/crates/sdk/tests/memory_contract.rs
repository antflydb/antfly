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

use antfly_sdk::types::{MemoryRecallRequest, MemorySpaceConfig};

#[test]
fn memory_native_uint64_configuration_round_trips() {
    let config: MemorySpaceConfig = serde_json::from_value(serde_json::json!({
        "retention_ms": u64::MAX,
    }))
    .unwrap();
    let encoded = serde_json::to_value(config).unwrap();
    assert_eq!(encoded["retention_ms"].as_u64(), Some(u64::MAX));
}

#[test]
fn memory_provisional_filter_distinguishes_false_from_the_native_default() {
    let defaults: MemoryRecallRequest = serde_json::from_str("{}").unwrap();
    let excluded: MemoryRecallRequest =
        serde_json::from_str(r#"{"include_provisional":false}"#).unwrap();
    let default_json = serde_json::to_value(defaults).unwrap();
    let excluded_json = serde_json::to_value(excluded).unwrap();
    assert_eq!(default_json["include_provisional"].as_bool(), Some(true));
    assert_eq!(excluded_json["include_provisional"].as_bool(), Some(false));
}
