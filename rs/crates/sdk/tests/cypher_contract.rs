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

use antfly_sdk::types;
use serde_json::json;

#[test]
fn cypher_integer_values_and_unknown_write_receipts_round_trip() {
    let value = json!({
        "wire_version": 1,
        "columns": ["n"],
        "rows": [[{"type": "integer", "value": "9223372036854775807"}]],
        "plan": [], "stats": {"search_approximate": true},
        "graph_incarnation": "18446744073709551615", "graph_revision": "1",
        "complete": false, "read_only": false,
        "commit": {"transaction_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "status": "unknown"}
    });
    let result: types::GraphQueryResult = serde_json::from_value(value).unwrap();
    let encoded = serde_json::to_value(result).unwrap();
    assert_eq!(encoded["rows"][0][0]["value"], "9223372036854775807");
    assert_eq!(encoded["graph_incarnation"], "18446744073709551615");
    assert_eq!(encoded["commit"]["status"], "unknown");
    assert_eq!(encoded["read_only"], false);
}

#[test]
fn cypher_parameter_maps_keep_user_type_keys_as_data() {
    let source = json!({
        "query": "RETURN $payload",
        "typed_parameters": {"type": "map", "value": {
            "payload": {"type": "map", "value": {
                "type": {"type": "string", "value": "node"},
                "n": {"type": "integer", "value": "-9223372036854775808"}
            }}
        }}
    });
    let request: types::GraphQueryRequest = serde_json::from_value(source.clone()).unwrap();
    let encoded = serde_json::to_value(request).unwrap();
    assert_eq!(encoded["typed_parameters"], source["typed_parameters"]);
}
