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

//! Constructors for ordinary JSON queries. Raw writes retain normal upsert and
//! conditional-write semantics; timestamp equality is not event identity.
use serde_json::{Value, json};

/// Build average/min/max per duration bucket, with optional series grouping.
/// The server validates RFC3339 bounds and the positive fixed duration.
pub fn series_query(
    table: &str,
    timestamp: &str,
    start: &str,
    end: &str,
    measurement: &str,
    every: &str,
    group_by: &[&str],
) -> Result<crate::types::QueryRequest, serde_json::Error> {
    let points = json!({"type":"date_histogram","field":timestamp,
        "fixed_interval":every,"date_bounds":{"start":start,"end":end},
        "min_doc_count":0,"sub_aggregations":{
            "avg":{"type":"avg","field":measurement},
            "min":{"type":"min","field":measurement},
            "max":{"type":"max","field":measurement}}});
    let aggregations: Value = if group_by.is_empty() {
        json!({"points":points})
    } else {
        json!({"series":{"type":"terms","fields":group_by,"size":100,
            "sub_aggregations":{"points":points}}})
    };
    serde_json::from_value(json!({"table":table,"limit":0,
        "filter_query":{"field":timestamp,"start":start,"end":end,
            "inclusive_start":true,"inclusive_end":false},"aggregations":aggregations}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn serializes_the_shared_series_contract() {
        let query = series_query(
            "readings",
            "ts",
            "2026-11-06T00:00:00.000000001Z",
            "2026-11-06T01:00:00Z",
            "temperature",
            "5m",
            &["device_id"],
        )
        .unwrap();
        let value = serde_json::to_value(query).unwrap();
        assert_eq!(
            value["aggregations"]["series"]["sub_aggregations"]["points"]["fixed_interval"],
            "5m"
        );
        assert_eq!(value["filter_query"]["inclusive_end"], false);
        assert_eq!(
            value["filter_query"]["start"],
            "2026-11-06T00:00:00.000000001Z"
        );
    }
    #[test]
    fn measurement_responses_preserve_float64_precision() {
        let result: crate::types::AggregationResult =
            serde_json::from_value(json!({"avg":23.123456789})).unwrap();
        assert_eq!(f64::from(result.avg.unwrap()), 23.123456789_f64);
    }
}
