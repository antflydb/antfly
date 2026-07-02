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

package metadata

func exactQueryHitsTotal(value uint64) QueryHitsTotal {
	return QueryHitsTotal{
		Value:    value,
		Relation: QueryHitsTotalRelationExact,
	}
}

func exactQueryHitsTotalFromInt(value int) QueryHitsTotal {
	if value <= 0 {
		return exactQueryHitsTotal(0)
	}
	return exactQueryHitsTotal(uint64(value))
}

func gteQueryHitsTotal(value uint64) QueryHitsTotal {
	return QueryHitsTotal{
		Value:    value,
		Relation: QueryHitsTotalRelationGte,
	}
}

func queryHitsTotalValue(total QueryHitsTotal) uint64 {
	return total.Value
}
