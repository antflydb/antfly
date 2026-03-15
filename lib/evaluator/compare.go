// Copyright 2025 Antfly, Inc.
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

// Package evaluator provides a parsed representation of Bleve-style filter
// queries with in-memory evaluation, plus shared comparison utilities used
// across the Antfly codebase.
package evaluator

// ToFloat64 attempts to convert a value to float64.
// Returns (0, false) for non-numeric types.
func ToFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	default:
		return 0, false
	}
}

// ValuesEqual checks if two values are equal, handling type coercion.
// String values are compared directly. Numeric types are coerced to float64.
// Falls back to interface equality for other types.
func ValuesEqual(a, b any) bool {
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			return as == bs
		}
	}

	af, aok := ToFloat64(a)
	bf, bok := ToFloat64(b)
	if aok && bok {
		return af == bf
	}

	return a == b
}

// CompareOrdered compares two ordered values, returning -1, 0, or 1.
// String values use lexicographic comparison. Numeric types are coerced
// to float64. Returns 0 for incomparable types.
func CompareOrdered(a, b any) int {
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			if as < bs {
				return -1
			} else if as > bs {
				return 1
			}
			return 0
		}
	}

	af, aok := ToFloat64(a)
	bf, bok := ToFloat64(b)
	if aok && bok {
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0
	}

	return 0
}
