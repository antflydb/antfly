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

package evaluator

import "testing"

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want float64
		ok   bool
	}{
		{"float64", float64(3.14), 3.14, true},
		{"float32", float32(2.5), 2.5, true},
		{"int", int(42), 42, true},
		{"int64", int64(100), 100, true},
		{"int32", int32(50), 50, true},
		{"uint", uint(7), 7, true},
		{"uint64", uint64(99), 99, true},
		{"uint32", uint32(33), 33, true},
		{"string", "hello", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ToFloat64(tt.v)
			if ok != tt.ok {
				t.Errorf("ToFloat64(%v) ok = %v, want %v", tt.v, ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("ToFloat64(%v) = %v, want %v", tt.v, got, tt.want)
			}
		})
	}
}

func TestValuesEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b any
		want bool
	}{
		{"strings equal", "hello", "hello", true},
		{"strings not equal", "hello", "world", false},
		{"int == float64", int(42), float64(42), true},
		{"int64 == float64", int64(100), float64(100), true},
		{"int32 == uint64", int32(50), uint64(50), true},
		{"cross-numeric not equal", int(1), float64(2), false},
		{"nil == nil", nil, nil, true},
		{"string != int", "42", int(42), false},
		{"bool == bool", true, true, true},
		{"bool != bool", true, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ValuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("ValuesEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestCompareOrdered(t *testing.T) {
	tests := []struct {
		name string
		a, b any
		want int
	}{
		{"strings <", "a", "b", -1},
		{"strings >", "z", "a", 1},
		{"strings ==", "m", "m", 0},
		{"int < float64", int(1), float64(2), -1},
		{"int > float64", int(5), float64(3), 1},
		{"int == float64", int(4), float64(4), 0},
		{"incomparable", "hello", int(42), 0},
		{"nil nil", nil, nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareOrdered(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("CompareOrdered(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
