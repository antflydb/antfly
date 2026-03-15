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

package embeddings

import (
	"fmt"
	"reflect"
)

// ConvertToFloat32 attempts to convert any numeric type to float32
func ConvertToFloat32(value any) (float32, bool) {
	if value == nil {
		return 0, false
	}

	val := reflect.ValueOf(value)

	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float32(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float32(val.Uint()), true
	case reflect.Float32, reflect.Float64:
		return float32(val.Float()), true
	default:
		return 0, false
	}
}

// ConvertToFloat32Slice converts a slice of any numeric types to []float32
func ConvertToFloat32Slice(input []any) ([]float32, error) {
	result := make([]float32, len(input))
	var ok bool
	for i, val := range input {
		result[i], ok = ConvertToFloat32(val)
		if !ok {
			return nil, fmt.Errorf("unsupported type at index %d: %T", i, val)
		}
	}
	return result, nil
}
