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

package commands

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

// Compare JSON numbers exactly, independently of their wire spelling and the
// server's opaque revision. Exponents remain symbolic, so 1e1000000 does not
// allocate a million-digit coefficient. Neither declarations nor PUT bodies
// are modified by comparison.
func manifestComparisonJSON(source management.Source) ([]byte, error) {
	raw, err := json.Marshal(manifestDeclaration(source))
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(normalizeComparisonNumbers(value))
}
func normalizeComparisonNumbers(value any) any {
	switch v := value.(type) {
	case json.Number:
		return json.Number(canonicalNumber(string(v)))
	case map[string]any:
		for key, child := range v {
			v[key] = normalizeComparisonNumbers(child)
		}
	case []any:
		for i, child := range v {
			v[i] = normalizeComparisonNumbers(child)
		}
	}
	return value
}

// Input is always a valid JSON number from the decoder above.
func canonicalNumber(value string) string {
	sign := ""
	if strings.HasPrefix(value, "-") {
		sign = "-"
		value = value[1:]
	}
	mantissa, exponent, hasExponent := strings.Cut(strings.ToLower(value), "e")
	scale := new(big.Int)
	if hasExponent {
		scale.SetString(exponent, 10)
	}
	fraction := 0
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		fraction = len(mantissa) - dot - 1
		mantissa = mantissa[:dot] + mantissa[dot+1:]
	}
	mantissa = strings.TrimLeft(mantissa, "0")
	if mantissa == "" {
		return "0"
	}
	coefficient := strings.TrimRight(mantissa, "0")
	scale.Add(scale, big.NewInt(int64(len(mantissa)-len(coefficient)-fraction)))
	return sign + coefficient + "e" + scale.String()
}
