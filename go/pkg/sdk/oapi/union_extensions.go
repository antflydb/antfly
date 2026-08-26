// Copyright 2026 The Antfly Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package oapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
)

// DecodeInto decodes the retained GraphResult JSON directly into value.
// It lets SDK adapters inspect a small discriminator probe without first
// marshaling and copying a potentially large result payload.
func (t GraphResult) DecodeInto(value any) error {
	return json.Unmarshal(t.union, value)
}

// DecodeInto decodes a canonical GraphQueryResult without copying it through
// an intermediate JSON map.
func (t GraphQueryResult) DecodeInto(value any) error {
	return json.Unmarshal(t.union, value)
}

// DecodeStrictInto decodes the selected canonical GraphResult shape while
// enforcing additionalProperties: false throughout typed objects. It reads
// directly from the retained union bytes and therefore does not copy large
// result arrays before decoding them.
func (t GraphResult) DecodeStrictInto(value any) error {
	return decodeStrict(t.union, value)
}

// DecodeStrictInto is the canonical GraphQueryResult equivalent.
func (t GraphQueryResult) DecodeStrictInto(value any) error {
	return decodeStrict(t.union, value)
}

func decodeStrict(encoded []byte, value any) error {
	shape := canonicalGraphResultShape(value)
	if shape == nil {
		return decodeStrictStandard(encoded, value)
	}
	destination := reflect.ValueOf(value)
	if destination.Kind() != reflect.Pointer || destination.IsNil() {
		return errors.New("strict JSON decode destination must be a non-nil pointer")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := decodeJSONShapeValue(decoder, shape, destination.Elem()); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func decodeStrictStandard(encoded []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

// requiredJSONShape is generated from the canonical OpenAPI graph-result
// closure. Generated Go structs intentionally expose required scalar fields as
// values rather than pointers, so encoding/json alone cannot distinguish an
// omitted zero-valued field from a present zero. The shaped decoder validates
// presence and unknown fields while populating the generated model in one pass.
type requiredJSONShape struct {
	name                      string
	object                    bool
	array                     bool
	nullable                  bool
	nonEmptyString            bool
	required                  []string
	properties                map[string]*requiredJSONShape
	allowAdditionalProperties bool
	additionalProperties      *requiredJSONShape
	items                     *requiredJSONShape
	reference                 *requiredJSONShape
}

var jsonStructFieldCache sync.Map // map[reflect.Type]map[string]int

func decodeJSONShapeValue(decoder *json.Decoder, shape *requiredJSONShape, destination reflect.Value) error {
	if shape == nil {
		return decoder.Decode(destination.Addr().Interface())
	}
	if shape.reference != nil && !shape.nullable {
		return decodeJSONShapeValue(decoder, shape.reference, destination)
	}
	if shape.object || shape.array || shape.nullable {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if token == nil {
			if !shape.nullable {
				return fmt.Errorf("%s must not be null", shape.name)
			}
			destination.SetZero()
			return nil
		}
		if shape.reference != nil {
			return decodeJSONShapeToken(decoder, shape.reference, destination, token)
		}
		return decodeJSONShapeToken(decoder, shape, destination, token)
	}
	return decodeJSONScalar(decoder, shape, destination)
}

func decodeJSONScalar(decoder *json.Decoder, shape *requiredJSONShape, destination reflect.Value) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return fmt.Errorf("%s must not be null", shape.name)
	}
	destination = indirectJSONDestination(destination)
	switch destination.Kind() {
	case reflect.String:
		value, ok := token.(string)
		if !ok {
			return fmt.Errorf("%s must be a string", shape.name)
		}
		destination.SetString(value)
	case reflect.Bool:
		value, ok := token.(bool)
		if !ok {
			return fmt.Errorf("%s must be a boolean", shape.name)
		}
		destination.SetBool(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value, ok := token.(float64)
		if !ok || math.Trunc(value) != value || value < math.MinInt64 || value >= -float64(math.MinInt64) {
			return fmt.Errorf("%s must be an integer", shape.name)
		}
		parsed := int64(value)
		if destination.OverflowInt(parsed) {
			return fmt.Errorf("%s integer is out of range", shape.name)
		}
		destination.SetInt(parsed)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value, ok := token.(float64)
		if !ok || math.Trunc(value) != value || value < 0 || value >= math.Exp2(64) {
			return fmt.Errorf("%s must be an unsigned integer", shape.name)
		}
		parsed := uint64(value)
		if destination.OverflowUint(parsed) {
			return fmt.Errorf("%s unsigned integer is out of range", shape.name)
		}
		destination.SetUint(parsed)
	case reflect.Float32, reflect.Float64:
		value, ok := token.(float64)
		if !ok {
			return fmt.Errorf("%s must be a number", shape.name)
		}
		if destination.OverflowFloat(value) {
			return fmt.Errorf("%s must be a representable number", shape.name)
		}
		destination.SetFloat(value)
	default:
		return fmt.Errorf("%s cannot decode scalar into %s", shape.name, destination.Type())
	}
	if shape.nonEmptyString {
		if destination.Kind() != reflect.String || destination.Len() == 0 {
			return fmt.Errorf("%s must be a non-empty string", shape.name)
		}
	}
	return nil
}

func decodeJSONShapeToken(
	decoder *json.Decoder,
	shape *requiredJSONShape,
	destination reflect.Value,
	token json.Token,
) error {
	if shape.reference != nil {
		return decodeJSONShapeToken(decoder, shape.reference, destination, token)
	}
	if shape.object {
		delim, ok := token.(json.Delim)
		if !ok || delim != '{' {
			return fmt.Errorf("%s must be an object", shape.name)
		}
		return decodeJSONObject(decoder, shape, indirectJSONDestination(destination))
	}
	if shape.array {
		delim, ok := token.(json.Delim)
		if !ok || delim != '[' {
			return fmt.Errorf("%s must be an array", shape.name)
		}
		return decodeJSONArray(decoder, shape, indirectJSONDestination(destination))
	}
	return fmt.Errorf("%s has an invalid generated decoding shape", shape.name)
}

func indirectJSONDestination(destination reflect.Value) reflect.Value {
	for destination.Kind() == reflect.Pointer {
		if destination.IsNil() {
			destination.Set(reflect.New(destination.Type().Elem()))
		}
		destination = destination.Elem()
	}
	return destination
}

func decodeJSONObject(decoder *json.Decoder, shape *requiredJSONShape, destination reflect.Value) error {
	if destination.Kind() != reflect.Struct && destination.Kind() != reflect.Map {
		return fmt.Errorf("%s cannot decode into %s", shape.name, destination.Type())
	}
	if destination.Kind() == reflect.Map && destination.IsNil() {
		destination.Set(reflect.MakeMap(destination.Type()))
	}
	seen := make(map[string]struct{}, len(shape.required))
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("%s contains a non-string property name", shape.name)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%s contains duplicate field %q", shape.name, key)
		}
		seen[key] = struct{}{}

		child, declared := shape.properties[key]
		if !declared {
			if !shape.allowAdditionalProperties {
				return fmt.Errorf("%s contains unknown field %q", shape.name, key)
			}
			child = shape.additionalProperties
		}
		if destination.Kind() == reflect.Map {
			if destination.Type().Key().Kind() != reflect.String {
				return fmt.Errorf("%s cannot decode into non-string-keyed map", shape.name)
			}
			value := reflect.New(destination.Type().Elem()).Elem()
			if err := decodeJSONShapeValue(decoder, child, value); err != nil {
				return err
			}
			destination.SetMapIndex(reflect.ValueOf(key).Convert(destination.Type().Key()), value)
			continue
		}

		fieldIndex, found := jsonStructFields(destination.Type())[key]
		if !found {
			if declared {
				return fmt.Errorf("%s generated Go model is missing field %q", shape.name, key)
			}
			var discarded any
			if err := decodeJSONShapeValue(decoder, child, reflect.ValueOf(&discarded).Elem()); err != nil {
				return err
			}
			continue
		}
		if err := decodeJSONShapeValue(decoder, child, destination.Field(fieldIndex)); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim('}') {
		return fmt.Errorf("%s has an invalid object terminator", shape.name)
	}
	for _, required := range shape.required {
		if _, ok := seen[required]; !ok {
			return fmt.Errorf("%s requires field %q", shape.name, required)
		}
	}
	return nil
}

func decodeJSONArray(decoder *json.Decoder, shape *requiredJSONShape, destination reflect.Value) error {
	if destination.Kind() != reflect.Slice {
		return fmt.Errorf("%s cannot decode into %s", shape.name, destination.Type())
	}
	if destination.IsNil() {
		destination.Set(reflect.MakeSlice(destination.Type(), 0, 0))
	} else {
		destination.SetLen(0)
	}
	for decoder.More() {
		value := reflect.New(destination.Type().Elem()).Elem()
		if err := decodeJSONShapeValue(decoder, shape.items, value); err != nil {
			return err
		}
		destination.Set(reflect.Append(destination, value))
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim(']') {
		return fmt.Errorf("%s has an invalid array terminator", shape.name)
	}
	return nil
}

func jsonStructFields(structType reflect.Type) map[string]int {
	if cached, ok := jsonStructFieldCache.Load(structType); ok {
		return cached.(map[string]int)
	}
	fields := make(map[string]int, structType.NumField())
	for i := range structType.NumField() {
		field := structType.Field(i)
		name := field.Tag.Get("json")
		if comma := strings.IndexByte(name, ','); comma >= 0 {
			name = name[:comma]
		}
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = i
	}
	actual, _ := jsonStructFieldCache.LoadOrStore(structType, fields)
	return actual.(map[string]int)
}
