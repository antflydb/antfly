/*
Copyright 2025 The Antfly Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package json provides a configurable JSON encoding/decoding layer.
// It defaults to encoding/json but can be swapped for faster implementations
// like github.com/goccy/go-json or github.com/bytedance/sonic.
//
// Usage:
//
//	import json "github.com/antflydb/antfly/pkg/libaf/json"
//
//	// Works like encoding/json
//	data, err := json.Marshal(v)
//	err = json.Unmarshal(data, &v)
//
// To use a different JSON library:
//
//	import (
//		json "github.com/antflydb/antfly/pkg/libaf/json"
//		gojson "github.com/goccy/go-json"
//	)
//
//	func init() {
//		json.SetConfig(json.Config{
//			Marshal:         gojson.Marshal,
//			MarshalIndent:   gojson.MarshalIndent,
//			MarshalString:   gojson.MarshalString,
//			Unmarshal:       gojson.Unmarshal,
//			UnmarshalString: gojson.UnmarshalString,
//			NewEncoder: func(w io.Writer) json.Encoder {
//				return gojson.NewEncoder(w)
//			},
//			NewDecoder: func(r io.Reader) json.Decoder {
//				return gojson.NewDecoder(r)
//			},
//		})
//	}
package json

import (
	"io"

	stdjson "encoding/json"
)

// Encoder is the interface for streaming JSON encoding.
// Both encoding/json and alternative libraries satisfy this interface.
type Encoder interface {
	Encode(v any) error
}

// Decoder is the interface for streaming JSON decoding.
// Both encoding/json and alternative libraries satisfy this interface.
type Decoder interface {
	Decode(v any) error
}

// Config holds the JSON encoding/decoding functions.
type Config struct {
	Marshal         func(v any) ([]byte, error)
	MarshalIndent   func(v any, prefix, indent string) ([]byte, error)
	MarshalString   func(v any) (string, error)
	Unmarshal       func(data []byte, v any) error
	UnmarshalString func(s string, v any) error
	NewEncoder      func(w io.Writer) Encoder
	NewDecoder      func(r io.Reader) Decoder
}

// DefaultConfig returns the default configuration using encoding/json.
func DefaultConfig() Config {
	return Config{
		Marshal:       stdjson.Marshal,
		MarshalIndent: stdjson.MarshalIndent,
		MarshalString: func(v any) (string, error) {
			data, err := stdjson.Marshal(v)
			if err != nil {
				return "", err
			}
			return string(data), nil
		},
		Unmarshal: stdjson.Unmarshal,
		UnmarshalString: func(s string, v any) error {
			return stdjson.Unmarshal([]byte(s), v)
		},
		NewEncoder: func(w io.Writer) Encoder {
			return stdjson.NewEncoder(w)
		},
		NewDecoder: func(r io.Reader) Decoder {
			return stdjson.NewDecoder(r)
		},
	}
}

// Global configuration - defaults to encoding/json
var config = DefaultConfig()

// SetConfig sets the global JSON configuration.
// Call this before using any JSON functions to use a custom JSON library.
func SetConfig(c Config) {
	config = c
}

// GetConfig returns the current JSON configuration.
func GetConfig() Config {
	return config
}

// Marshal returns the JSON encoding of v.
func Marshal(v any) ([]byte, error) {
	return config.Marshal(v)
}

// MarshalIndent is like Marshal but applies Indent to format the output.
func MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	return config.MarshalIndent(v, prefix, indent)
}

// MarshalString returns the JSON encoding of v as a string.
func MarshalString(v any) (string, error) {
	return config.MarshalString(v)
}

// Unmarshal parses the JSON-encoded data and stores the result in v.
func Unmarshal(data []byte, v any) error {
	return config.Unmarshal(data, v)
}

// UnmarshalString parses the JSON-encoded string and stores the result in v.
func UnmarshalString(s string, v any) error {
	return config.UnmarshalString(s, v)
}

// NewEncoder returns a new Encoder that writes to w.
func NewEncoder(w io.Writer) Encoder {
	return config.NewEncoder(w)
}

// NewDecoder returns a new Decoder that reads from r.
func NewDecoder(r io.Reader) Decoder {
	return config.NewDecoder(r)
}

// RawMessage is a raw encoded JSON value.
// It implements Marshaler and Unmarshaler and can be used to delay JSON decoding.
type RawMessage = stdjson.RawMessage

// Number represents a JSON number literal.
type Number = stdjson.Number

// Marshaler is the interface implemented by types that can marshal themselves into valid JSON.
type Marshaler = stdjson.Marshaler

// Unmarshaler is the interface implemented by types that can unmarshal a JSON description of themselves.
type Unmarshaler = stdjson.Unmarshaler

// EncodeOption represents an option for JSON encoding.
type EncodeOption func(*encodeOptions)

type encodeOptions struct {
	sortMapKeys bool
}

// SortMapKeys is an option that sorts map keys in the output.
var SortMapKeys EncodeOption = func(o *encodeOptions) {
	o.sortMapKeys = true
}

// EncodeIndented returns the JSON encoding of v with indentation.
// It accepts optional EncodeOptions like SortMapKeys.
// Note: SortMapKeys is a no-op with encoding/json as it always sorts keys.
//
// If the configured JSON library panics (e.g. goccy/go-json on nil union
// fields from oapi-codegen), it falls back to encoding/json.
// Note: opts and SortMapKeys are currently unused; they exist for future
// alternative JSON library support.
func EncodeIndented(v any, prefix, indent string, opts ...EncodeOption) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Fallback to encoding/json which handles nil fields safely.
			// This intentionally swallows the panic from the JSON library;
			// programming errors in MarshalJSON impls may be masked.
			result, err = stdjson.MarshalIndent(v, prefix, indent)
		}
	}()
	return config.MarshalIndent(v, prefix, indent)
}
