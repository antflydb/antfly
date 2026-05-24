// Copyright 2015 The etcd Authors
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

package types

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ID represents a generic identifier which is canonically
// stored as a uint64 but is typically represented as a
// base-16 string for input/output
type ID uint64

func (i ID) String() string {
	return strconv.FormatUint(uint64(i), 16)
}

// IDFromString attempts to create an ID from a base-16 string.
func IDFromString(s string) (ID, error) {
	if s == "" { // Prevent parsing empty string as valid number ( strconv.ParseUint("", 16, 64) gives 0, nil )
		return 0, fmt.Errorf("cannot parse empty string as ID")
	}
	i, err := strconv.ParseUint(s, 16, 64)
	return ID(i), err
}

func (i ID) MarshalJSON() ([]byte, error) {
	// Marshal the string representation of the range.
	// This ensures proper JSON string quoting and escaping.
	return json.Marshal(i.String())
}
func (i ID) MarshalText() ([]byte, error) {
	return []byte(i.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
// The ID is expected to be a hex string.
func (i *ID) UnmarshalText(b []byte) error { // Fixed: Renamed from UnmarhsalText, changed receiver to pointer
	id, err := IDFromString(string(b))
	if err != nil {
		return err
	}
	*i = id // Fixed: Correctly assign to the value pointed to by i
	return nil
}

func (i *ID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("ID should be a string, got %s: %w", string(b), err)
	}
	id, err := IDFromString(s)
	if err != nil {
		return err
	}
	*i = id // Correctly assign to the value pointed to by i
	return nil
}

// IDSlice implements the sort interface
type IDSlice []ID

func (p IDSlice) Len() int           { return len(p) }
func (p IDSlice) Less(i, j int) bool { return uint64(p[i]) < uint64(p[j]) }
func (p IDSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p IDSlice) String() string {
	var b strings.Builder
	if p.Len() > 0 {
		b.WriteString(p[0].String())
	}

	for i := 1; i < p.Len(); i++ {
		b.WriteString(",")
		b.WriteString(p[i].String())
	}

	return b.String()
}

func (p IDSlice) MarshalJSON() ([]byte, error) {
	// Marshal the string representation of the range.
	// This ensures proper JSON string quoting and escaping.
	return json.Marshal(p.String())
}

func (p *IDSlice) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("IDSlice should be a string, got %s: %w", string(b), err)
	}

	if s == "" {
		*p = make(IDSlice, 0) // or *p = nil
		return nil
	}

	idsStr := strings.Split(s, ",")
	parsedIDs := make(IDSlice, len(idsStr))
	for i, idString := range idsStr {
		id, err := IDFromString(idString)
		if err != nil {
			return fmt.Errorf("failed to parse ID '%s' in IDSlice string \"%s\": %w", idString, s, err)
		}
		parsedIDs[i] = id
	}
	*p = parsedIDs
	return nil
}

// MarshalText implements the encoding.TextMarshaler interface.
// The textual representation is a comma-separated list of hex strings.
func (p IDSlice) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
// The IDSlice is expected to be a comma-separated list of hex strings.
func (p *IDSlice) UnmarshalText(text []byte) error {
	s := string(text)
	if s == "" {
		// Consistent with UnmarshalJSON: create an empty slice if the string is empty.
		*p = make(IDSlice, 0)
		return nil
	}

	idsStr := strings.Split(s, ",")
	parsedIDs := make(IDSlice, len(idsStr))
	for i, idString := range idsStr {
		id, err := IDFromString(idString)
		if err != nil {
			// Provide context for which part of the string failed
			return fmt.Errorf("failed to parse ID '%s' in IDSlice text string \"%s\": %w", idString, s, err)
		}
		parsedIDs[i] = id
	}
	*p = parsedIDs
	return nil
}
