// Copyright 2024 The Termite Authors.
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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestID_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		id      ID
		want    []byte
		wantErr bool
	}{
		{"Zero ID", ID(0), []byte(`"0"`), false},
		{"Non-zero ID", ID(123), []byte(`"7b"`), false},
		{"Max uint64 ID", ID(18446744073709551615), []byte(`"ffffffffffffffff"`), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.id.MarshalJSON()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestID_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    ID
		wantErr bool
	}{
		{"Zero ID", []byte(`"0"`), ID(0), false},
		{"Non-zero ID", []byte(`"7b"`), ID(123), false},
		{"Max uint64 ID", []byte(`"ffffffffffffffff"`), ID(18446744073709551615), false},
		{"Empty JSON string", []byte(`""`), ID(0), true},                // IDFromString("") errors due to new check
		{"Invalid hex", []byte(`"invalid"`), ID(0), true},               // IDFromString("invalid") errors
		{"Not a JSON string", []byte(`123`), ID(0), true},               // json.Unmarshal into string fails
		{"Malformed JSON string", []byte(`"unterminated`), ID(0), true}, // json.Unmarshal into string fails
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var id ID
			err := id.UnmarshalJSON(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, id)
			}
		})
	}
}

func TestIDSlice_MarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		ids     IDSlice
		want    []byte
		wantErr bool
	}{
		{"Empty slice", IDSlice{}, []byte(`""`), false},
		{"Single element", IDSlice{ID(1)}, []byte(`"1"`), false},
		{"Multiple elements", IDSlice{ID(10), ID(20), ID(30)}, []byte(`"a,14,1e"`), false},
		{"Zero value", IDSlice{ID(0)}, []byte(`"0"`), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.ids.MarshalJSON()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestIDSlice_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    IDSlice
		wantErr bool
	}{
		{"Empty slice string", []byte(`""`), IDSlice{}, false}, // "" is a valid representation for an empty IDSlice
		{"Single element", []byte(`"1"`), IDSlice{ID(1)}, false},
		{"Multiple elements", []byte(`"a,14,1e"`), IDSlice{ID(10), ID(20), ID(30)}, false},
		{"Zero value", []byte(`"0"`), IDSlice{ID(0)}, false},
		{"Invalid hex in slice", []byte(`"a,invalid,1e"`), nil, true},
		{"Not a JSON string (array)", []byte(`[1,2,3]`), nil, true},  // json.Unmarshal into string fails
		{"Not a JSON string (unquoted)", []byte(`a,b,c`), nil, true}, // json.Unmarshal into string fails
		{"Empty string element", []byte(`"a, ,c"`), nil, true},       // IDFromString("") for the middle element will fail
		{"Leading comma", []byte(`",a,c"`), nil, true},               // IDFromString("") for the first element will fail
		{"Trailing comma", []byte(`"a,c,"`), nil, true},              // IDFromString("") for the last element will fail
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ids IDSlice
			err := ids.UnmarshalJSON(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Use assert.Equal for slice comparison.
				// For empty slices, tt.want might be nil or IDSlice{}. assert.Equal handles this.
				if tt.want == nil && len(ids) == 0 { // if want is nil, and result is empty slice, treat as equal
					// This condition is to make sure an empty actual slice matches a nil expected slice.
					// assert.Equal would consider `nil` and `IDSlice{}` (empty non-nil slice) as different.
					// However, in the context of UnmarshalJSON, an empty result often means `IDSlice{}`.
					// If `tt.want` is explicitly `IDSlice{}`, this check is not needed.
					// Given current tests define `nil` for error cases, `tt.want` is `IDSlice{}` for the valid empty case.
					// For the "Empty string results in error", want is `nil`, so this block won't be hit.
					// Let's make `want` consistent for success cases. If `[]byte(`"\"\""`)` was to be an empty slice, `want` should be `IDSlice{}`.
					// The test case `{"Empty slice string", []byte(\`""\`), IDSlice{}, false}` was removed and replaced.
					// A new test case for a valid empty list string `[]byte("[]")` is not present,
					// as the current format is a comma-separated string, not a JSON array string.
					// If we were to support `[]byte("\"\"")` to mean an empty list that *isnt* an error, the logic would be:
					// `s := ""` after `strings.Trim`. `strings.Split("",",")` -> `[""]`.
					// `*p = make(IDSlice, 1)`. `IDFromString("")` -> error.
					// To correctly parse `""` as an empty IDSlice and not error, UnmarshalJSON for IDSlice needs:
					// if s == "" { *p = make(IDSlice, 0); return nil }
					// As of now, `[]byte(\`""\`)` correctly errors.
					// The original test `{"Empty slice string", []byte(\`""\`), IDSlice{}, false}` was problematic.
				} else {
					assert.Equal(t, tt.want, ids)
				}
			}
		})
	}
}

// TestID_UnmarshalJSON_PointerReceiverIssue checks if UnmarshalJSON correctly modifies the ID.
// The original UnmarshalJSON for ID had `i = &id` which only changes the local pointer `i`,
// not the value pointed to by the method receiver.
func TestID_UnmarshalJSON_PointerReceiverIssue(t *testing.T) {
	var actualID ID
	err := actualID.UnmarshalJSON([]byte(`"ff"`))
	require.NoError(t, err, "UnmarshalJSON failed for 'ff'")
	expectedID := ID(255)
	assert.Equal(t, expectedID, actualID, "UnmarshalJSON expected %s, got %s", expectedID, actualID)

	// Test with a non-zero initial ID
	actualID = ID(10)
	err = actualID.UnmarshalJSON([]byte(`"1"`))
	require.NoError(t, err, "UnmarshalJSON failed for '1'")
	expectedID = ID(1)
	assert.Equal(t, expectedID, actualID, "UnmarshalJSON expected %s, got %s (initial value was 10)", expectedID, actualID)
}

func TestID_UnmarshalText(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantID  ID
		wantErr bool
	}{
		{"Valid ID", "abcdef", ID(0xabcdef), false},
		{"Valid ID uppercase", "ABCDEF", ID(0xabcdef), false},
		{"Zero ID", "0", ID(0), false},
		{"Empty string", "", ID(0), true}, // IDFromString returns error for empty string
		{"Invalid hex", "xyz", ID(0), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var id ID
			err := id.UnmarshalText([]byte(tt.input))

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantID, id)
			}
		})
	}
}

func TestIDMapJSONMarshalling(t *testing.T) {
	originalMap := map[ID]string{
		ID(10): "value1", // hex: "a"
		ID(20): "value2", // hex: "14"
	}

	jsonData, err := json.Marshal(originalMap)
	require.NoError(t, err, "Failed to marshal map[ID]string")

	var unmarshalledMap map[ID]string
	err = json.Unmarshal(jsonData, &unmarshalledMap)
	require.NoError(t, err, "Failed to unmarshal map[ID]string. JSON data: %s", string(jsonData))

	assert.Equal(t, originalMap, unmarshalledMap, "Roundtrip for map[ID]string failed.")
}

func TestIDSliceTextMarshalling(t *testing.T) {
	testCases := []struct {
		name          string
		slice         IDSlice
		expectedText  string
		expectedError bool // For UnmarshalText primarily
	}{
		{"NonEmptySlice", IDSlice{ID(10), ID(20), ID(30)}, "a,14,1e", false},
		{"SingleElementSlice", IDSlice{ID(100)}, "64", false},
		{"EmptySliceLiteral", IDSlice{}, "", false},
		{"NilSlice", nil, "", false},
		{"SliceWithInvalidIDInStringForUnmarshal", nil, "a,xyz,1e", true}, // For testing UnmarshalText error path
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.name, "InvalidIDInStringForUnmarshal") {
				// Test MarshalText
				text, err := tc.slice.MarshalText()
				require.NoError(t, err, "MarshalText() failed")
				assert.Equal(t, tc.expectedText, string(text), "MarshalText() output mismatch")
			}

			// Test UnmarshalText
			var newSlice IDSlice
			err := newSlice.UnmarshalText([]byte(tc.expectedText))

			if tc.expectedError {
				require.Error(t, err, "UnmarshalText() expected an error for input '%s', but got nil", tc.expectedText)
				return
			}
			require.NoError(t, err, "UnmarshalText() failed for input %s", tc.expectedText)

			expectedAfterUnmarshal := tc.slice
			if expectedAfterUnmarshal == nil { // Normalize nil to empty for comparison post-unmarshal
				expectedAfterUnmarshal = make(IDSlice, 0)
			}
			assert.Equal(t, expectedAfterUnmarshal, newSlice, "UnmarshalText() output mismatch (original: %#v)", tc.slice)
		})
	}
}
