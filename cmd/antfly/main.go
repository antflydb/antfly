/*
Copyright © 2025 AJ Roetker ajroetker@antfly.io

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
package main

import (
	"io"
	"runtime"

	"github.com/antflydb/antfly/cmd/antfly/cmd"
	"github.com/antflydb/antfly/lib/utils"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	gojson "github.com/goccy/go-json"
)

func init() {
	// Configure the JSON wrapper to use goccy/go-json for performance
	json.SetConfig(json.Config{
		Marshal: gojson.Marshal,
		MarshalIndent: func(v any, prefix, indent string) ([]byte, error) {
			return gojson.MarshalIndent(v, prefix, indent)
		},
		Unmarshal: gojson.Unmarshal,
		MarshalString: func(v any) (string, error) {
			data, err := gojson.Marshal(v)
			if err != nil {
				return "", err
			}
			return string(data), nil
		},
		UnmarshalString: func(s string, v any) error {
			return gojson.Unmarshal([]byte(s), v)
		},
		NewEncoder: func(w io.Writer) json.Encoder {
			return gojson.NewEncoder(w)
		},
		NewDecoder: func(r io.Reader) json.Decoder {
			return gojson.NewDecoder(r)
		},
	})
}

// https://goreleaser.com/cookbooks/using-main.version/
//
// By default, GoReleaser will set the following 3 ldflags:
//
// main.version: Current Git tag (the v prefix is stripped) or the name of the snapshot, if you're using the --snapshot flag
var version = "dev"

// main.commit: Current git commit SHA
// commit = "none"
// main.date: Date in the RFC3339 format
// date = "unknown"

func main() {
	runtime.SetMutexProfileFraction(1) // Enable mutex profiling
	runtime.SetBlockProfileRate(1)     // Sample every blocking event
	// Or, for a rate of 1 sample per second of blocking:
	// runtime.SetBlockProfileRate(int(time.Second.Nanoseconds()))
	cmd.Version = version
	utils.Version = version
	cmd.Execute()
}
