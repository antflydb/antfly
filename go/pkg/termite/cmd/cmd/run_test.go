// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cmd

import (
	"testing"

	"github.com/spf13/viper"
)

func TestRunServerRejectsNegativeMaxLoadedModels(t *testing.T) {
	previous := viper.Get("max_loaded_models")
	viper.Set("max_loaded_models", -1)
	t.Cleanup(func() { viper.Set("max_loaded_models", previous) })

	if err := runServer(nil, nil); err == nil {
		t.Fatal("expected negative max_loaded_models to be rejected")
	}
}
