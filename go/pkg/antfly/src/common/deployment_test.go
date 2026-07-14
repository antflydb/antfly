/*
Copyright © 2026 Antfly, Inc.

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
package common

import "testing"

func TestEffectiveDeploymentModeDefaultsToDistributed(t *testing.T) {
	cfg := Config{}
	if got := cfg.EffectiveDeploymentMode(); got != ConfigDeploymentModeDistributed {
		t.Fatalf("EffectiveDeploymentMode() = %q, want %q", got, ConfigDeploymentModeDistributed)
	}
}

func TestValidateDeploymentModeRejectsUnknownValue(t *testing.T) {
	cfg := Config{DeploymentMode: "unknown"}
	if err := cfg.validateDeploymentMode(); err == nil {
		t.Fatal("validateDeploymentMode() accepted an unknown value")
	}
}

func TestLiteStorageEffectiveFsyncDefaultsTrueAndAllowsExplicitDisable(t *testing.T) {
	if !(LiteStorageConfig{}).EffectiveFsync() {
		t.Fatal("Lite fsync must default to true")
	}
	disabled := false
	if (LiteStorageConfig{Fsync: &disabled}).EffectiveFsync() {
		t.Fatal("explicit fsync=false was ignored")
	}
}
