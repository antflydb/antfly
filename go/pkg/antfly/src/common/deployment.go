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

// EffectiveDeploymentMode returns the configured runtime topology.
func (c *Config) EffectiveDeploymentMode() ConfigDeploymentMode {
	if c.DeploymentMode != "" {
		return c.DeploymentMode
	}
	return ConfigDeploymentModeDistributed
}

// NormalizeDeploymentMode makes the canonical deployment mode explicit.
func (c *Config) NormalizeDeploymentMode() {
	c.DeploymentMode = c.EffectiveDeploymentMode()
}

func (c *Config) IsStandalone() bool {
	return c.EffectiveDeploymentMode() == ConfigDeploymentModeStandalone
}

// EffectiveFsync applies the durable-by-default policy for Lite storage.
func (c LiteStorageConfig) EffectiveFsync() bool {
	return c.Fsync == nil || *c.Fsync
}
