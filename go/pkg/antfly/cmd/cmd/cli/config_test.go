/*
Copyright 2026 The Antfly Authors

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

package cli

import (
	"testing"

	"github.com/spf13/cobra"
)

func TestResolveURLUsesEnvWhenFlagUnset(t *testing.T) {
	t.Setenv("ANTFLY_URL", "https://platform.antfly.io/cloud/v1/instance")
	cmd := &cobra.Command{}
	cmd.Flags().String("url", "http://localhost:8080", "")

	if got := resolveURL(cmd); got != "https://platform.antfly.io/cloud/v1/instance" {
		t.Fatalf("resolveURL = %q", got)
	}
}

func TestResolveURLPrefersFlag(t *testing.T) {
	t.Setenv("ANTFLY_URL", "https://platform.antfly.io/cloud/v1/env")
	cmd := &cobra.Command{}
	cmd.Flags().String("url", "http://localhost:8080", "")
	if err := cmd.Flags().Set("url", "https://platform.antfly.io/cloud/v1/flag"); err != nil {
		t.Fatalf("set url flag: %v", err)
	}

	if got := resolveURL(cmd); got != "https://platform.antfly.io/cloud/v1/flag" {
		t.Fatalf("resolveURL = %q", got)
	}
}

func TestResolveTokenUsesEnvWhenFlagUnset(t *testing.T) {
	t.Setenv("ANTFLY_TOKEN", "env-token")
	cmd := &cobra.Command{}
	cmd.Flags().String("token", "", "")

	if got := resolveToken(cmd); got != "env-token" {
		t.Fatalf("resolveToken = %q", got)
	}
}

func TestResolveTokenPrefersFlag(t *testing.T) {
	t.Setenv("ANTFLY_TOKEN", "env-token")
	cmd := &cobra.Command{}
	cmd.Flags().String("token", "", "")
	if err := cmd.Flags().Set("token", "flag-token"); err != nil {
		t.Fatalf("set token flag: %v", err)
	}

	if got := resolveToken(cmd); got != "flag-token" {
		t.Fatalf("resolveToken = %q", got)
	}
}
