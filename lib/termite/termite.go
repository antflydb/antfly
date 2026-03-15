/*
Copyright 2025 The Antfly Authors

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

// Package termite provides shared configuration for the Termite ML service.
// All packages that need the Termite API URL (embeddings, generators, rerankers,
// chunkers, etc.) should use this package to resolve it.
package termite

import "os"

// defaultURL is the default URL for the Termite API, set from config at startup.
var defaultURL string

// SetDefaultURL sets the default Termite API URL used when no URL is specified
// in config. This should be called during config initialization.
func SetDefaultURL(url string) {
	defaultURL = url
}

// GetDefaultURL returns the current default Termite API URL.
func GetDefaultURL() string {
	return defaultURL
}

// ResolveURL resolves the Termite API URL from the given value, falling back
// to the ANTFLY_TERMITE_URL environment variable, then the global default.
// Returns an empty string if no URL is available.
func ResolveURL(configURL string) string {
	if configURL != "" {
		return configURL
	}
	if envURL := os.Getenv("ANTFLY_TERMITE_URL"); envURL != "" {
		return envURL
	}
	return defaultURL
}
