/*
Copyright 2026 The Antfly Contributors

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

package sdk

import "strings"

const apiV1Path = "/api/v1"

// NormalizeBaseURL returns the server root URL used by the joined public
// OpenAPI client. The generated paths already include /api/v1.
// It accepts either a server root URL or an API-root URL.
func NormalizeBaseURL(baseURL string) string {
	trimmed := strings.TrimRight(baseURL, "/")
	return strings.TrimSuffix(trimmed, apiV1Path)
}

// NormalizeServerURL returns the Antfly server root URL for non-OpenAPI
// endpoints such as internal admin routes.
func NormalizeServerURL(baseURL string) string {
	return strings.TrimSuffix(strings.TrimRight(baseURL, "/"), apiV1Path)
}
