// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/oauth2/google"
)

func GoogleCredentials(ctx context.Context, provider, secret string) (*google.Credentials, error) {
	return GoogleCredentialsForServices(ctx, provider, secret, nil)
}

// GoogleCredentialsForServices requests only the selected Workspace services.
// Refresh credentials must already have those grants; adding a service requires
// renewed user consent. Service accounts do not implicitly gain mailbox access.
func GoogleCredentialsForServices(ctx context.Context, provider, secret string, services []string) (*google.Credentials, error) {
	var config struct {
		Type     string `json:"type"`
		TokenURI string `json:"token_uri"`
		Universe string `json:"universe_domain"`
	}
	if json.Unmarshal([]byte(secret), &config) != nil || (config.Type != "service_account" && config.Type != "authorized_user") {
		return nil, fmt.Errorf("Google sources require service_account or authorized_user credential JSON")
	}
	if (config.TokenURI != "" && config.TokenURI != "https://oauth2.googleapis.com/token" && config.TokenURI != "https://accounts.google.com/o/oauth2/token") || (config.Universe != "" && config.Universe != "googleapis.com") {
		return nil, fmt.Errorf("Google credential endpoints must use the supported Google identity service")
	}
	scopes := []string{"https://www.googleapis.com/auth/devstorage.read_only"}
	if provider == ProviderGoogleDrive {
		scopes = []string{"https://www.googleapis.com/auth/drive.readonly"}
	} else if provider == ProviderGoogleWorkspace {
		if len(services) == 0 {
			services = []string{"drive"}
		}
		scopes = nil
		seen := map[string]bool{}
		for _, service := range services {
			if seen[service] {
				continue
			}
			seen[service] = true
			switch service {
			case "drive":
				scopes = append(scopes, "https://www.googleapis.com/auth/drive.readonly")
			case "gmail":
				scopes = append(scopes, "https://www.googleapis.com/auth/gmail.readonly")
			case "calendar":
				scopes = append(scopes, "https://www.googleapis.com/auth/calendar.readonly")
			default:
				return nil, fmt.Errorf("unsupported Google Workspace service %q", service)
			}
		}
	} else if provider != ProviderGCS {
		return nil, fmt.Errorf("unsupported Google source provider")
	}
	credentials, err := google.CredentialsFromJSON(ctx, []byte(secret), scopes...)
	if err != nil {
		return nil, fmt.Errorf("invalid Google source credentials")
	}
	return credentials, nil
}
