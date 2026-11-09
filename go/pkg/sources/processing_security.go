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
	"net/url"
	"strings"
)

// Match native common/credential_safety.zig: normalize spellings before
// classifying credential fields, including extensible provider documents.
func processingCredentialField(key string) bool {
	if len(key) > 128 {
		return true
	}
	var normalized strings.Builder
	for _, c := range strings.ToLower(key) {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			normalized.WriteRune(c)
		}
	}
	value := normalized.String()
	switch value {
	case "", "auth", "code", "cookie", "credentials", "credentialspath", "key", "password", "secret", "sig", "token", "xauth", "setcookie":
		return true
	}
	for _, part := range []string{"apikey", "accesskey", "secretkey", "privatekey", "subscriptionkey", "authtoken", "authkey"} {
		if strings.Contains(value, part) {
			return true
		}
	}
	for _, suffix := range []string{"password", "passwd", "secret", "token", "credential", "signature", "authorization"} {
		if strings.HasSuffix(value, suffix) {
			return true
		}
	}
	return false
}
func processingSecretReference(value string) bool {
	if !strings.HasPrefix(value, "${secret:") || !strings.HasSuffix(value, "}") {
		return false
	}
	name := value[len("${secret:") : len(value)-1]
	return name != "" && !strings.ContainsAny(name, "{}\x00\r\n")
}
func processingEndpointField(key string) bool {
	switch strings.ToLower(key) {
	case "url", "api_url", "base_url", "endpoint", "endpoint_url":
		return true
	}
	return false
}
func processingURLCredentials(value string) bool {
	if processingSecretReference(value) {
		return false
	}
	u, err := url.Parse(value)
	if err != nil {
		return true
	}
	if u.User != nil {
		return true
	}
	for _, part := range []string{u.RawQuery, u.Fragment} {
		parameters, err := url.ParseQuery(strings.ReplaceAll(part, ";", "&"))
		if err != nil {
			return true
		}
		for key := range parameters {
			if processingCredentialField(key) {
				return true
			}
		}
	}
	return false
}
