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

package controllers

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const DefaultClusterDomain = "cluster.local"

// NormalizeClusterDomain validates the DNS suffix used for Kubernetes Service
// and StatefulSet pod addresses. A trailing root dot is accepted for operator
// ergonomics but omitted from generated URLs.
func NormalizeClusterDomain(raw string) (string, error) {
	domain := strings.TrimSpace(raw)
	if domain == "" {
		return DefaultClusterDomain, nil
	}
	domain = strings.TrimSuffix(domain, ".")
	if domain == "" {
		return "", fmt.Errorf("cluster domain must contain at least one DNS label")
	}
	if problems := validation.IsDNS1123Subdomain(domain); len(problems) > 0 {
		return "", fmt.Errorf("invalid cluster domain %q: %s", raw, strings.Join(problems, "; "))
	}
	return domain, nil
}

func clusterDomainOrDefault(domain string) string {
	if domain == "" {
		return DefaultClusterDomain
	}
	return domain
}

func normalizeClusterDomainField(domain *string) error {
	normalized, err := NormalizeClusterDomain(*domain)
	if err != nil {
		return err
	}
	*domain = normalized
	return nil
}

func serviceDNSName(service, namespace, clusterDomain string) string {
	return fmt.Sprintf("%s.%s.svc.%s", service, namespace, clusterDomainOrDefault(clusterDomain))
}

func statefulPodDNSName(pod, service, namespace, clusterDomain string) string {
	return pod + "." + serviceDNSName(service, namespace, clusterDomain)
}
