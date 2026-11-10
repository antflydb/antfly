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
	"fmt"
	"net/http"
)

type searchScopeKey struct{}

// One request-local scope pins gateway policy/identity across native metadata,
// query and generation rechecks. These receipts never grant authorization.
type searchScope struct {
	source, subject, access, publication string
	hosted                               bool
}

func (s *searchScope) headers(header http.Header) {
	if s.source != "" {
		header.Set("X-Antfly-Source-ID", s.source)
	}
	if s.access != "" {
		header.Set("X-Antfly-Source-Access", s.access)
	}
	if s.publication != "" {
		header.Set("X-Antfly-Source-Publication", s.publication)
	}
}
func (s *searchScope) observe(header http.Header) error {
	subject := header.Get("X-Antfly-Subject")
	access := header.Get("X-Antfly-Source-Access")
	publication := header.Get("X-Antfly-Source-Publication")
	if s.subject != "" && s.subject != subject {
		return fmt.Errorf("source query identity changed")
	}
	if subject != "" {
		s.subject = subject
	}
	if access != "" || publication != "" {
		s.hosted = true
	}
	if !s.hosted {
		return nil
	}
	if subject == "" || access == "" || publication == "" || access == "changed" || publication == "changed" {
		return fmt.Errorf("source authorization/publication context unavailable or changed")
	}
	if s.subject != "" && s.subject != subject || s.access != "" && s.access != access || s.publication != "" && s.publication != publication {
		return fmt.Errorf("source authorization/publication context changed")
	}
	s.subject, s.access, s.publication = subject, access, publication
	return nil
}
func scopedSearchContext(ctx context.Context, binding Binding) context.Context {
	return context.WithValue(ctx, searchScopeKey{}, &searchScope{source: binding.ID, access: binding.AccessID, hosted: binding.AccessID != ""})
}
