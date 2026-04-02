// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package metadata

import (
	"net/http"
	"time"
)

const apiServerReadTimeout = time.Minute

// NewAPIServer returns the shared metadata HTTP server configuration used by
// both standalone metadata nodes and swarm mode.
func NewAPIServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:        addr,
		Handler:     handler,
		ReadTimeout: apiServerReadTimeout,
	}
}
