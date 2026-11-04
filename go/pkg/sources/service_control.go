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
	"bytes"
	"net/http"
)

// Responses contain only bounded control metadata. Never hold the state mutex
// while waiting for a remote caller to receive a response.
type controlResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *controlResponse) Header() http.Header { return w.header }
func (w *controlResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *controlResponse) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	return w.body.Write(data)
}
func (w *controlResponse) flush(out http.ResponseWriter) {
	for key, values := range w.header {
		out.Header()[key] = values
	}
	if w.status == 0 {
		w.status = 200
	}
	out.WriteHeader(w.status)
	_, _ = out.Write(w.body.Bytes())
}
