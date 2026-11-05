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
	"crypto/sha256"
	"fmt"
	"net/http"
	"time"
)

// ServeMedia implements the shared version-bound HTTP delivery protocol. The
// hosting runtime must authenticate the capability, resolve protected credentials,
// and check current provider access/version and source/object authority before calling it. No storage, tenant
// selection, credential installation or signing policy is owned by this function.
func ServeMedia(w http.ResponseWriter, r *http.Request, media MediaConnector, request Request, item Item, digest string) {
	if !validContentSHA(digest) || item.Size < 1 || media == nil {
		serviceError(w, 500, "invalid_media_receipt")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		serviceError(w, 405, "method_not_allowed")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	r = r.WithContext(ctx)
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = controller.SetWriteDeadline(deadline)
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = controller.SetWriteDeadline(time.Now()); close(stopped) })
	defer func() {
		if !stop() {
			<-stopped
		}
		_ = controller.SetWriteDeadline(time.Time{})
	}()
	etag := `"` + digest + `"`
	w.Header().Set("Content-Type", mediaMIME(item))
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// Hosted delivery shares an origin with authenticated UI routes. Provider
	// HTML/SVG must never execute there when someone follows a capability URL.
	w.Header().Set("Content-Disposition", "attachment")
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if match := r.Header.Get("If-Match"); match != "" && !etagListContains(match, etag, false) {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	if etagListContains(r.Header.Get("If-None-Match"), etag, true) {
		w.Header().Del("Content-Type")
		w.WriteHeader(http.StatusNotModified)
		return
	}
	effectiveRange := r.Header.Get("Range")
	if ifRange := r.Header.Get("If-Range"); ifRange != "" && ifRange != etag {
		effectiveRange = ""
	}
	spans, err := parseMediaRanges(effectiveRange, item.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", item.Size))
		serviceError(w, 416, "invalid_range")
		return
	}
	// Export endpoints and collaboration attachments do not support immutable
	// range reads. Acquire and verify once, then reuse only within this request.
	if r.Method == http.MethodGet {
		if acquirer, ok := media.(AcquiredMediaConnector); ok {
			data, acquired, err := acquirer.AcquireMedia(ctx, request, item)
			if err != nil {
				serveMediaError(w, err)
				return
			}
			if acquired {
				if int64(len(data)) != item.Size || fmt.Sprintf("%x", sha256.Sum256(data)) != digest {
					serveMediaError(w, &RevisionChangedError{Expected: item.Revision, Actual: "representation changed; synchronize this source again"})
					return
				}
				media = acquiredMedia{data: data}
			}
		}
	}
	seeker := &mediaSeeker{ctx: ctx, connector: media, request: request, item: item, ranges: spans}
	defer seeker.Close()
	// Validate the first bounded window before committing HTTP success headers.
	// Conditional responses and HEAD must not fetch the object body.
	if r.Method == http.MethodGet {
		if len(spans) > 0 {
			seeker.offset = spans[0].Offset
		}
		if _, err := seeker.Read(make([]byte, 1)); err != nil {
			serveMediaError(w, err)
			return
		}
		seeker.offset = 0
	}
	http.ServeContent(w, r, item.Filename, time.Time{}, seeker)
}

// WriteMediaError applies the common safe media error vocabulary after runtime
// authorization or provider access/version checks fail.
func WriteMediaError(w http.ResponseWriter, err error) { serveMediaError(w, mediaProviderError(err)) }
