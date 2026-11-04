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
	"crypto/hmac"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/minio/minio-go/v7"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const mediaReadWindow = 1 << 20

type mediaSeeker struct {
	ctx                     context.Context
	connector               MediaConnector
	request                 Request
	item                    Item
	ranges                  []MediaRange
	offset                  int64
	buffer                  []byte
	bufferOffset            int64
	body                    io.ReadCloser
	streamOffset, streamEnd int64
}

func (r *mediaSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.offset
	case io.SeekEnd:
		offset += r.item.Size
	default:
		return 0, ErrInvalidInput
	}
	if offset < 0 || offset > r.item.Size {
		return 0, ErrInvalidInput
	}
	r.offset = offset
	return offset, nil
}
func (r *mediaSeeker) Read(out []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.offset >= r.item.Size {
		return 0, io.EOF
	}
	if len(out) == 0 {
		return 0, nil
	}
	if len(r.buffer) > 0 && r.offset >= r.bufferOffset && r.offset < r.bufferOffset+int64(len(r.buffer)) {
		n := copy(out, r.buffer[r.offset-r.bufferOffset:])
		r.offset += int64(n)
		return n, nil
	}
	end := r.item.Size
	for _, span := range r.ranges {
		if r.offset >= span.Offset && r.offset < span.Offset+span.Length {
			end = span.Offset + span.Length
			break
		}
	}
	length := min(int64(mediaReadWindow), end-r.offset)
	stable := false
	if connector, ok := r.connector.(StableMediaConnector); ok {
		stable = connector.StableMediaRanges()
	}
	var body io.ReadCloser
	if stable && r.body != nil && r.streamOffset == r.offset && r.streamEnd >= r.offset+length {
		body = r.body
	} else {
		_ = r.Close()
		requested := length
		if stable {
			requested = end - r.offset
		}
		opened, err := r.connector.OpenMedia(r.ctx, r.request, r.item, MediaRange{r.offset, requested})
		if err != nil {
			return 0, mediaProviderError(err)
		}
		body = opened
		if stable {
			r.body = body
			r.streamOffset = r.offset
			r.streamEnd = r.offset + requested
		}
	}
	if cap(r.buffer) < int(length) {
		r.buffer = make([]byte, length)
	} else {
		r.buffer = r.buffer[:length]
	}
	_, readErr := io.ReadFull(body, r.buffer)
	var closeErr error
	if stable {
		r.streamOffset += length
		if readErr != nil || r.streamOffset == r.streamEnd {
			closeErr = r.Close()
		}
	} else {
		closeErr = body.Close()
	}
	if readErr != nil {
		return 0, readErr
	}
	if closeErr != nil {
		return 0, mediaProviderError(closeErr)
	}
	// The complete bounded window is verified before releasing any of it to HTTP.
	// This is required for Drive's before/after version fence. A version change
	// during a larger response truncates that response; it never mixes accepted
	// windows from different provider revisions under one successful receipt.
	r.bufferOffset = r.offset
	n := copy(out, r.buffer)
	r.offset += int64(n)
	return n, nil
}
func (r *mediaSeeker) Close() error {
	if r.body == nil {
		return nil
	}
	body := r.body
	r.body = nil
	return body.Close()
}
func (s *Service) serveMediaStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		serviceError(w, 405, "method_not_allowed")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/media/"), "/")
	if len(parts) != 2 || len(parts[0]) != 64 || len(parts[1]) != 64 {
		serviceError(w, 404, "media_not_found")
		return
	}
	id := parts[0]
	if _, err := hex.DecodeString(id); err != nil || !hmac.Equal([]byte(parts[1]), []byte(s.mediaSignature(id))) {
		serviceError(w, 404, "media_not_found")
		return
	}
	select {
	case s.fetchSlots <- struct{}{}:
		defer func() { <-s.fetchSlots }()
	default:
		w.Header().Set("Retry-After", "1")
		serviceError(w, 503, "fetch_busy")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	stopServiceCancel := context.AfterFunc(s.ctx, cancel)
	defer stopServiceCancel()
	// Context cancellation alone cannot interrupt a blocked socket write.
	controller := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = controller.SetWriteDeadline(deadline)
	writeStopped := make(chan struct{})
	stopWrite := context.AfterFunc(ctx, func() {
		_ = controller.SetWriteDeadline(time.Now())
		close(writeStopped)
	})
	defer func() {
		if !stopWrite() {
			<-writeStopped
		}
		_ = controller.SetWriteDeadline(time.Time{})
	}()

	pin, err := s.readMediaPin(id)
	if err != nil {
		serviceError(w, 410, "media_revoked")
		return
	}
	s.mu.Lock()
	entry, found := s.mediaRegistry[pin.SourceID]
	source := entry.source
	connection, exists := s.profile.Connections[pin.Connection]
	binding := s.connectionBindings[pin.Connection]
	generation := s.clientGeneration(connection)
	closed := s.closed
	s.mu.Unlock()
	if closed || !found || !exists || entry.binding != pin.SourceBinding || binding != pin.ConnectionBinding {
		serviceError(w, 410, "media_revoked")
		return
	}
	selected, err := s.mediaAuthorized(source.ID, itemMediaKey(pin.Item))
	if err != nil || !selected {
		serviceError(w, 410, "media_revoked")
		return
	}
	connector, err := s.providerConnector(ctx, connection)
	if err == nil {
		connector, err = publicationConnector(connector, source)
		if err == nil {
			connector = WithAcquisitionContentStore(connector, &FileAcquisitionContentStore{Directory: filepath.Join(s.runnerPath(), "content", source.ID)})
		}
	}
	var media MediaConnector
	if err == nil {
		var ok bool
		media, ok = connector.(MediaConnector)
		if !ok {
			err = fmt.Errorf("%w: provider does not support streaming media", ErrInvalidInput)
		}
	}
	request := Request{SourceNamespace: "job/" + source.ID, Config: source.Config, Mode: ImportModeSource}
	if err == nil {
		err = checkMediaAccess(ctx, connector, media, request, pin.Item)
	}
	if err != nil {
		serveMediaError(w, mediaProviderError(err))
		return
	}
	// A registry mutation during provider authorization cannot renew an old grant.
	s.mu.Lock()
	current, live := s.mediaRegistry[pin.SourceID]
	_, connected := s.profile.Connections[pin.Connection]
	revoked := s.closed || !live || !connected || current.binding != pin.SourceBinding || s.connectionBindings[pin.Connection] != pin.ConnectionBinding || generation != s.clientGeneration(connection)
	s.mu.Unlock()
	if revoked {
		serviceError(w, 410, "media_revoked")
		return
	}
	ServeMedia(w, r.WithContext(ctx), media, request, pin.Item, pin.SHA256)
}

func serveMediaError(w http.ResponseWriter, err error) {
	var changed *RevisionChangedError
	var google *GoogleHTTPError
	provider := minio.ToErrorResponse(err)
	if errors.As(err, &google) && google.Retryable() {
		if delay := time.Until(google.AntflyRetryAfter()); delay > 0 {
			w.Header().Set("Retry-After", strconv.FormatInt(int64(delay/time.Second)+1, 10))
		}
		serviceError(w, 503, "source_temporarily_unavailable")
		return
	}
	switch {
	case errors.As(err, &changed):
		serviceError(w, 412, "source_version_changed")
	case errors.As(err, &google) && (google.Status == 401 || google.Status == 403 || google.Status == 404):
		serviceError(w, 410, "source_access_revoked")
	case provider.StatusCode == 401 || provider.StatusCode == 403 || provider.Code == "NoSuchKey" || provider.Code == "NoSuchBucket":
		serviceError(w, 410, "source_access_revoked")
	case errors.Is(err, context.DeadlineExceeded):
		serviceError(w, 504, "source_fetch_timeout")
	case errors.Is(err, context.Canceled):
		serviceError(w, 408, "source_fetch_cancelled")
	case errors.Is(err, ErrInvalidInput):
		serviceError(w, 400, "invalid_media_source")
	default:
		serviceError(w, 502, "source_fetch_failed")
	}
}

// HTTP entity tags are comma-separated quoted strings; SHA tags contain no commas.
func etagListContains(value, etag string, weak bool) bool {
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "*" {
			return true
		}
		if weak {
			entry = strings.TrimPrefix(entry, "W/")
		}
		if entry == etag {
			return true
		}
	}
	return false
}

type mediaRegistrySource struct {
	source  Source
	binding string
}

// Called only while publishing a profile under the state mutex (or at startup).
func (s *Service) refreshMediaRegistry() {
	registry := make(map[string]mediaRegistrySource, len(s.profile.Sources))
	bindings := make(map[string]string, len(s.profile.Connections))
	for _, source := range s.profile.Sources {
		registry[source.ID] = mediaRegistrySource{source, sourceMediaBinding(source)}
	}
	for name, connection := range s.profile.Connections {
		bindings[name] = connectionMediaBinding(connection)
	}
	s.mediaRegistry, s.connectionBindings = registry, bindings
}
