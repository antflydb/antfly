// Copyright 2015 The etcd Authors
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

package multirafthttp

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/multirafthttp/transport"
	"github.com/antflydb/antfly/lib/types"

	"go.uber.org/zap"
)

var errMemberRemoved = fmt.Errorf("the member has been permanently removed from the cluster")

// NewRoundTripper returns a roundTripper used to send requests
// to rafthttp listener of remote peers.
// The returned io.Closer must be closed to release HTTP/3 resources (nil if not configured).
func NewRoundTripper(tlsInfo transport.TLSInfo, dialTimeout time.Duration) (http.RoundTripper, io.Closer, error) {
	// It uses timeout transport to pair with remote timeout listeners.
	// It sets no read/write timeout, because message in requests may
	// take long time to write out before reading out the response.
	return transport.NewTimeoutTransport(tlsInfo, dialTimeout, 0, 0)
}

// NewStreamRoundTripper returns a roundTripper used to send stream requests
// to rafthttp listener of remote peers.
// Read/write timeout is set for stream roundTripper to promptly
// find out broken status, which minimizes the number of messages
// sent on broken connection.
// The returned io.Closer must be closed to release HTTP/3 resources (nil if not configured).
func NewStreamRoundTripper(tlsInfo transport.TLSInfo, dialTimeout time.Duration) (http.RoundTripper, io.Closer, error) {
	return transport.NewTimeoutTransport(tlsInfo, dialTimeout, ConnReadTimeout, ConnWriteTimeout)
}

// createPostRequest creates an HTTP POST request that sends a raft message.
func createPostRequest(lg *zap.Logger, u url.URL, path string, body io.Reader, ct string, urls types.URLs, from, shardID types.ID) *http.Request {
	uu := u
	uu.Path = path
	req, err := http.NewRequest(http.MethodPost, uu.String(), body)
	if err != nil {
		if lg != nil {
			lg.Panic("unexpected new request error", zap.Error(err))
		}
	}
	req.Header.Set("Content-Type", ct)
	req.Header.Set("X-Server-From", from.String())
	req.Header.Set("X-Server-Version", Version)
	req.Header.Set("X-Min-Cluster-Version", MinClusterVersion)
	req.Header.Set("X-Raft-Shard-ID", shardID.String())
	setPeerURLsHeader(req, urls)

	return req
}

// createGetRequest creates an HTTP GET request for raft communication.
func createGetRequest(lg *zap.Logger, u url.URL, path string, urls types.URLs, from, shardID types.ID) *http.Request {
	uu := u
	uu.Path = path
	req, err := http.NewRequest(http.MethodGet, uu.String(), nil)
	if err != nil {
		if lg != nil {
			lg.Panic("unexpected new request error", zap.Error(err))
		}
	}
	req.Header.Set("X-Server-From", from.String())
	req.Header.Set("X-Server-Version", Version)
	req.Header.Set("X-Min-Cluster-Version", MinClusterVersion)
	req.Header.Set("X-Raft-Shard-ID", shardID.String())
	setPeerURLsHeader(req, urls)

	return req
}

func checkGetResponse(lg *zap.Logger, resp *http.Response, body []byte, req *http.Request, to types.ID) error {
	return checkResponse(lg, resp, body, req, to, http.StatusNoContent, http.StatusOK)
}

// checkPostResponse checks the response of the HTTP POST request that sends
// raft message.
func checkPostResponse(lg *zap.Logger, resp *http.Response, body []byte, req *http.Request, to types.ID) error {
	return checkResponse(lg, resp, body, req, to, http.StatusNoContent)
}

func checkResponse(lg *zap.Logger, resp *http.Response, body []byte, req *http.Request, to types.ID, acceptedStatuses ...int) error {
	switch resp.StatusCode {
	case http.StatusPreconditionFailed:
		switch strings.TrimSuffix(string(body), "\n") {
		case errIncompatibleVersion.Error():
			if lg != nil {
				lg.Error(
					"request sent was ignored by peer",
					zap.Stringer("remote-peer-id", to),
				)
			}
			return errIncompatibleVersion
		case ErrClusterIDMismatch.Error():
			if lg != nil {
				lg.Error(
					"request sent was ignored due to cluster ID mismatch",
					zap.Stringer("remote-peer-id", to),
					zap.String("remote-peer-shard-id", resp.Header.Get("X-Raft-Shard-ID")),
					zap.String("local-member-shard-id", req.Header.Get("X-Raft-Shard-ID")),
				)
			}
			return ErrClusterIDMismatch
		default:
			return fmt.Errorf("unhandled error %q when precondition failed", string(body))
		}
	case http.StatusForbidden:
		return errMemberRemoved
	default:
		if slices.Contains(acceptedStatuses, resp.StatusCode) {
			return nil
		}
		return fmt.Errorf("unexpected http status %s while requesting %q: %s",
			http.StatusText(resp.StatusCode), req.URL.String(), string(body))
	}
}

// reportCriticalError reports the given error through sending it into
// the given error channel.
// If the error channel is filled up when sending error, it drops the error
// because the fact that error has happened is reported, which is
// good enough.
func reportCriticalError(err error, errc chan<- error) {
	select {
	case errc <- err:
	default:
	}
}

// setPeerURLsHeader reports local urls for peer discovery
func setPeerURLsHeader(req *http.Request, urls types.URLs) {
	if urls == nil {
		// often not set in unit tests
		return
	}
	peerURLs := make([]string, urls.Len())
	for i := range urls {
		peerURLs[i] = urls[i].String()
	}
	req.Header.Set("X-PeerURLs", strings.Join(peerURLs, ","))
}

// addRemoteFromRequest adds a remote peer according to an http request header
func addRemoteFromRequest(tr Transporter, r *http.Request) {
	if from, err := types.IDFromString(r.Header.Get("X-Server-From")); err == nil {
		if shardID, err := types.IDFromString(r.Header.Get("X-Raft-Shard-ID")); err == nil {
			if urls := r.Header.Get("X-PeerURLs"); urls != "" {
				tr.AddRemote(shardID, from, strings.Split(urls, ","))
			}
		}
	}
}
