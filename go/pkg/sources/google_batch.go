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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
)

const GCSAccessBatchLimit = 100

// CheckGCSAccessBatch verifies the exact revision of each object using metadata
// requests. Results are ordered and include per-object provider errors.
func (c *GoogleConnector) CheckGCSAccessBatch(ctx context.Context, r Request, items []Item) ([]error, error) {
	if c.ProviderName != ProviderGCS || len(items) == 0 || len(items) > GCSAccessBatchLimit {
		return nil, ErrInvalidInput
	}
	cfg, err := decodeSourceConfig[GoogleConfig](r.Config)
	if err != nil {
		return nil, err
	}
	client, err := c.client(ctx, r)
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for i, item := range items {
		path, err := gcsObjectAccessPath(cfg, item)
		if err != nil {
			return nil, err
		}
		part, err := writer.CreatePart(textproto.MIMEHeader{
			"Content-Type": {"application/http"},
			"Content-Id":   {"<" + strconv.Itoa(i) + ">"},
		})
		if err != nil {
			return nil, err
		}
		if _, err := fmt.Fprintf(part, "GET /storage/v1%s HTTP/1.1\r\n\r\n", path); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	endpoint := "https://storage.googleapis.com/batch/storage/v1"
	if c.APIBase != "" {
		endpoint = c.APIBase + "/batch/storage/v1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+writer.Boundary())
	resp, err := client.Do(req)
	if err != nil {
		return nil, googleRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, googleResponseError(resp)
	}
	kind, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || kind != "multipart/mixed" || params["boundary"] == "" {
		return nil, fmt.Errorf("invalid GCS metadata batch response")
	}
	reader := multipart.NewReader(io.LimitReader(resp.Body, 4<<20), params["boundary"])
	results := make([]error, len(items))
	for i, item := range items {
		part, err := reader.NextPart()
		if err != nil {
			return nil, fmt.Errorf("read GCS metadata batch response: %w", err)
		}
		if part.Header.Get("Content-ID") != "<response-"+strconv.Itoa(i)+">" {
			return nil, fmt.Errorf("mismatched GCS metadata batch response")
		}
		reply, err := http.ReadResponse(bufio.NewReader(part), nil)
		if err != nil {
			return nil, err
		}
		results[i] = checkGCSAccessReply(reply, item)
	}
	if _, err := reader.NextPart(); err != io.EOF {
		return nil, fmt.Errorf("unexpected trailing GCS metadata batch response")
	}
	return results, nil
}

func checkGCSAccessReply(reply *http.Response, item Item) error {
	defer reply.Body.Close()
	if reply.StatusCode != http.StatusOK {
		return googleResponseError(reply)
	}
	var object struct {
		Generation string `json:"generation"`
	}
	if err := json.NewDecoder(io.LimitReader(reply.Body, 4096)).Decode(&object); err != nil {
		return err
	}
	if object.Generation != item.Revision {
		return &RevisionChangedError{Expected: item.Revision, Actual: object.Generation}
	}
	return nil
}
