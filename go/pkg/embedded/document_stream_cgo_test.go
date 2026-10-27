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

//go:build cgo && libantfly

package embedded

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Keep the model response fixtures shared between /extract and the durable
// /extract/execute transport. The latter must acknowledge a real checkpoint in
// the caller's Lite store; returning only a result frame would leave run debt.
func fakeDocumentExtractHandler(t *testing.T, extract http.HandlerFunc) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/extract/execute") {
			extract(w, r)
			return
		}
		if err := serveFakeDocumentExtract(w, r, extract); err != nil {
			t.Errorf("durable extractor fixture: %v", err)
		}
	})
}

type documentFixturePeer struct {
	w       http.ResponseWriter
	r       io.Reader
	request uint64
	id      json.RawMessage
	owner   json.RawMessage
}

func readDocumentFixtureFrame(r io.Reader) (header [32]byte, metadata, body []byte, err error) {
	if _, err = io.ReadFull(r, header[:]); err != nil {
		return
	}
	if string(header[:4]) != "AFDX" || header[4] != 1 || header[6] != 0 || header[7] != 0 {
		err = fmt.Errorf("invalid document frame header")
		return
	}
	m, n := binary.BigEndian.Uint32(header[24:28]), binary.BigEndian.Uint32(header[28:32])
	if m > 1024*1024 || n > 64*1024*1024 {
		err = fmt.Errorf("oversized document frame")
		return
	}
	metadata, body = make([]byte, m), make([]byte, n)
	if _, err = io.ReadFull(r, metadata); err == nil {
		_, err = io.ReadFull(r, body)
	}
	return
}

func (p *documentFixturePeer) send(kind byte, id uint64, code uint32, metadata, body []byte) error {
	var header [32]byte
	copy(header[:4], "AFDX")
	header[4], header[5] = 1, kind
	binary.BigEndian.PutUint64(header[8:16], id)
	binary.BigEndian.PutUint32(header[16:20], code)
	binary.BigEndian.PutUint32(header[24:28], uint32(len(metadata)))
	binary.BigEndian.PutUint32(header[28:32], uint32(len(body)))
	for _, part := range [][]byte{header[:], metadata, body} {
		if _, err := p.w.Write(part); err != nil {
			return err
		}
	}
	return http.NewResponseController(p.w).Flush()
}

func (p *documentFixturePeer) call(operation string, fields map[string]any, body []byte) ([]byte, []byte, error) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["operation"] = operation
	if p.id != nil {
		fields["id"], fields["owner"] = p.id, p.owner
	}
	metadata, err := json.Marshal(fields)
	if err != nil {
		return nil, nil, err
	}
	p.request++
	if err := p.send(2, p.request, 0, metadata, body); err != nil {
		return nil, nil, err
	}
	header, metadata, reply, err := readDocumentFixtureFrame(p.r)
	if err == nil && (header[5] != 3 || binary.BigEndian.Uint64(header[8:16]) != p.request || binary.BigEndian.Uint32(header[16:20]) != 0 || binary.BigEndian.Uint32(header[20:24]) != 0) {
		err = fmt.Errorf("document RPC %s rejected: status=%d detail=%d", operation, binary.BigEndian.Uint32(header[16:20]), binary.BigEndian.Uint32(header[20:24]))
	}
	return metadata, reply, err
}

func serveFakeDocumentExtract(w http.ResponseWriter, r *http.Request, extract http.HandlerFunc) error {
	if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
		return err
	}
	header, _, request, err := readDocumentFixtureFrame(r.Body)
	if err != nil {
		return err
	}
	if header[5] != 1 {
		return fmt.Errorf("expected document start frame")
	}
	legacy := r.Clone(r.Context())
	legacy.URL.Path = strings.TrimSuffix(r.URL.Path, "/execute")
	legacy.Body, legacy.ContentLength = io.NopCloser(bytes.NewReader(request)), int64(len(request))
	recorder := httptest.NewRecorder()
	extract(recorder, legacy)
	if recorder.Code != http.StatusOK {
		return fmt.Errorf("model fixture returned HTTP %d", recorder.Code)
	}
	payload := recorder.Body.Bytes()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	p := documentFixturePeer{w: w, r: r.Body}
	_, descriptor, err := p.call("describe", nil, nil)
	if err != nil {
		return err
	}
	var scope map[string]json.RawMessage
	if err := json.Unmarshal(descriptor, &scope); err != nil {
		return err
	}
	p.id, p.owner = scope["id"], scope["owner"]
	_, encoded, err := p.call("load", nil, nil)
	if err != nil {
		return err
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &record); err != nil {
		return err
	}
	set := func(key string, value any) error {
		encoded, err := json.Marshal(value)
		if err == nil {
			record[key] = encoded
		}
		return err
	}
	cas := func(change func() error) error {
		expected := map[string]json.RawMessage{"revision": record["revision"], "attempt": record["attempt"], "nonce": record["nonce"]}
		var revision uint64
		if err := json.Unmarshal(record["revision"], &revision); err != nil {
			return err
		}
		if err := change(); err != nil {
			return err
		}
		if err := set("revision", revision+1); err != nil {
			return err
		}
		body, err := json.Marshal(record)
		if err != nil {
			return err
		}
		changed, _, err := p.call("cas", map[string]any{"expected": expected}, body)
		if err == nil && string(changed) != "true" {
			err = fmt.Errorf("fixture checkpoint CAS conflict")
		}
		return err
	}
	if err := cas(func() error { return set("prepared", true) }); err != nil {
		return err
	}
	uploadScope := map[string]json.RawMessage{"run_id": record["run_id"], "owner": record["owner"], "attempt": record["attempt"], "nonce": record["nonce"]}
	put := func(body []byte) (json.RawMessage, error) {
		ref, _, err := p.call("put", map[string]any{"scope": uploadScope}, body)
		return json.RawMessage(ref), err
	}
	ref, err := put(payload)
	if err != nil {
		return err
	}
	var sequence uint64
	if err := json.Unmarshal(record["sequence"], &sequence); err != nil {
		return err
	}
	checkpoint, err := json.Marshal(map[string]any{"format": 1, "sequence": sequence + 1, "key": sha256.Sum256(request), "payload": ref, "parent": record["head"]})
	if err != nil {
		return err
	}
	head, err := put(checkpoint)
	if err != nil {
		return err
	}
	// Acknowledge the stage before terminal publication, as the real peer does.
	if err := cas(func() error {
		var usage, limits map[string]json.RawMessage
		if err := json.Unmarshal(record["used"], &usage); err != nil {
			return err
		}
		if err := json.Unmarshal(record["limits"], &limits); err != nil {
			return err
		}
		var used, ceiling uint64
		if err := json.Unmarshal(usage["output_bytes"], &used); err != nil {
			return err
		}
		if err := json.Unmarshal(limits["output_bytes"], &ceiling); err != nil {
			return err
		}
		if used > ceiling || uint64(len(payload)) > ceiling-used {
			return fmt.Errorf("fixture output exceeds the document budget")
		}
		usage["output_bytes"], err = json.Marshal(used + uint64(len(payload)))
		if err != nil {
			return err
		}
		if err := set("used", usage); err != nil {
			return err
		}
		record["head"] = head
		return set("sequence", sequence+1)
	}); err != nil {
		return err
	}
	if err := cas(func() error {
		record["result"] = ref
		if err := set("lease_until_ms", 0); err != nil {
			return err
		}
		return set("state", "succeeded")
	}); err != nil {
		return err
	}
	return p.send(4, 0, http.StatusOK, []byte(`{"version":1,"usage_known":true,"total_tokens":0}`), payload)
}
