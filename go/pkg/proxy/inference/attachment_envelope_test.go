// Copyright 2026 Antfly, Inc.
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

package proxy

import (
	"bytes"
	"encoding/binary"
	"testing"
)

type testProxyAttachment struct {
	mime string
	data []byte
}

func TestProxyAttachmentRoutingPrefixLeavesMediaOnStream(t *testing.T) {
	metadata := []byte(`{"model":"owner/reader","images":[{"url":"attachment:0"}]}`)
	body := testProxyAttachmentEnvelope(metadata, testProxyAttachment{mime: "image/png", data: []byte{1, 2, 3}})
	reader := bytes.NewReader(body)
	prefix, routing, err := readProxyAttachmentRoutingPrefix(reader, int64(len(body)), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	if string(routing) != string(metadata) {
		t.Fatalf("routing metadata = %q, want %q", routing, metadata)
	}
	if len(prefix)+reader.Len() != len(body) {
		t.Fatalf("prefix=%d remaining=%d total=%d", len(prefix), reader.Len(), len(body))
	}
	if reader.Len() != len("image/png")+3 {
		t.Fatalf("media tail was materialized while routing: remaining=%d", reader.Len())
	}
	if _, _, err := readProxyAttachmentRoutingPrefix(bytes.NewReader(body), -1, int64(len(body))); err == nil {
		t.Fatal("chunked v1 attachment body was accepted")
	}
}

func testProxyAttachmentEnvelope(metadata []byte, attachments ...testProxyAttachment) []byte {
	descriptorBytes := len(attachments) * proxyAttachmentDescriptorBytes
	total := proxyAttachmentEnvelopeHeaderBytes + descriptorBytes + len(metadata)
	for _, attachment := range attachments {
		total += len(attachment.mime) + len(attachment.data)
	}
	body := make([]byte, total)
	copy(body[:8], proxyAttachmentEnvelopeMagic)
	binary.LittleEndian.PutUint64(body[8:16], uint64(len(metadata)))
	binary.LittleEndian.PutUint32(body[16:20], uint32(len(attachments)))
	offset := proxyAttachmentEnvelopeHeaderBytes
	for _, attachment := range attachments {
		binary.LittleEndian.PutUint32(body[offset:offset+4], uint32(len(attachment.mime)))
		binary.LittleEndian.PutUint64(body[offset+8:offset+16], uint64(len(attachment.data)))
		offset += proxyAttachmentDescriptorBytes
	}
	copy(body[offset:], metadata)
	offset += len(metadata)
	for _, attachment := range attachments {
		copy(body[offset:], attachment.mime)
		offset += len(attachment.mime)
		copy(body[offset:], attachment.data)
		offset += len(attachment.data)
	}
	return body
}

func TestProxyAttachmentEnvelopeReturnsBorrowedRoutingMetadata(t *testing.T) {
	metadata := []byte(`{"model":"owner/reader","images":["attachment:0"]}`)
	body := testProxyAttachmentEnvelope(metadata, testProxyAttachment{mime: "image/png", data: []byte{1, 2, 3}})
	routing, framed, err := proxyRoutingPayload(body, proxyAttachmentEnvelopeContentType+"; version=1")
	if err != nil {
		t.Fatal(err)
	}
	if !framed {
		t.Fatal("attachment envelope was not detected")
	}
	if string(routing) != string(metadata) {
		t.Fatalf("metadata = %q, want %q", routing, metadata)
	}
	if &routing[0] != &body[proxyAttachmentEnvelopeHeaderBytes+proxyAttachmentDescriptorBytes] {
		t.Fatal("routing metadata was copied instead of borrowed from the admitted body")
	}
	model, err := proxyRequestModel(routing, "read")
	if err != nil || model != "owner/reader" {
		t.Fatalf("model = %q, err = %v", model, err)
	}
}

func TestProxyAttachmentEnvelopeValidatesWholeBodyBeforeRouting(t *testing.T) {
	valid := testProxyAttachmentEnvelope(
		[]byte(`{"model":"owner/reader"}`),
		testProxyAttachment{mime: "image/png", data: []byte{1}},
	)
	for name, mutate := range map[string]func([]byte) []byte{
		"bad magic": func(body []byte) []byte {
			body[0] = 'X'
			return body
		},
		"reserved header": func(body []byte) []byte {
			body[20] = 1
			return body
		},
		"reserved descriptor": func(body []byte) []byte {
			body[proxyAttachmentEnvelopeHeaderBytes+4] = 1
			return body
		},
		"trailing data": func(body []byte) []byte {
			return append(body, 0)
		},
		"truncated data": func(body []byte) []byte {
			return body[:len(body)-1]
		},
	} {
		t.Run(name, func(t *testing.T) {
			body := append([]byte(nil), valid...)
			if _, _, err := proxyRoutingPayload(mutate(body), proxyAttachmentEnvelopeContentType); err == nil {
				t.Fatal("malformed attachment envelope was accepted")
			}
		})
	}
}

func TestProxyRoutingPayloadLeavesJSONUntouched(t *testing.T) {
	body := []byte(`{"model":"owner/generator"}`)
	routing, framed, err := proxyRoutingPayload(body, "application/json")
	if err != nil || framed {
		t.Fatalf("framed = %v, err = %v", framed, err)
	}
	if &routing[0] != &body[0] {
		t.Fatal("JSON routing body was copied")
	}
}
