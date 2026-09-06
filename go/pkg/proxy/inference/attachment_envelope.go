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
	"errors"
	"io"
	"mime"
	"strings"
)

const (
	proxyAttachmentEnvelopeContentType = "application/vnd.antfly.attachments.v1"
	proxyAttachmentEnvelopeMagic       = "AFATT001"
	proxyAttachmentEnvelopeHeaderBytes = 24
	proxyAttachmentDescriptorBytes     = 16
	proxyAttachmentMaxMetadataBytes    = 16 << 20
	proxyAttachmentMaxCount            = 1024
	proxyAttachmentMaxMIMEBytes        = 1024
)

func proxyUsesAttachmentEnvelope(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && strings.EqualFold(mediaType, proxyAttachmentEnvelopeContentType)
}

// readProxyAttachmentRoutingPrefix reads only the fixed envelope table and
// JSON metadata needed for routing. Attachment payload bytes remain on the
// incoming stream and can be forwarded or spooled without a heap-sized copy.
// Framed transport deliberately requires Content-Length: v1 descriptors make
// the exact wire length knowable, and rejecting ambiguous chunked bodies keeps
// descriptor/length validation deterministic at the proxy boundary.
func readProxyAttachmentRoutingPrefix(reader io.Reader, contentLength, maxBytes int64) ([]byte, []byte, error) {
	if contentLength < 0 || contentLength > maxBytes {
		return nil, nil, errInvalidProxyAttachmentEnvelope
	}
	header := make([]byte, proxyAttachmentEnvelopeHeaderBytes)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, nil, errInvalidProxyAttachmentEnvelope
	}
	if !bytes.Equal(header[:8], []byte(proxyAttachmentEnvelopeMagic)) {
		return nil, nil, errInvalidProxyAttachmentEnvelope
	}
	if binary.LittleEndian.Uint32(header[20:24]) != 0 {
		return nil, nil, errUnsupportedProxyAttachmentEnvelope
	}
	metadataBytes := binary.LittleEndian.Uint64(header[8:16])
	attachmentCount := binary.LittleEndian.Uint32(header[16:20])
	if metadataBytes > proxyAttachmentMaxMetadataBytes || attachmentCount > proxyAttachmentMaxCount {
		return nil, nil, errInvalidProxyAttachmentEnvelope
	}
	descriptorBytes := uint64(attachmentCount) * proxyAttachmentDescriptorBytes
	prefixBytes := uint64(proxyAttachmentEnvelopeHeaderBytes) + descriptorBytes + metadataBytes
	if prefixBytes > uint64(contentLength) || prefixBytes > uint64(maxBytes) || prefixBytes > uint64(^uint(0)>>1) {
		return nil, nil, errInvalidProxyAttachmentEnvelope
	}
	prefix := make([]byte, int(prefixBytes))
	copy(prefix, header)
	if _, err := io.ReadFull(reader, prefix[proxyAttachmentEnvelopeHeaderBytes:]); err != nil {
		return nil, nil, errInvalidProxyAttachmentEnvelope
	}

	totalBytes := prefixBytes
	for index := uint32(0); index < attachmentCount; index++ {
		descriptorOffset := proxyAttachmentEnvelopeHeaderBytes + int(index)*proxyAttachmentDescriptorBytes
		if binary.LittleEndian.Uint32(prefix[descriptorOffset+4:descriptorOffset+8]) != 0 {
			return nil, nil, errUnsupportedProxyAttachmentEnvelope
		}
		mimeBytes := binary.LittleEndian.Uint32(prefix[descriptorOffset : descriptorOffset+4])
		dataBytes := binary.LittleEndian.Uint64(prefix[descriptorOffset+8 : descriptorOffset+16])
		if mimeBytes == 0 || mimeBytes > proxyAttachmentMaxMIMEBytes || dataBytes == 0 {
			return nil, nil, errInvalidProxyAttachmentEnvelope
		}
		if dataBytes > ^uint64(0)-uint64(mimeBytes)-totalBytes {
			return nil, nil, errInvalidProxyAttachmentEnvelope
		}
		totalBytes += uint64(mimeBytes) + dataBytes
	}
	if totalBytes != uint64(contentLength) || totalBytes > uint64(maxBytes) {
		return nil, nil, errInvalidProxyAttachmentEnvelope
	}
	metadataStart := proxyAttachmentEnvelopeHeaderBytes + int(descriptorBytes)
	return prefix, prefix[metadataStart:], nil
}

var (
	errInvalidProxyAttachmentEnvelope     = errors.New("invalid attachment envelope")
	errUnsupportedProxyAttachmentEnvelope = errors.New("unsupported attachment envelope")
)

// proxyAttachmentEnvelopeMetadata validates the complete task-neutral binary
// envelope and returns a borrowed view of its JSON metadata. The proxy needs
// only that view to select a model; it forwards the original body unchanged so
// attachment bytes are neither decoded nor copied at the routing boundary.
func proxyAttachmentEnvelopeMetadata(body []byte) ([]byte, error) {
	if len(body) < proxyAttachmentEnvelopeHeaderBytes || !bytes.Equal(body[:8], []byte(proxyAttachmentEnvelopeMagic)) {
		return nil, errInvalidProxyAttachmentEnvelope
	}
	if binary.LittleEndian.Uint32(body[20:24]) != 0 {
		return nil, errUnsupportedProxyAttachmentEnvelope
	}
	metadataBytes := binary.LittleEndian.Uint64(body[8:16])
	attachmentCount := binary.LittleEndian.Uint32(body[16:20])
	if metadataBytes > proxyAttachmentMaxMetadataBytes || attachmentCount > proxyAttachmentMaxCount {
		return nil, errInvalidProxyAttachmentEnvelope
	}
	descriptorBytes := uint64(attachmentCount) * proxyAttachmentDescriptorBytes
	payloadOffset := uint64(proxyAttachmentEnvelopeHeaderBytes) + descriptorBytes
	metadataEnd := payloadOffset + metadataBytes
	if metadataEnd < payloadOffset || metadataEnd > uint64(len(body)) {
		return nil, errInvalidProxyAttachmentEnvelope
	}
	payloadOffset = metadataEnd
	for index := uint32(0); index < attachmentCount; index++ {
		descriptorOffset := proxyAttachmentEnvelopeHeaderBytes + int(index)*proxyAttachmentDescriptorBytes
		if binary.LittleEndian.Uint32(body[descriptorOffset+4:descriptorOffset+8]) != 0 {
			return nil, errUnsupportedProxyAttachmentEnvelope
		}
		mimeBytes := binary.LittleEndian.Uint32(body[descriptorOffset : descriptorOffset+4])
		dataBytes := binary.LittleEndian.Uint64(body[descriptorOffset+8 : descriptorOffset+16])
		if mimeBytes == 0 || mimeBytes > proxyAttachmentMaxMIMEBytes || dataBytes == 0 {
			return nil, errInvalidProxyAttachmentEnvelope
		}
		payloadEnd := payloadOffset + uint64(mimeBytes) + dataBytes
		if payloadEnd < payloadOffset || payloadEnd > uint64(len(body)) {
			return nil, errInvalidProxyAttachmentEnvelope
		}
		payloadOffset = payloadEnd
	}
	if payloadOffset != uint64(len(body)) {
		return nil, errInvalidProxyAttachmentEnvelope
	}
	return body[proxyAttachmentEnvelopeHeaderBytes+int(descriptorBytes) : int(metadataEnd)], nil
}

// proxyRoutingPayload returns the JSON bytes used only for routing. Binary
// attachment envelopes are validated before their metadata is exposed.
func proxyRoutingPayload(body []byte, contentType string) ([]byte, bool, error) {
	if !proxyUsesAttachmentEnvelope(contentType) {
		return body, false, nil
	}
	metadata, err := proxyAttachmentEnvelopeMetadata(body)
	if err != nil {
		return nil, true, err
	}
	return metadata, true, nil
}
