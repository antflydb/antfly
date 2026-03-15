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

package ai

// ContentPart is a part of a message content, can be text, binary, or image URL.
type ContentPart interface {
	isContentPart()
}

// TextContent represents textual content.
type TextContent struct {
	Text string
}

func (tc TextContent) isContentPart() {}

// BinaryContent represents binary data with a MIME type.
type BinaryContent struct {
	MIMEType string
	Data     []byte
}

func (bc BinaryContent) isContentPart() {}

// ImageURLContent represents an image referenced by URL.
type ImageURLContent struct {
	URL string
}

func (iuc ImageURLContent) isContentPart() {}
