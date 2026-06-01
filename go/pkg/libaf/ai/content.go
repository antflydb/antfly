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
