package reading

// BinaryContent represents binary data with a MIME type.
type BinaryContent struct {
	MIMEType string
	Data     []byte
}
