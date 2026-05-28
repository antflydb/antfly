package ai

import (
	"testing"

	aimessages "github.com/antflydb/antfly/go/pkg/antfly/lib/ai/messages"
)

func TestChatMessagesToGenKitPreservesMultimodalParts(t *testing.T) {
	textPart := aimessages.ContentPart{}
	if err := textPart.FromTextContentPart(aimessages.TextContentPart{
		Type: aimessages.TextContentPartTypeText,
		Text: "describe",
	}); err != nil {
		t.Fatalf("text part: %v", err)
	}

	imagePart := aimessages.ContentPart{}
	if err := imagePart.FromImageURLContentPart(aimessages.ImageURLContentPart{
		Type:     aimessages.ImageURLContentPartTypeImageUrl,
		ImageUrl: aimessages.ImageURL{Url: "data:image/png;base64,aaa"},
	}); err != nil {
		t.Fatalf("image part: %v", err)
	}

	audioURL := "https://example.test/audio.wav"
	audioMime := "audio/wav"
	mediaURLPart := aimessages.ContentPart{}
	if err := mediaURLPart.FromMediaContentPart(aimessages.MediaContentPart{
		Type:     aimessages.MediaContentPartTypeMedia,
		Url:      &audioURL,
		MimeType: &audioMime,
	}); err != nil {
		t.Fatalf("media url part: %v", err)
	}

	inlineData := []byte{1, 2, 3}
	inlineMime := "image/jpeg"
	inlineMediaPart := aimessages.ContentPart{}
	if err := inlineMediaPart.FromMediaContentPart(aimessages.MediaContentPart{
		Type:     aimessages.MediaContentPartTypeMedia,
		Data:     &inlineData,
		MimeType: &inlineMime,
	}); err != nil {
		t.Fatalf("inline media part: %v", err)
	}

	content := aimessages.ChatMessageContent{}
	if err := content.FromChatMessageContent1([]aimessages.ContentPart{
		textPart,
		imagePart,
		mediaURLPart,
		inlineMediaPart,
	}); err != nil {
		t.Fatalf("content: %v", err)
	}

	messages := ChatMessagesToGenKit([]ChatMessage{{
		Role:    ChatMessageRoleUser,
		Content: &content,
	}})

	if len(messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(messages))
	}
	parts := messages[0].Content
	if len(parts) != 4 {
		t.Fatalf("got %d parts, want 4", len(parts))
	}
	if parts[0].Text != "describe" {
		t.Fatalf("text part = %q", parts[0].Text)
	}
	if !parts[1].IsMedia() || parts[1].ContentType != "image/png" || parts[1].Text != "data:image/png;base64,aaa" {
		t.Fatalf("image part = %#v", parts[1])
	}
	if !parts[2].IsMedia() || parts[2].ContentType != "audio/wav" || parts[2].Text != audioURL {
		t.Fatalf("media url part = %#v", parts[2])
	}
	if !parts[3].IsMedia() || parts[3].ContentType != "image/jpeg" || parts[3].Text != "data:image/jpeg;base64,AQID" {
		t.Fatalf("inline media part = %#v", parts[3])
	}
}
