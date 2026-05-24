// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

package embeddings

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/ai"
	json "github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/stretchr/testify/require"
)

type mockBedrockRuntime struct {
	bodies [][]byte
	resp   []byte
	resps  [][]byte
}

func (m *mockBedrockRuntime) InvokeModel(_ context.Context, in *bedrockruntime.InvokeModelInput, _ ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
	m.bodies = append(m.bodies, append([]byte(nil), in.Body...))
	resp := m.resp
	if len(m.resps) > 0 {
		resp = m.resps[0]
		m.resps = m.resps[1:]
	}
	return &bedrockruntime.InvokeModelOutput{Body: resp}, nil
}

func newTestBedrock(model string, client bedrockRuntimeClient) *BedrockImpl {
	return &BedrockImpl{
		client:    client,
		model:     model,
		batchSize: 100,
		caps:      ResolveCapabilities(model, nil),
	}
}

func decodeBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body))
	return body
}

func TestBedrockTitanMultimodalImageOnlyOmitsEmptyInputText(t *testing.T) {
	client := &mockBedrockRuntime{resp: []byte(`{"embedding":[0.1,0.2]}`)}
	emb := newTestBedrock("amazon.titan-embed-image-v1", client)
	emb.dimension = 384

	out, err := emb.Embed(context.Background(), [][]ai.ContentPart{{
		ai.BinaryContent{MIMEType: "image/png", Data: []byte{1, 2, 3}},
	}})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.1, 0.2}}, out)
	require.Len(t, client.bodies, 1)

	body := decodeBody(t, client.bodies[0])
	require.NotContains(t, body, "inputText")
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte{1, 2, 3}), body["inputImage"])
	cfg, ok := body["embeddingConfig"].(map[string]any)
	require.True(t, ok)
	require.EqualValues(t, 384, cfg["outputEmbeddingLength"])
}

func TestBedrockTitanMultimodalFusesTextAndImage(t *testing.T) {
	client := &mockBedrockRuntime{resp: []byte(`{"embedding":[0.3,0.4]}`)}
	emb := newTestBedrock("amazon.titan-embed-image-v1", client)

	_, err := emb.Embed(context.Background(), [][]ai.ContentPart{{
		ai.TextContent{Text: "red square"},
		ai.BinaryContent{MIMEType: "image/jpeg", Data: []byte{9, 8, 7}},
	}})
	require.NoError(t, err)

	body := decodeBody(t, client.bodies[0])
	require.Equal(t, "red square", body["inputText"])
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte{9, 8, 7}), body["inputImage"])
}

func TestBedrockTitanMultimodalImageURLDataURI(t *testing.T) {
	client := &mockBedrockRuntime{resp: []byte(`{"embedding":[0.3,0.4]}`)}
	emb := newTestBedrock("amazon.titan-embed-image-v1", client)

	_, err := emb.Embed(context.Background(), [][]ai.ContentPart{{
		ai.ImageURLContent{URL: "data:image/png;base64,AQID"},
	}})
	require.NoError(t, err)

	body := decodeBody(t, client.bodies[0])
	require.NotContains(t, body, "inputText")
	require.Equal(t, "AQID", body["inputImage"])
}

func TestBedrockTitanMultimodalRemoteImageURLRequiresRemoteMedia(t *testing.T) {
	client := &mockBedrockRuntime{resp: []byte(`{"embedding":[0.3,0.4]}`)}
	emb := newTestBedrock("amazon.titan-embed-image-v1", client)

	_, err := emb.Embed(context.Background(), [][]ai.ContentPart{{
		ai.ImageURLContent{URL: "https://example.com/image.png"},
	}})
	require.ErrorContains(t, err, "use remoteMedia")
	require.Empty(t, client.bodies)
}

func TestBedrockCohereTextBatchRequest(t *testing.T) {
	client := &mockBedrockRuntime{resp: []byte(`{"embeddings":[[0.1,0.2],[0.3,0.4]]}`)}
	emb := newTestBedrock("cohere.embed-english-v3", client)
	emb.inputType = "search_query"
	emb.truncate = "END"

	out, err := emb.Embed(context.Background(), [][]ai.ContentPart{
		{ai.TextContent{Text: "first"}},
		{ai.TextContent{Text: "second"}},
	})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.1, 0.2}, {0.3, 0.4}}, out)

	body := decodeBody(t, client.bodies[0])
	require.Equal(t, "search_query", body["input_type"])
	require.Equal(t, "END", body["truncate"])
	require.Equal(t, []any{"first", "second"}, body["texts"])
}

func TestBedrockCohereTextRespectsProviderBatchLimit(t *testing.T) {
	client := &mockBedrockRuntime{
		resps: [][]byte{
			[]byte(`{"embeddings":[[0.1]]}`),
			[]byte(`{"embeddings":[[0.2]]}`),
		},
	}
	emb := newTestBedrock("cohere.embed-english-v3", client)
	emb.batchSize = 1000

	contents := make([][]ai.ContentPart, BedrockCohereMaxBatchSize+1)
	for i := range contents {
		contents[i] = []ai.ContentPart{ai.TextContent{Text: "doc"}}
	}
	_, err := emb.Embed(context.Background(), contents)
	require.NoError(t, err)
	require.Len(t, client.bodies, 2)

	first := decodeBody(t, client.bodies[0])
	second := decodeBody(t, client.bodies[1])
	require.Len(t, first["texts"], BedrockCohereMaxBatchSize)
	require.Len(t, second["texts"], 1)
}

func TestBedrockCohereV4MixedRequest(t *testing.T) {
	client := &mockBedrockRuntime{resp: []byte(`{"embeddings":{"float":[[0.5,0.6]]}}`)}
	emb := newTestBedrock("cohere.embed-v4", client)
	emb.inputType = "search_document"
	emb.dimension = 512

	out, err := emb.Embed(context.Background(), [][]ai.ContentPart{{
		ai.TextContent{Text: "caption"},
		ai.BinaryContent{MIMEType: "image/png", Data: []byte{4, 5, 6}},
	}})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.5, 0.6}}, out)

	body := decodeBody(t, client.bodies[0])
	require.Equal(t, "search_document", body["input_type"])
	require.EqualValues(t, 512, body["output_dimension"])
	require.Equal(t, []any{"float"}, body["embedding_types"])
	inputs, ok := body["inputs"].([]any)
	require.True(t, ok)
	content := inputs[0].(map[string]any)["content"].([]any)
	imagePart := content[1].(map[string]any)
	require.Equal(t, "image_url", imagePart["type"])
	require.Equal(t, "data:image/png;base64,"+base64.StdEncoding.EncodeToString([]byte{4, 5, 6}), imagePart["image_url"].(map[string]any)["url"])
}

func TestBedrockCohereV4ImageURLDataURI(t *testing.T) {
	client := &mockBedrockRuntime{resp: []byte(`{"embeddings":{"float":[[0.5,0.6]]}}`)}
	emb := newTestBedrock("cohere.embed-v4", client)

	_, err := emb.Embed(context.Background(), [][]ai.ContentPart{{
		ai.ImageURLContent{URL: "data:image/png;base64,AQID"},
	}})
	require.NoError(t, err)

	body := decodeBody(t, client.bodies[0])
	inputs := body["inputs"].([]any)
	content := inputs[0].(map[string]any)["content"].([]any)
	imagePart := content[0].(map[string]any)
	require.Equal(t, "image_url", imagePart["type"])
	require.Equal(t, "data:image/png;base64,AQID", imagePart["image_url"].(map[string]any)["url"])
}

func TestBedrockCohereV4RemoteImageURLRequiresRemoteMedia(t *testing.T) {
	client := &mockBedrockRuntime{resp: []byte(`{"embeddings":{"float":[[0.5,0.6]]}}`)}
	emb := newTestBedrock("cohere.embed-v4", client)

	_, err := emb.Embed(context.Background(), [][]ai.ContentPart{{
		ai.ImageURLContent{URL: "https://example.com/image.png"},
	}})
	require.ErrorContains(t, err, "use remoteMedia")
	require.Empty(t, client.bodies)
}

func TestBedrockCohereV4MixedRespectsProviderBatchLimit(t *testing.T) {
	client := &mockBedrockRuntime{
		resps: [][]byte{
			[]byte(`{"embeddings":{"float":[[0.1]]}}`),
			[]byte(`{"embeddings":{"float":[[0.2]]}}`),
		},
	}
	emb := newTestBedrock("cohere.embed-v4", client)
	emb.batchSize = 1000

	contents := make([][]ai.ContentPart, BedrockCohereMaxBatchSize+1)
	for i := range contents {
		contents[i] = []ai.ContentPart{
			ai.TextContent{Text: "caption"},
			ai.BinaryContent{MIMEType: "image/png", Data: []byte{byte(i)}},
		}
	}
	_, err := emb.Embed(context.Background(), contents)
	require.NoError(t, err)
	require.Len(t, client.bodies, 2)

	first := decodeBody(t, client.bodies[0])
	second := decodeBody(t, client.bodies[1])
	require.Len(t, first["inputs"], BedrockCohereMaxBatchSize)
	require.Len(t, second["inputs"], 1)
}

func TestParseBedrockEmbeddingsResponseVariants(t *testing.T) {
	for _, raw := range [][]byte{
		[]byte(`{"embedding":[1,2]}`),
		[]byte(`{"embeddings":[[1,2]]}`),
		[]byte(`{"embeddings":{"float":[[1,2]]}}`),
	} {
		got, err := parseBedrockEmbeddings(raw)
		require.NoError(t, err)
		require.Equal(t, [][]float32{{1, 2}}, got)
	}
}
