package antflyevalaf

import (
	"context"
	"fmt"
	"net/http"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/evalaf/eval"
)

// Type aliases for the generated types from the Antfly SDK.
// These provide access to query hits with full score information.
type (
	QueryResult = antfly.QueryResult
	QueryHit    = antfly.Hit
	QueryHits   = antfly.Hits
)

// Client wraps the Antfly SDK client for evalaf integration.
type Client struct {
	*antfly.AntflyClient
}

// NewClient creates a new Antfly client for evalaf.
func NewClient(baseURL string) (*Client, error) {
	if baseURL == "" {
		baseURL = "http://localhost:3210"
	}
	sdkClient, err := antfly.NewAntflyClient(baseURL, http.DefaultClient)
	if err != nil {
		return nil, fmt.Errorf("creating antfly client: %w", err)
	}
	return &Client{AntflyClient: sdkClient}, nil
}

// CallRetrievalAgent calls the Antfly RetrievalAgent endpoint and returns the full result including query hits with scores.
func (c *Client) CallRetrievalAgent(ctx context.Context, req antfly.RetrievalAgentRequest, opts ...antfly.RetrievalAgentOptions) (*antfly.RetrievalAgentResult, error) {
	return c.RetrievalAgent(ctx, req, opts...)
}

// CreateRetrievalAgentTargetFunc creates a target function for evaluating Antfly's RetrievalAgent endpoint.
func (c *Client) CreateRetrievalAgentTargetFunc(tables []string) eval.TargetFunc {
	return func(ctx context.Context, example eval.Example) (any, error) {
		query, ok := example.Input.(string)
		if !ok {
			return nil, fmt.Errorf("input must be a string")
		}

		resp, err := c.CallRetrievalAgent(ctx, antfly.RetrievalAgentRequest{
			Queries: []antfly.RetrievalQueryRequest{{Table: tables[0]}},
			Query:   query,
		})
		if err != nil {
			return nil, err
		}

		return resp.Generation, nil
	}
}

// CreateRetrievalAgentClassificationTargetFunc creates a target function that returns
// classification metadata (route_type, confidence) along with the answer.
func (c *Client) CreateRetrievalAgentClassificationTargetFunc(tables []string) eval.TargetFunc {
	return func(ctx context.Context, example eval.Example) (any, error) {
		query, ok := example.Input.(string)
		if !ok {
			return nil, fmt.Errorf("input must be a string")
		}

		resp, err := c.CallRetrievalAgent(ctx, antfly.RetrievalAgentRequest{
			Queries: []antfly.RetrievalQueryRequest{{Table: tables[0]}},
			Query:   query,
		})
		if err != nil {
			return nil, err
		}

		// Return structured response for classification evaluators
		return map[string]any{
			"route_type": resp.Classification.RouteType,
			"confidence": resp.Classification.Confidence,
			"generation": resp.Generation,
		}, nil
	}
}
