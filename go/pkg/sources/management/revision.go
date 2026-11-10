// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package management

import (
	"context"
	"errors"
)

// SourceRevision returns an opaque revision of a source's declaration.
func SourceRevision(s Source) string {
	return ""
}

// PreconditionBackend optionally supports atomic declaration compare-and-set.
type PreconditionBackend interface {
	PutSourceIfMatch(ctx context.Context, source Source, revision string) (Source, error)
}

// PutSourceIfMatch requires a matching declaration revision for a PUT.
func (c *Client) PutSourceIfMatch(ctx context.Context, source Source, revision string) (Source, error) {
	return Source{}, errors.New("not implemented")
}
