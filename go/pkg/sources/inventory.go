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

package sources

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"net/http"
)

// ValidateInventory rechecks acquisition authority on cached or empty manifests.
// A persisted manifest or staged payload never grants current provider access.
func (c *GoogleConnector) ValidateInventory(ctx context.Context, r Request, s Inventory) error {
	return c.ValidateCachedSnapshot(ctx, r, s)
}
func (c *GitHubClient) CheckRepositoryAccess(ctx context.Context, token, repository string) error {
	owner, repo, err := ParseRepository(repository)
	if err != nil {
		return err
	}
	_, _, err = c.newClient(token).Repositories.Get(ctx, owner, repo)
	return err
}
func (c *GitHubConnector) ValidateInventory(ctx context.Context, r Request, _ Inventory) error {
	if err := c.Validate(ctx, r); err != nil {
		return err
	}
	access, err := c.access(ctx)
	if err != nil {
		return err
	}
	_, selections, err := GitHubSelections(r.Config)
	if err != nil {
		return err
	}
	checker, ok := c.Client.(interface {
		CheckRepositoryAccess(context.Context, string, string) error
	})
	if !ok {
		return fmt.Errorf("GitHub client cannot establish current repository access")
	}
	for _, selection := range selections {
		if err := checker.CheckRepositoryAccess(ctx, access.Token, selection.Repository); err != nil {
			return err
		}
	}
	return nil
}
func (c *S3Connector) ValidateInventory(ctx context.Context, r Request, _ Inventory) error {
	cfg, err := decodeSourceConfig[S3Config](r.Config)
	if err != nil {
		return err
	}
	if cfg.Key != "" {
		_, err := c.Client.StatObject(ctx, cfg.Bucket, cfg.Key, minio.StatObjectOptions{})
		if err != nil && minio.ToErrorResponse(err).StatusCode == http.StatusNotFound {
			return nil
		}
		return err
	}
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for item := range c.Client.ListObjects(listCtx, cfg.Bucket, minio.ListObjectsOptions{Prefix: cfg.Prefix, Recursive: true, MaxKeys: 1}) {
		return item.Err
	}
	return ctx.Err()
}
