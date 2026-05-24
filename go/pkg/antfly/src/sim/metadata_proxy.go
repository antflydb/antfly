// Copyright 2026 Antfly, Inc.
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

package sim

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/src/metadata/kv"
	"github.com/cockroachdb/pebble/v2"
)

type metadataDBProxy struct {
	leader func() (*metadataNode, error)
	run    func(timeout time.Duration, fn func() error) error
}

var _ kv.DB = (*metadataDBProxy)(nil)

func (p *metadataDBProxy) Get(ctx context.Context, key []byte) ([]byte, io.Closer, error) {
	node, err := p.leader()
	if err != nil {
		return nil, nil, err
	}
	if node.kv == nil {
		return nil, nil, fmt.Errorf("metadata leader unavailable")
	}
	return node.kv.Get(ctx, key)
}

func (p *metadataDBProxy) NewIter(ctx context.Context, opts *pebble.IterOptions) (*pebble.Iterator, error) {
	node, err := p.leader()
	if err != nil {
		return nil, err
	}
	if node.kv == nil {
		return nil, fmt.Errorf("metadata leader unavailable")
	}
	return node.kv.NewIter(ctx, opts)
}

func (p *metadataDBProxy) Batch(ctx context.Context, writes [][2][]byte, deletes [][]byte) error {
	node, err := p.leader()
	if err != nil {
		return err
	}
	if node.kv == nil {
		return fmt.Errorf("metadata leader unavailable")
	}
	if p.run == nil {
		return node.kv.Batch(ctx, writes, deletes)
	}
	return p.run(simulatorControlPlaneTimeout, func() error {
		return node.kv.Batch(ctx, writes, deletes)
	})
}
