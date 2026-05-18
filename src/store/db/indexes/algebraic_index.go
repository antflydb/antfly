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

package indexes

import (
	"context"
	"fmt"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
)

const algebraicGoStubMessage = "algebraic index runtime is provided by Zig and is not active in the Go store"

type AlgebraicIndex struct {
	name string
}

func init() {
	RegisterIndex(IndexTypeAlgebraic, NewAlgebraicIndex)
}

func NewAlgebraicIndex(
	_ *zap.Logger,
	_ *common.Config,
	_ *pebble.DB,
	_ string,
	name string,
	config *IndexConfig,
	_ *pebbleutils.Cache,
) (Index, error) {
	if _, err := config.AsAlgebraicIndexConfig(); err != nil {
		return nil, fmt.Errorf("reading algebraic config: %w", err)
	}
	return &AlgebraicIndex{name: name}, nil
}

func (a *AlgebraicIndex) Name() string {
	return a.name
}

func (a *AlgebraicIndex) Type() IndexType {
	return IndexTypeAlgebraic
}

func (a *AlgebraicIndex) Batch(_ context.Context, _ [][2][]byte, _ [][]byte, _ bool) error {
	return nil
}

func (a *AlgebraicIndex) Search(_ context.Context, _ any) (any, error) {
	return nil, fmt.Errorf(algebraicGoStubMessage)
}

func (a *AlgebraicIndex) Close() error {
	return nil
}

func (a *AlgebraicIndex) Delete() error {
	return nil
}

func (a *AlgebraicIndex) Open(_ bool, _ *schema.TableSchema, _ types.Range) error {
	return nil
}

func (a *AlgebraicIndex) Stats() IndexStats {
	return AlgebraicIndexStats{
		IndexType: AlgebraicIndexStatsIndexTypeAlgebraic,
		Error:     algebraicGoStubMessage,
		Healthy:   false,
	}.AsIndexStats()
}

func (a *AlgebraicIndex) UpdateRange(_ types.Range) error {
	return nil
}

func (a *AlgebraicIndex) UpdateSchema(_ *schema.TableSchema) error {
	return nil
}

func (a *AlgebraicIndex) Pause(_ context.Context) error {
	return nil
}

func (a *AlgebraicIndex) Resume() {}
