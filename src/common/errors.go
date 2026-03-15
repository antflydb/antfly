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

package common

import (
	"fmt"

	"github.com/antflydb/antfly/lib/types"
)

type ErrKeyOutOfRange struct {
	Key   []byte
	Range types.Range
}

func NewErrKeyOutOfRange(key []byte, r types.Range) ErrKeyOutOfRange {
	return ErrKeyOutOfRange{
		Key:   key,
		Range: r,
	}
}

func (e ErrKeyOutOfRange) Error() string {
	return fmt.Sprintf("key %s out of range %s", types.FormatKey(e.Key), e.Range)
}
