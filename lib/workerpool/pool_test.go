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

package workerpool

import (
	"testing"
)

func TestNewPool(t *testing.T) {
	p, err := NewPool(WithSize(4))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	if got := p.Cap(); got != 4 {
		t.Errorf("Cap() = %d, want 4", got)
	}
}

func TestNewPoolDefault(t *testing.T) {
	p, err := NewPool()
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	if p.Cap() <= 0 {
		t.Errorf("Cap() = %d, want > 0", p.Cap())
	}
}
