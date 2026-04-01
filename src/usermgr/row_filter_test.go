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

package usermgr

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetRowFilter_Success(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	filter := json.RawMessage(`{"term":{"department":"eng"}}`)
	err := um.SetRowFilter("alice", "orders", filter)
	require.NoError(t, err)

	got, err := um.GetRowFilter("alice", "orders")
	require.NoError(t, err)
	assert.JSONEq(t, `{"term":{"department":"eng"}}`, string(got))
}

func TestSetRowFilter_ReplacesExisting(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	err := um.SetRowFilter("alice", "orders", json.RawMessage(`{"term":{"dept":"eng"}}`))
	require.NoError(t, err)

	err = um.SetRowFilter("alice", "orders", json.RawMessage(`{"term":{"dept":"hr"}}`))
	require.NoError(t, err)

	got, err := um.GetRowFilter("alice", "orders")
	require.NoError(t, err)
	assert.JSONEq(t, `{"term":{"dept":"hr"}}`, string(got))

	// Should have exactly one entry, not two.
	entries, err := um.ListRowFilters("alice")
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestSetRowFilter_NonexistentUser(t *testing.T) {
	um, _ := newTestUserManager(t)

	err := um.SetRowFilter("nonexistent", "orders", json.RawMessage(`{"term":{"x":"y"}}`))
	require.ErrorIs(t, err, ErrUserNotFound)
}

func TestSetRowFilter_InvalidJSON(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	err := um.SetRowFilter("alice", "orders", json.RawMessage(`not valid json`))
	require.Error(t, err)
}

func TestRemoveRowFilter_Success(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	err := um.SetRowFilter("alice", "orders", json.RawMessage(`{"term":{"x":"y"}}`))
	require.NoError(t, err)

	err = um.RemoveRowFilter("alice", "orders")
	require.NoError(t, err)

	_, err = um.GetRowFilter("alice", "orders")
	require.ErrorIs(t, err, ErrRowFilterNotFound)
}

func TestRemoveRowFilter_NotFound(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	err := um.RemoveRowFilter("alice", "nonexistent_table")
	require.ErrorIs(t, err, ErrRowFilterNotFound)
}

func TestGetRowFilters_MultipleTables(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	err := um.SetRowFilter("alice", "orders", json.RawMessage(`{"term":{"dept":"eng"}}`))
	require.NoError(t, err)
	err = um.SetRowFilter("alice", "docs", json.RawMessage(`{"term":{"status":"active"}}`))
	require.NoError(t, err)

	filters, err := um.GetRowFilters("alice")
	require.NoError(t, err)
	require.Len(t, filters, 2)

	assert.JSONEq(t, `{"term":{"dept":"eng"}}`, string(filters["orders"]))
	assert.JSONEq(t, `{"term":{"status":"active"}}`, string(filters["docs"]))
}

func TestGetRowFilters_Empty(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	filters, err := um.GetRowFilters("alice")
	require.NoError(t, err)
	assert.Nil(t, filters)
}

func TestGetRowFilters_WildcardTable(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	err := um.SetRowFilter("alice", "*", json.RawMessage(`{"term":{"active":"true"}}`))
	require.NoError(t, err)

	filters, err := um.GetRowFilters("alice")
	require.NoError(t, err)
	require.Len(t, filters, 1)
	assert.JSONEq(t, `{"term":{"active":"true"}}`, string(filters["*"]))
}

func TestListRowFilters_ReturnsAll(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	require.NoError(t, um.SetRowFilter("alice", "orders", json.RawMessage(`{"term":{"x":"1"}}`)))
	require.NoError(t, um.SetRowFilter("alice", "docs", json.RawMessage(`{"term":{"x":"2"}}`)))

	entries, err := um.ListRowFilters("alice")
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	tables := map[string]bool{}
	for _, e := range entries {
		tables[e.Table] = true
	}
	assert.True(t, tables["orders"])
	assert.True(t, tables["docs"])
}

func TestDeleteUser_RemovesRowFilters(t *testing.T) {
	um, _ := newTestUserManager(t)
	createTestUser(t, um, "alice", "password123")

	require.NoError(t, um.SetRowFilter("alice", "orders", json.RawMessage(`{"term":{"x":"1"}}`)))

	require.NoError(t, um.DeleteUser("alice"))

	// Recreate user and verify no leftover filters.
	createTestUser(t, um, "alice", "password456")
	filters, err := um.GetRowFilters("alice")
	require.NoError(t, err)
	assert.Nil(t, filters)
}
