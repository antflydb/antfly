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

package kvadapter

import (
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/casbin/casbin/v3"
	fileadapter "github.com/casbin/casbin/v3/persist/file-adapter"
	"github.com/casbin/casbin/v3/util"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const testDB = "test.db"

type AdapterTestSuite struct {
	suite.Suite

	db       *kv.PebbleDB
	enforcer casbin.IEnforcer
}

func testGetPolicy(t *testing.T, e casbin.IEnforcer, wanted [][]string) {
	t.Helper()
	got, err := e.GetPolicy()
	require.NoError(t, err)
	if !util.Array2DEquals(wanted, got) {
		t.Error("got policy: ", got, ", wanted policy: ", wanted)
	}
}

func TestAdapterFromFileExample(t *testing.T) {
	r := require.New(t)
	db, err := pebble.Open(testDB, pebbleutils.NewMemPebbleOpts())
	r.NoError(err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	pa, err := NewAdapter(&kv.PebbleDB{DB: db}, "casbin::")
	r.NoError(err)
	pebbleEnforcer, err := casbin.NewEnforcer("examples/rbac_model.conf", pa)
	r.NoError(err)

	fa := fileadapter.NewAdapter("examples/rbac_policy.csv")
	fileEnforcer, err := casbin.NewEnforcer("examples/rbac_model.conf", fa)
	r.NoError(err)

	pol, err := fileEnforcer.GetPolicy()
	r.NoError(err)
	ok, err := pebbleEnforcer.AddPolicies(pol)
	r.NoError(err)
	assert.True(t, ok)

	pol, err = fileEnforcer.GetPolicy()
	r.NoError(err)

	testGetPolicy(t, pebbleEnforcer, pol)
}

func (suite *AdapterTestSuite) SetupTest() {
	t := suite.T()
	r := require.New(t)

	db, err := pebble.Open(testDB, pebbleutils.NewMemPebbleOpts())
	r.NoError(err)
	suite.db = &kv.PebbleDB{DB: db}

	a, err := NewAdapter(suite.db, "casbin::")
	r.NoError(err)

	enforcer, err := casbin.NewEnforcer("examples/rbac_model.conf", a)
	r.NoError(err)

	suite.enforcer = enforcer
}

func (suite *AdapterTestSuite) TearDownTest() {
	_ = os.Remove(testDB)
}

func Test_AdapterTest_Suite(t *testing.T) {
	suite.Run(t, new(AdapterTestSuite))
}

func (suite *AdapterTestSuite) Test_SavePolicy_ReturnsErr() {
	e := suite.enforcer
	t := suite.T()

	err := e.SavePolicy()
	assert.EqualError(t, err, "not supported: must use auto-save with this adapter")
}

func (suite *AdapterTestSuite) Test_AutoSavePolicy() {
	e := suite.enforcer
	t := suite.T()

	e.EnableAutoSave(true)

	e.AddPolicy("roger", "data1", "write")
	testGetPolicy(t, e, [][]string{{"roger", "data1", "write"}})

	e.RemovePolicy("roger", "data1", "write")
	testGetPolicy(t, e, [][]string{})

	e.AddPolicies([][]string{{"roger", "data1", "read"}, {"roger", "data1", "write"}})
	testGetPolicy(t, e, [][]string{{"roger", "data1", "read"}, {"roger", "data1", "write"}})

	_, err := e.RemoveFilteredPolicy(1, "data1")
	assert.NoError(t, err)
	testGetPolicy(t, e, [][]string{})

	e.AddPolicies([][]string{{"roger", "data1", "read"}, {"roger", "data1", "write"}})
	testGetPolicy(t, e, [][]string{{"roger", "data1", "read"}, {"roger", "data1", "write"}})

	e.RemovePolicies([][]string{{"roger", "data1", "read"}, {"roger", "data1", "write"}})
	testGetPolicy(t, e, [][]string{})
}

func (suite *AdapterTestSuite) Test_UpdatePolicy() {
	e := suite.enforcer
	t := suite.T()

	ok, err := e.AddPolicies(
		[][]string{
			{"alice", "data1", "read"},
			{"bob", "data2", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
		},
	)
	assert.NoError(t, err)
	assert.True(t, ok)

	testGetPolicy(
		t,
		e,
		[][]string{
			{"alice", "data1", "read"},
			{"bob", "data2", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
		},
	)

	ok, err = e.UpdatePolicy([]string{"alice", "data1", "read"}, []string{"alice", "data3", "read"})
	assert.NoError(t, err)
	assert.True(t, ok)

	testGetPolicy(
		t,
		e,
		[][]string{
			{"alice", "data3", "read"},
			{"bob", "data2", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
		},
	)
}

func (suite *AdapterTestSuite) Test_UpdatePolices() {
	e := suite.enforcer
	t := suite.T()

	ok, err := e.AddPolicies(
		[][]string{
			{"alice", "data1", "read"},
			{"bob", "data2", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
		},
	)
	assert.NoError(t, err)
	assert.True(t, ok)

	testGetPolicy(
		t,
		e,
		[][]string{
			{"alice", "data1", "read"},
			{"bob", "data2", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
		},
	)

	ok, err = e.UpdatePolicies(
		[][]string{{"alice", "data1", "read"}, {"bob", "data2", "write"}},
		[][]string{{"alice", "data3", "read"}, {"bob", "data3", "write"}},
	)
	assert.NoError(t, err)
	assert.True(t, ok)

	testGetPolicy(
		t,
		e,
		[][]string{
			{"alice", "data3", "read"},
			{"bob", "data3", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
		},
	)
}

func (suite *AdapterTestSuite) Test_UpdateFilteredPolicies() {
	e := suite.enforcer
	t := suite.T()

	ok, err := e.AddPolicies(
		[][]string{
			{"alice", "data1", "read"},
			{"bob", "data2", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
		},
	)
	assert.NoError(t, err)
	assert.True(t, ok)

	testGetPolicy(
		t,
		e,
		[][]string{
			{"alice", "data1", "read"},
			{"bob", "data2", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
		},
	)

	_, err = e.UpdateFilteredPolicies([][]string{{"alice", "data3", "read"}}, 0, "alice", "data1")
	assert.NoError(t, err)

	testGetPolicy(
		t,
		e,
		[][]string{
			{"bob", "data2", "write"},
			{"data2_admin", "data2", "read"},
			{"data2_admin", "data2", "write"},
			{"alice", "data3", "read"},
		},
	)
}
