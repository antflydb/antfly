// Copyright 2015 The etcd Authors
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

package multirafthttp

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/Masterminds/semver"
	"go.etcd.io/raft/v3/raftpb"
)

func TestEntry(t *testing.T) {
	tests := []raftpb.Entry{
		{},
		{Term: 1, Index: 1},
		{Term: 1, Index: 1, Data: []byte("some data")},
	}
	for i, tt := range tests {
		b := &bytes.Buffer{}
		if err := writeEntryTo(b, &tt); err != nil {
			t.Errorf("#%d: unexpected write ents error: %v", i, err)
			continue
		}
		var ent raftpb.Entry
		if err := readEntryFrom(b, &ent); err != nil {
			t.Errorf("#%d: unexpected read ents error: %v", i, err)
			continue
		}
		if !reflect.DeepEqual(ent, tt) {
			t.Errorf("#%d: ent = %+v, want %+v", i, ent, tt)
		}
	}
}

func TestCompareMajorMinorVersion(t *testing.T) {
	tests := []struct {
		va, vb *semver.Version
		w      int
	}{
		// equal to
		{
			semver.MustParse("2.1.0"),
			semver.MustParse("2.1.0"),
			0,
		},
		// smaller than
		{
			semver.MustParse("2.0.0"),
			semver.MustParse("2.1.0"),
			-1,
		},
		// bigger than
		{
			semver.MustParse("2.2.0"),
			semver.MustParse("2.1.0"),
			1,
		},
		// ignore patch
		{
			semver.MustParse("2.1.1"),
			semver.MustParse("2.1.0"),
			1,
		},
		// ignore prerelease
		{
			semver.MustParse("2.1.0-dev0"),
			semver.MustParse("2.1.0"),
			-1,
		},
	}
	for i, tt := range tests {
		if g := tt.va.Compare(tt.vb); g != tt.w {
			t.Errorf("#%d: compare = %d, want %d for %s compared with %s", i, g, tt.w, tt.va, tt.vb)
		}
	}
}

func TestServerVersion(t *testing.T) {
	tests := []struct {
		h  http.Header
		wv *semver.Version
	}{
		// backward compatibility with etcd 2.0
		{
			http.Header{},
			semver.MustParse("0.0.0"),
		},
		{
			http.Header{"X-Server-Version": []string{"2.1.0"}},
			semver.MustParse("2.1.0"),
		},
		{
			http.Header{"X-Server-Version": []string{"2.1.0-alpha.0+git"}},
			semver.MustParse("2.1.0-alpha.0+git"),
		},
	}
	for i, tt := range tests {
		v := serverVersion(tt.h)
		if v.String() != tt.wv.String() {
			t.Errorf("#%d: version = %s, want %s", i, v, tt.wv)
		}
	}
}

func TestMinClusterVersion(t *testing.T) {
	tests := []struct {
		h  http.Header
		wv *semver.Version
	}{
		{
			// No version defaults to "0.0.0-dev0"
			http.Header{},
			semver.MustParse("0.0.0-dev0"),
		},
		{
			http.Header{"X-Min-Cluster-Version": []string{"2.1.0"}},
			semver.MustParse("2.1.0"),
		},
		{
			http.Header{"X-Min-Cluster-Version": []string{"2.1.0-alpha.0+git"}},
			semver.MustParse("2.1.0-alpha.0+git"),
		},
	}
	for i, tt := range tests {
		v := minClusterVersion(tt.h)
		if v.String() != tt.wv.String() {
			t.Errorf("#%d: version = %s, want %s", i, v, tt.wv)
		}
	}
}

func TestCheckVersionCompatibility(t *testing.T) {
	ls := semver.MustParse(Version)

	oneVersionHigher := semver.MustParse(ls.Original()).
		IncMajor()

	tooHigh := semver.MustParse(ls.Original()).
		IncMajor().
		IncMinor()

	lmc := semver.MustParse(MinClusterVersion)
	tests := []struct {
		server     *semver.Version
		minCluster *semver.Version
		wok        bool
	}{
		// the same version as local
		{
			ls,
			lmc,
			true,
		},
		// one version lower
		{
			lmc,
			semver.MustParse("0.0.0-dev0"),
			true,
		},
		// one version higher
		{
			&oneVersionHigher,
			ls,
			true,
		},
		// TODO (ajr): Re-enable when relevant
		// too low version
		// {
		// 	&semver.Version{Major: lmc.Major - 1},
		// 	&semver.Version{},
		// 	false,
		// },
		// too high version
		{
			&tooHigh,
			&oneVersionHigher,
			false,
		},
	}
	for i, tt := range tests {
		_, _, err := checkVersionCompatibility("", tt.server, tt.minCluster)
		if ok := err == nil; ok != tt.wok {
			t.Errorf("#%d: ok = %v, want %v, err = %v", i, ok, tt.wok, err)
		}
	}
}

func writeEntryTo(w io.Writer, ent *raftpb.Entry) error {
	size := ent.Size()
	if err := binary.Write(w, binary.BigEndian, uint64(size)); err != nil {
		return err
	}
	b, err := ent.Marshal()
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func readEntryFrom(r io.Reader, ent *raftpb.Entry) error {
	var l uint64
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return err
	}
	buf := make([]byte, int(l))
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return ent.Unmarshal(buf)
}
