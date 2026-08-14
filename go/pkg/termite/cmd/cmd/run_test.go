// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveInferenceAdmissionValues(t *testing.T) {
	tests := []struct {
		name              string
		canonical         int
		canonicalExplicit bool
		legacy            int
		legacyExplicit    bool
		want              int
		wantErr           bool
	}{
		{name: "default", canonical: 32, want: 32},
		{name: "canonical", canonical: 8, canonicalExplicit: true, want: 8},
		{name: "legacy including zero", canonical: 32, legacy: 0, legacyExplicit: true, want: 0},
		{name: "matching dual config", canonical: 8, canonicalExplicit: true, legacy: 8, legacyExplicit: true, want: 8},
		{name: "conflicting dual config", canonical: 8, canonicalExplicit: true, legacy: 7, legacyExplicit: true, wantErr: true},
		{name: "negative canonical", canonical: -1, canonicalExplicit: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveInferenceAdmissionValues(tt.canonical, tt.canonicalExplicit, tt.legacy, tt.legacyExplicit)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
