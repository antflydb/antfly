// Copyright 2026 Antfly, Inc.
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

package backends

import "testing"

func TestParsePublicBackendPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want BackendSpec
	}{
		{name: "native", in: "native", want: BackendSpec{Backend: BackendGo, Device: DeviceCPU}},
		{name: "onnx", in: "onnx", want: BackendSpec{Backend: BackendONNX, Device: DeviceAuto}},
		{name: "metal", in: "metal", want: BackendSpec{Backend: BackendCoreML, Device: DeviceCoreML}},
		{name: "cuda", in: "cuda", want: BackendSpec{Backend: BackendONNX, Device: DeviceCUDA}},
		{name: "pjrt", in: "pjrt", want: BackendSpec{Backend: BackendXLA, Device: DeviceAuto}},
		{name: "pjrt tpu", in: "pjrt:tpu", want: BackendSpec{Backend: BackendXLA, Device: DeviceTPU}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseBackendSpec(tt.in)
			if err != nil {
				t.Fatalf("ParseBackendSpec(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("ParseBackendSpec(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParsePublicBackendPriorityRejectsUnsupportedWebGPU(t *testing.T) {
	t.Parallel()
	if _, err := ParseBackendSpec("webgpu"); err == nil {
		t.Fatal("ParseBackendSpec(webgpu) unexpectedly succeeded")
	}
}
