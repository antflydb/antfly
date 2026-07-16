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

import (
	"errors"
	"testing"
)

type failingRuntimeProbeBackend struct{}

var errProviderInitializationFailed = errors.New("provider initialization failed")

func (failingRuntimeProbeBackend) Type() BackendType { return "runtime-probe-test" }
func (failingRuntimeProbeBackend) Name() string      { return "runtime probe test" }
func (failingRuntimeProbeBackend) Available() bool   { return true }
func (failingRuntimeProbeBackend) Priority() int     { return 0 }
func (failingRuntimeProbeBackend) Loader() ModelLoader {
	return nil
}
func (failingRuntimeProbeBackend) ProbeRuntime(DeviceType) error {
	return errProviderInitializationFailed
}

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

func TestParsePublicBackendPriorityRejectsDeviceSuffix(t *testing.T) {
	t.Parallel()

	if _, err := ParseBackendSpec("pjrt:tpu"); err == nil {
		t.Fatal("ParseBackendSpec(pjrt:tpu) unexpectedly succeeded")
	}
}

func TestValidateStrictBackendPriorityRunsProviderProbe(t *testing.T) {
	backend := failingRuntimeProbeBackend{}
	RegisterBackend(backend)

	err := ValidateStrictBackendPriority([]BackendSpec{{Backend: backend.Type(), Device: DeviceCUDA}})
	if err == nil {
		t.Fatal("ValidateStrictBackendPriority unexpectedly accepted a failed provider probe")
	}
	if !errors.Is(err, errProviderInitializationFailed) {
		t.Fatalf("ValidateStrictBackendPriority error = %v", err)
	}
}

func TestParsePublicBackendPriorityRejectsUnsupportedWebGPU(t *testing.T) {
	t.Parallel()
	if _, err := ParseBackendSpec("webgpu"); err == nil {
		t.Fatal("ParseBackendSpec(webgpu) unexpectedly succeeded")
	}
}
