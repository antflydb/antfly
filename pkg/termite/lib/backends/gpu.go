// Copyright 2025 Antfly, Inc.
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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var (
	// gpuAvailable caches the GPU detection result
	gpuAvailable     bool
	gpuAvailableOnce sync.Once
	gpuInfo          GPUInfo
)

// DetectGPU checks if GPU acceleration is available.
// Results are cached after the first call.
func DetectGPU() GPUInfo {
	gpuAvailableOnce.Do(func() {
		gpuInfo = detectGPUImpl()
		gpuAvailable = gpuInfo.Available
	})
	return gpuInfo
}

// IsGPUAvailable returns true if GPU acceleration is available.
func IsGPUAvailable() bool {
	DetectGPU()
	return gpuAvailable
}

// detectGPUImpl performs actual GPU detection based on platform.
func detectGPUImpl() GPUInfo {
	// Check for TPU first (GKE TPU nodes)
	if tpuInfo := detectTPU(); tpuInfo.Available {
		return tpuInfo
	}

	switch runtime.GOOS {
	case "darwin":
		// macOS always has CoreML available (Apple Silicon or Intel with ANE)
		return GPUInfo{
			Available: true,
			Type:      "coreml",
		}
	case "linux", "windows":
		return detectCUDA()
	default:
		return GPUInfo{Available: false, Type: "none"}
	}
}

// detectTPU checks for Google Cloud TPU availability.
// TPUs are available on GKE TPU node pools and Cloud TPU VMs.
func detectTPU() GPUInfo {
	info := GPUInfo{Type: "none"}

	// Method 1: Check GOMLX_BACKEND for xla:tpu
	if backend := os.Getenv("GOMLX_BACKEND"); strings.Contains(strings.ToLower(backend), "tpu") {
		info.Available = true
		info.Type = "tpu"
		info.DeviceName = "TPU (via GOMLX_BACKEND)"
		return info
	}

	// Method 2: Check for libtpu.so (available on GKE TPU nodes)
	if tpuLibsExist() {
		info.Available = true
		info.Type = "tpu"
		info.DeviceName = "TPU (libtpu.so detected)"
		return info
	}

	// Method 3: Check for TPU metadata endpoint (GKE TPU nodes)
	if isGKETPUNode() {
		info.Available = true
		info.Type = "tpu"
		info.DeviceName = "TPU (GKE TPU node)"
		return info
	}

	return info
}

// tpuLibsExist checks if TPU libraries are present.
func tpuLibsExist() bool {
	// Common TPU library paths
	tpuPaths := []string{
		"/usr/local/lib",
		"/usr/lib",
	}

	// Check PJRT_PLUGIN_LIBRARY_PATH
	if pjrtPath := os.Getenv("PJRT_PLUGIN_LIBRARY_PATH"); pjrtPath != "" {
		tpuPaths = append([]string{pjrtPath}, tpuPaths...)
	}

	// Also check LD_LIBRARY_PATH
	if ldPath := os.Getenv("LD_LIBRARY_PATH"); ldPath != "" {
		tpuPaths = append(strings.Split(ldPath, ":"), tpuPaths...)
	}

	// Look for libtpu.so or pjrt_plugin_tpu.so
	for _, dir := range tpuPaths {
		if matches, _ := filepath.Glob(filepath.Join(dir, "libtpu.so*")); len(matches) > 0 {
			return true
		}
		if matches, _ := filepath.Glob(filepath.Join(dir, "pjrt_plugin_tpu.so*")); len(matches) > 0 {
			return true
		}
	}

	return false
}

// isGKETPUNode checks if running on a GKE TPU node by looking for TPU-specific files.
func isGKETPUNode() bool {
	// GKE TPU nodes have /dev/accel* devices
	if matches, _ := filepath.Glob("/dev/accel*"); len(matches) > 0 {
		return true
	}

	// Check for TPU chip type file
	if _, err := os.Stat("/sys/class/tpu"); err == nil {
		return true
	}

	return false
}

// IsTPUAvailable returns true if TPU acceleration is available.
func IsTPUAvailable() bool {
	info := DetectGPU()
	return info.Type == "tpu"
}

// detectCUDA checks for NVIDIA CUDA availability.
func detectCUDA() GPUInfo {
	info := GPUInfo{Type: "none"}

	// Method 1: Try nvidia-smi command
	if nvidiaInfo := tryNvidiaSMI(); nvidiaInfo.Available {
		return nvidiaInfo
	}

	// Method 2: Check for CUDA libraries
	if cudaLibsExist() {
		info.Available = true
		info.Type = "cuda"
		info.DeviceName = "CUDA (libraries detected)"
		return info
	}

	return info
}

// tryNvidiaSMI attempts to run nvidia-smi to detect GPU.
func tryNvidiaSMI() GPUInfo {
	info := GPUInfo{Type: "none"}

	// Try to find nvidia-smi
	nvidiaSMI, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return info
	}

	// Run nvidia-smi to get GPU info
	cmd := exec.Command(nvidiaSMI, "--query-gpu=name,driver_version", "--format=csv,noheader,nounits") //nolint:gosec // G204: nvidiaSMI path comes from LookPath("nvidia-smi")
	output, err := cmd.Output()
	if err != nil {
		return info
	}

	// Parse output (format: "GPU Name, Driver Version")
	parts := strings.Split(strings.TrimSpace(string(output)), ", ")
	info.Available = true
	info.Type = "cuda"
	if len(parts) >= 1 {
		info.DeviceName = strings.TrimSpace(parts[0])
	}
	if len(parts) >= 2 {
		info.DriverVer = strings.TrimSpace(parts[1])
	}

	// Try to get CUDA version
	cmd = exec.Command(nvidiaSMI, "--query-gpu=compute_cap", "--format=csv,noheader,nounits") //nolint:gosec // G204: nvidiaSMI path comes from LookPath("nvidia-smi")
	if output, err := cmd.Output(); err == nil {
		info.CUDAVersion = strings.TrimSpace(string(output))
	}

	return info
}

// cudaLibsExist checks if CUDA libraries are present.
func cudaLibsExist() bool {
	// Common CUDA library paths
	cudaPaths := []string{
		"/usr/local/cuda/lib64",
		"/usr/lib/x86_64-linux-gnu",
		"/usr/lib64",
	}

	// Also check LD_LIBRARY_PATH
	if ldPath := os.Getenv("LD_LIBRARY_PATH"); ldPath != "" {
		cudaPaths = append(strings.Split(ldPath, ":"), cudaPaths...)
	}

	// Look for libcudart (CUDA runtime)
	for _, dir := range cudaPaths {
		matches, _ := filepath.Glob(filepath.Join(dir, "libcudart.so*"))
		if len(matches) > 0 {
			return true
		}
	}

	return false
}

// ShouldUseGPU determines if GPU should be used based on mode and availability.
func ShouldUseGPU(mode GPUMode) bool {
	switch mode {
	case GPUModeOff:
		return false
	case GPUModeTpu, GPUModeCuda, GPUModeCoreML:
		return true // Force specific accelerator, will fail at runtime if unavailable
	case GPUModeAuto, "":
		return IsGPUAvailable()
	default:
		return IsGPUAvailable()
	}
}
