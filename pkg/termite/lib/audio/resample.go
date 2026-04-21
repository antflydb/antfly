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

package audio

// Resample performs linear interpolation resampling from fromRate to toRate.
func Resample(samples []float32, fromRate, toRate int) []float32 {
	if fromRate == toRate {
		return samples
	}

	ratio := float64(fromRate) / float64(toRate)
	newLen := int(float64(len(samples)) / ratio)
	resampled := make([]float32, newLen)

	for i := range newLen {
		srcIdx := float64(i) * ratio
		srcIdxInt := int(srcIdx)
		frac := float32(srcIdx - float64(srcIdxInt))

		if srcIdxInt+1 < len(samples) {
			resampled[i] = samples[srcIdxInt]*(1-frac) + samples[srcIdxInt+1]*frac
		} else if srcIdxInt < len(samples) {
			resampled[i] = samples[srcIdxInt]
		}
	}

	return resampled
}
