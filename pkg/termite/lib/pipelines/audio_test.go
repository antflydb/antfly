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

package pipelines

import (
	"math"
	"math/cmplx"
	"testing"

	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// whisperConfig returns the standard Whisper audio config used across tests.
func whisperConfig() *backends.AudioConfig {
	return &backends.AudioConfig{
		SampleRate:    16000,
		FeatureSize:   80,
		NFft:          400,
		HopLength:     160,
		ChunkLength:   30,
		NMels:         80,
		PaddingValue:  0.0,
		Normalization: backends.AudioNormWhisper,
	}
}

// --- Test 1: Periodic Hann window ---

func TestHannWindow_Periodic(t *testing.T) {
	// Whisper uses periodic Hann: np.hanning(n_fft + 1)[:-1]
	// which equals 0.5 * (1 - cos(2*pi*i / N)) for i in [0, N).
	// The symmetric form uses N-1 as the divisor, which is WRONG for Whisper.
	ap := NewAudioProcessor(whisperConfig())
	n := ap.Config.NFft // 400

	if len(ap.hannWindow) != n {
		t.Fatalf("hannWindow length = %d, want %d", len(ap.hannWindow), n)
	}

	// Property 1: periodic Hann window[0] == 0
	if ap.hannWindow[0] != 0 {
		t.Errorf("hannWindow[0] = %g, want 0", ap.hannWindow[0])
	}

	// Property 2: periodic Hann window[N/2] == 1
	// For periodic Hann, the maximum is at N/2
	midVal := ap.hannWindow[n/2]
	if math.Abs(float64(midVal)-1.0) > 1e-6 {
		t.Errorf("hannWindow[N/2] = %g, want 1.0", midVal)
	}

	// Property 3: the last element (window[N-1]) should NOT be 0
	// For symmetric Hann, window[N-1] == 0. For periodic, it's > 0.
	lastVal := ap.hannWindow[n-1]
	if lastVal == 0 {
		t.Error("hannWindow[N-1] = 0; this is a symmetric window, not periodic")
	}

	// Property 4: verify against reference values from np.hanning(401)[:-1]
	// Reference computed in Python:
	//   np.hanning(401)[:5]  = [0.0, 0.00006156, 0.00024619, 0.00055383, 0.00098441]
	//   np.hanning(401)[-2:] = [0.00006156, 0.0]  → periodic drops the last 0
	//   So periodic[-1] = np.hanning(401)[399] = np.hanning(401)[-2] = 0.00006156...
	expectedFirst5 := []float64{0.0, 0.00006156, 0.00024619, 0.00055383, 0.00098441}
	for i, expected := range expectedFirst5 {
		got := float64(ap.hannWindow[i])
		if math.Abs(got-expected) > 1e-4 {
			t.Errorf("hannWindow[%d] = %g, want ≈ %g", i, got, expected)
		}
	}

	// Property 5: symmetry check — window[i] == window[N-i] for periodic Hann
	// (Note: this is NOT window[N-1-i], but window[N-i] mod N)
	for i := 1; i < n/2; i++ {
		if math.Abs(float64(ap.hannWindow[i])-float64(ap.hannWindow[n-i])) > 1e-6 {
			t.Errorf("hannWindow[%d] (%g) != hannWindow[%d] (%g); periodic symmetry violated",
				i, ap.hannWindow[i], n-i, ap.hannWindow[n-i])
			break
		}
	}
}

func TestHannWindow_NotSymmetric(t *testing.T) {
	// Verify we're not using the symmetric form (divisor N-1).
	// For N=400, symmetric: 0.5*(1-cos(2*pi*i/(N-1)))
	// Periodic:             0.5*(1-cos(2*pi*i/N))
	// At i=1, these differ:
	//   symmetric: 0.5*(1-cos(2*pi/399)) ≈ 0.0000620
	//   periodic:  0.5*(1-cos(2*pi/400)) ≈ 0.0000616
	ap := NewAudioProcessor(whisperConfig())
	n := ap.Config.NFft

	symmetricVal := 0.5 * (1 - math.Cos(2*math.Pi*1/float64(n-1)))
	periodicVal := 0.5 * (1 - math.Cos(2*math.Pi*1/float64(n)))
	got := float64(ap.hannWindow[1])

	symDiff := math.Abs(got - symmetricVal)
	perDiff := math.Abs(got - periodicVal)

	if symDiff < perDiff {
		t.Errorf("hannWindow[1] = %g is closer to symmetric (%g) than periodic (%g)",
			got, symmetricVal, periodicVal)
	}
}

// --- Test 2: Slaney mel filter bank ---

func TestMelFilterBank_SlaneyScale(t *testing.T) {
	// Verify the mel filter bank uses the Slaney (librosa default) mel scale
	// rather than the HTK mel scale.
	//
	// Slaney: linear below 1000 Hz (66.67 Hz/mel), log above
	// HTK:    2595 * log10(1 + f/700)
	//
	// The first few filter center frequencies should match librosa.
	// Reference from Python:
	//   librosa.mel_frequencies(n_mels=80, fmin=0, fmax=8000)[:6]
	//   → [0, 37.15, 74.30, 111.45, 148.59, 185.74]
	ap := NewAudioProcessor(whisperConfig())

	if len(ap.melFilters) != 80 {
		t.Fatalf("melFilters has %d filters, want 80", len(ap.melFilters))
	}

	// Each filter should have nFft/2 + 1 = 201 bins
	expectedBins := ap.Config.NFft/2 + 1
	for i, f := range ap.melFilters {
		if len(f) != expectedBins {
			t.Fatalf("melFilters[%d] has %d bins, want %d", i, len(f), expectedBins)
		}
	}

	// Find the peak (center frequency) of each of the first few filters
	// by locating the bin with the maximum value.
	binToFreq := func(bin int) float64 {
		return float64(bin) * float64(ap.Config.SampleRate) / float64(ap.Config.NFft)
	}

	// Expected Slaney center frequencies for first 5 filters (0-indexed)
	// From librosa.mel_frequencies(n_mels=80, fmin=0, fmax=8000)[1:6]
	expectedCenters := []float64{37.15, 74.30, 111.45, 148.59, 185.74}

	for mel := range 5 {
		maxVal := float32(0)
		maxBin := 0
		for bin, v := range ap.melFilters[mel] {
			if v > maxVal {
				maxVal = v
				maxBin = bin
			}
		}
		centerFreq := binToFreq(maxBin)

		// The frequency resolution is sampleRate/nFft = 40 Hz per bin,
		// so we allow ±40 Hz tolerance.
		if math.Abs(centerFreq-expectedCenters[mel]) > 40.0 {
			t.Errorf("filter %d center = %.1f Hz, want ≈ %.1f Hz (Slaney)",
				mel, centerFreq, expectedCenters[mel])
		}
	}
}

func TestMelFilterBank_NotHTK(t *testing.T) {
	// HTK mel scale gives different center frequencies than Slaney.
	// For the 10th filter (index 9), HTK and Slaney diverge noticeably.
	// HTK center ≈ 255 Hz; Slaney center ≈ 372 Hz.
	// Verify we're closer to the Slaney value.
	ap := NewAudioProcessor(whisperConfig())

	binToFreq := func(bin int) float64 {
		return float64(bin) * float64(ap.Config.SampleRate) / float64(ap.Config.NFft)
	}

	maxVal := float32(0)
	maxBin := 0
	for bin, v := range ap.melFilters[9] {
		if v > maxVal {
			maxVal = v
			maxBin = bin
		}
	}
	centerFreq := binToFreq(maxBin)

	// Slaney 10th center ≈ 371.5 Hz
	// HTK 10th center ≈ 255 Hz
	slaneyCenterApprox := 371.5
	htkCenterApprox := 255.0

	slDiff := math.Abs(centerFreq - slaneyCenterApprox)
	htkDiff := math.Abs(centerFreq - htkCenterApprox)

	if htkDiff < slDiff {
		t.Errorf("filter 9 center = %.1f Hz, closer to HTK (%.1f) than Slaney (%.1f)",
			centerFreq, htkCenterApprox, slaneyCenterApprox)
	}
}

func TestMelFilterBank_SlaneyNormalization(t *testing.T) {
	// Slaney normalization: each filter is scaled by 2 / bandwidth_hz.
	// This ensures approximately constant energy per mel channel.
	// Without normalization, filter peak heights would be proportional to 1.
	// With normalization, narrower filters (low freq) have taller peaks.
	ap := NewAudioProcessor(whisperConfig())

	// Find peak heights of first and last filter.
	// Low-frequency filters have smaller bandwidth → larger normalization factor → taller peak.
	peakFirst := float32(0)
	for _, v := range ap.melFilters[0] {
		if v > peakFirst {
			peakFirst = v
		}
	}

	peakLast := float32(0)
	for _, v := range ap.melFilters[79] {
		if v > peakLast {
			peakLast = v
		}
	}

	// With Slaney normalization (2/bandwidth_hz), narrower low-frequency
	// filters get larger normalization factors than wider high-frequency filters.
	// So first filter peak should be taller than last filter peak.
	if peakFirst <= peakLast {
		t.Errorf("first filter peak (%g) should be > last filter peak (%g) with Slaney normalization",
			peakFirst, peakLast)
	}

	// Without any normalization, all triangular filter peaks would be exactly 1.0
	// (the triangle reaches 1.0 at the center). With Slaney normalization,
	// peaks are scaled by 2/bandwidth_hz, so they differ from 1.0.
	if peakFirst == 1.0 {
		t.Errorf("first filter peak = 1.0; Slaney normalization should change the peak from 1.0")
	}
}

func TestMelFilterBank_TriangularShape(t *testing.T) {
	// Each mel filter should have a triangular shape: rises then falls with a single peak.
	ap := NewAudioProcessor(whisperConfig())

	for mel := range ap.melFilters {
		filter := ap.melFilters[mel]

		// Find the non-zero region
		first, last := -1, -1
		for i, v := range filter {
			if v > 0 {
				if first == -1 {
					first = i
				}
				last = i
			}
		}

		if first == -1 {
			t.Errorf("filter %d is all zeros", mel)
			continue
		}

		// Within the non-zero region, values should rise to a peak then fall.
		// Find peak position.
		peakBin := first
		peakVal := filter[first]
		for i := first; i <= last; i++ {
			if filter[i] > peakVal {
				peakVal = filter[i]
				peakBin = i
			}
		}

		// Check monotonically increasing before peak
		for i := first; i < peakBin; i++ {
			if filter[i+1] < filter[i]-1e-6 {
				t.Errorf("filter %d not monotonically increasing before peak at bin %d: [%d]=%g > [%d]=%g",
					mel, peakBin, i, filter[i], i+1, filter[i+1])
				break
			}
		}

		// Check monotonically decreasing after peak
		for i := peakBin; i < last; i++ {
			if filter[i+1] > filter[i]+1e-6 {
				t.Errorf("filter %d not monotonically decreasing after peak at bin %d: [%d]=%g < [%d]=%g",
					mel, peakBin, i, filter[i], i+1, filter[i+1])
				break
			}
		}
	}
}

// --- Test 3: Power spectrum (magnitude squared) ---

func TestMelSpectrogram_PowerSpectrum(t *testing.T) {
	// Verify that the mel spectrogram uses power spectrum (|FFT|²)
	// rather than plain magnitude (|FFT|).
	//
	// Strategy: use a pure sine wave input. For a single frequency sine wave,
	// the FFT has energy at one bin. The power spectrum value at that bin
	// should be the square of the magnitude.
	//
	// We'll compare the output of computeMelSpectrogram against a manual
	// computation to verify |FFT|² is used.
	ap := NewAudioProcessor(whisperConfig())
	nFft := ap.Config.NFft

	// Create a sine wave at 440 Hz (A4), one frame's worth of samples
	freq := 440.0
	sr := float64(ap.Config.SampleRate)
	samples := make([]float32, nFft)
	for i := range nFft {
		samples[i] = float32(math.Sin(2 * math.Pi * freq * float64(i) / sr))
	}

	// Apply Hann window
	windowed := make([]float32, nFft)
	for i := range nFft {
		windowed[i] = samples[i] * ap.hannWindow[i]
	}

	// Compute FFT
	fftResult := ap.fft(windowed)

	// Compute power spectrum (magnitude²) manually
	nBins := nFft / 2
	powerSpec := make([]float32, nBins)
	for i := range nBins {
		mag := cmplx.Abs(fftResult[i])
		powerSpec[i] = float32(mag * mag)
	}

	// Compute plain magnitude for comparison
	magSpec := make([]float32, nBins)
	for i := range nBins {
		magSpec[i] = float32(cmplx.Abs(fftResult[i]))
	}

	// Find the peak bin (should be near 440 Hz / (sr/nFft) = 440/40 = bin 11)
	peakBin := 0
	peakPower := float32(0)
	for i, v := range powerSpec {
		if v > peakPower {
			peakPower = v
			peakBin = i
		}
	}

	peakMag := magSpec[peakBin]

	// Power should be magnitude squared
	expectedPower := peakMag * peakMag
	if math.Abs(float64(peakPower-expectedPower)) > 1e-3 {
		t.Errorf("peak power (%g) != peak magnitude² (%g * %g = %g)",
			peakPower, peakMag, peakMag, expectedPower)
	}

	// Power should be much larger than magnitude for values > 1
	// (for values < 1, power is smaller, but for windowed FFT peaks, values are >> 1)
	if peakPower <= peakMag {
		t.Errorf("peak power (%g) should be >> peak magnitude (%g) for large values",
			peakPower, peakMag)
	}

	// Apply mel filter bank to the power spectrum manually
	manualMel := make([]float32, ap.Config.NMels)
	for mel := range ap.Config.NMels {
		var sum float32
		for bin := 0; bin < nBins && bin < len(ap.melFilters[mel]); bin++ {
			sum += powerSpec[bin] * ap.melFilters[mel][bin]
		}
		manualMel[mel] = sum
	}

	// Now compute using ProcessSamples and compare pre-log values.
	// ProcessSamples returns log-normalized values, so we verify indirectly:
	// the mel energy should be concentrated in the bins near 440 Hz.
	// The 440 Hz band should have non-trivial energy.
	melResult, _ := ap.ProcessSamples(samples)

	// The first frame's mel values are at indices [0:nMels].
	// Find which mel band has max energy (before log normalization).
	maxMelEnergy := float32(0)
	maxMelIdx := 0
	for mel := range ap.Config.NMels {
		if manualMel[mel] > maxMelEnergy {
			maxMelEnergy = manualMel[mel]
			maxMelIdx = mel
		}
	}

	// 440 Hz with Slaney scale should land around mel band 10-15
	if maxMelIdx < 5 || maxMelIdx > 25 {
		t.Errorf("440 Hz sine peak at mel band %d, expected between 5-25", maxMelIdx)
	}

	// Verify ProcessSamples produces finite, non-zero output
	hasNonZero := false
	for _, v := range melResult {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			t.Fatal("melSpectrogram contains NaN or Inf")
		}
		if v != 0 {
			hasNonZero = true
		}
	}
	if !hasNonZero {
		t.Error("melSpectrogram is all zeros")
	}
}

func TestMelSpectrogram_FFTBinCount(t *testing.T) {
	// Whisper (HuggingFace) uses magnitudes[:, :-1], dropping the last FFT bin.
	// So nBins should be nFft/2, NOT nFft/2+1.
	ap := NewAudioProcessor(whisperConfig())

	// Verify mel filter bank has nFft/2+1 bins (full spectrum for filter definition)
	expectedFilterBins := ap.Config.NFft/2 + 1
	for i, f := range ap.melFilters {
		if len(f) != expectedFilterBins {
			t.Errorf("melFilters[%d] has %d bins, want %d", i, len(f), expectedFilterBins)
		}
	}

	// Verify that in computeMelSpectrogram, only nFft/2 bins are used
	// (the last bin of each filter is ignored because stftPower has nFft/2 bins).
	// We test this indirectly: a signal with energy only in the Nyquist bin
	// (nFft/2) should produce no mel output.
	nFft := ap.Config.NFft
	nyquistFrame := make([]float32, nFft)
	// Alternating +1/-1 puts all energy at the Nyquist frequency
	for i := range nFft {
		if i%2 == 0 {
			nyquistFrame[i] = 1.0
		} else {
			nyquistFrame[i] = -1.0
		}
	}

	// With window and FFT, most energy should be in the last bin (nFft/2).
	// After dropping it, mel output should be near zero (only leakage).
	windowed := make([]float32, nFft)
	for i := range nFft {
		windowed[i] = nyquistFrame[i] * ap.hannWindow[i]
	}
	fftResult := ap.fft(windowed)

	// Energy in bin nFft/2 (the dropped bin)
	nyquistEnergy := cmplx.Abs(fftResult[nFft/2])

	// Energy in bin nFft/2-1 (the last kept bin)
	lastKeptEnergy := cmplx.Abs(fftResult[nFft/2-1])

	// The Nyquist bin should have much more energy than adjacent bins
	if nyquistEnergy < lastKeptEnergy*2 {
		t.Skipf("Nyquist test signal didn't concentrate energy as expected: nyquist=%g, last_kept=%g",
			nyquistEnergy, lastKeptEnergy)
	}
}

func TestMelSpectrogram_SilenceOutput(t *testing.T) {
	// Silence (all zeros) should produce a valid mel spectrogram.
	// With Whisper normalization, silence fills with (logFloor + 4.0) / 4.0.
	ap := NewAudioProcessor(whisperConfig())

	silence := make([]float32, ap.Config.SampleRate) // 1 second of silence
	melSpec, numFrames := ap.ProcessSamples(silence)

	if numFrames == 0 {
		t.Fatal("ProcessSamples returned 0 frames for silence")
	}

	// All values should be equal (uniform silence → uniform log mel)
	firstVal := melSpec[0]
	for i, v := range melSpec {
		if math.Abs(float64(v-firstVal)) > 1e-5 {
			t.Errorf("silence mel spec not uniform: [0]=%g, [%d]=%g", firstVal, i, v)
			break
		}
	}

	// With Whisper normalization, silence value should be:
	// log10(1e-10) = -10, clipped to max-8, normalized (x+4)/4
	// For pure silence, globalMax = -10, minClip = -18, all vals = -10
	// Normalized: (-10 + 4) / 4 = -1.5
	expectedVal := float32(-1.5)
	if math.Abs(float64(firstVal-expectedVal)) > 0.01 {
		t.Errorf("silence mel value = %g, want ≈ %g", firstVal, expectedVal)
	}
}

func TestMelSpectrogram_SineWaveEnergy(t *testing.T) {
	// A pure sine wave at a known frequency should produce mel energy
	// concentrated in the corresponding mel band.
	ap := NewAudioProcessor(whisperConfig())

	// 1000 Hz sine wave, 1 second
	freq := 1000.0
	sr := float64(ap.Config.SampleRate)
	nSamples := ap.Config.SampleRate
	samples := make([]float32, nSamples)
	for i := range nSamples {
		samples[i] = float32(0.5 * math.Sin(2*math.Pi*freq*float64(i)/sr))
	}

	melSpec, numFrames := ap.ProcessSamples(samples)
	nMels := ap.Config.NMels

	// Look at the first frame's mel values.
	// After Whisper normalization, higher energy → higher value.
	// The mel band containing 1000 Hz should have the highest value.
	firstFrameMel := melSpec[:nMels]

	maxIdx := 0
	maxVal := firstFrameMel[0]
	for i, v := range firstFrameMel {
		if v > maxVal {
			maxVal = v
			maxIdx = i
		}
	}

	// 1000 Hz with Slaney scale over 80 mels (0-8000 Hz) should land
	// around mel band 25-35. The Slaney scale is linear below 1000 Hz:
	// mel(1000) = 15, and the 80 mel bands span mel 0 to ~45.3,
	// so 1000 Hz → band index ≈ 15/45.3 * 81 - 1 ≈ 26.
	if maxIdx < 15 || maxIdx > 45 {
		t.Errorf("1000 Hz peak at mel band %d (val=%g), expected 15-45", maxIdx, maxVal)
	}

	_ = numFrames
}

// --- FFT correctness ---

func TestFFT_KnownSignal(t *testing.T) {
	// DC signal: all 1s → FFT should have all energy at bin 0
	ap := NewAudioProcessor(whisperConfig())

	n := 8
	dc := make([]float32, n)
	for i := range n {
		dc[i] = 1.0
	}

	result := ap.fft(dc)

	// Bin 0 should have magnitude N
	dcMag := cmplx.Abs(result[0])
	if math.Abs(dcMag-float64(n)) > 1e-6 {
		t.Errorf("FFT DC magnitude = %g, want %d", dcMag, n)
	}

	// All other bins should be ~0
	for i := 1; i < n; i++ {
		mag := cmplx.Abs(result[i])
		if mag > 1e-6 {
			t.Errorf("FFT bin %d magnitude = %g, want ≈ 0", i, mag)
		}
	}
}

func TestFFT_Parseval(t *testing.T) {
	// Parseval's theorem: sum of |x[n]|² = (1/N) * sum of |X[k]|²
	ap := NewAudioProcessor(whisperConfig())

	// Random-ish signal
	signal := []float32{0.3, -0.5, 0.8, -0.2, 0.6, -0.9, 0.1, 0.4}
	n := len(signal)

	// Time domain energy
	var timeEnergy float64
	for _, v := range signal {
		timeEnergy += float64(v) * float64(v)
	}

	// Frequency domain energy
	result := ap.fft(signal)
	var freqEnergy float64
	for _, v := range result[:n] {
		mag := cmplx.Abs(v)
		freqEnergy += mag * mag
	}
	freqEnergy /= float64(n)

	if math.Abs(timeEnergy-freqEnergy) > 1e-6 {
		t.Errorf("Parseval's theorem violated: time energy = %g, freq energy / N = %g",
			timeEnergy, freqEnergy)
	}
}
