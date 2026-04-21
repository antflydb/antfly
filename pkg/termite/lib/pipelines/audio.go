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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/cmplx"

	"github.com/antflydb/antfly/pkg/termite/lib/audio"
	"github.com/antflydb/antfly/pkg/termite/lib/backends"
)

// AudioProcessor handles audio loading and preprocessing for speech models.
// It converts raw audio bytes to mel spectrogram features suitable for
// encoder-decoder speech models like Whisper.
type AudioProcessor struct {
	Config *backends.AudioConfig

	// Precomputed mel filter bank
	melFilters [][]float32

	// Precomputed Hann window for FFT
	hannWindow []float32
}

// NewAudioProcessor creates a new AudioProcessor with the given configuration.
func NewAudioProcessor(config *backends.AudioConfig) *AudioProcessor {
	if config == nil {
		config = backends.DefaultAudioConfig()
	}

	ap := &AudioProcessor{
		Config: config,
	}

	// Precompute mel filter bank and Hann window
	ap.melFilters = ap.computeMelFilterBank()
	ap.hannWindow = ap.computeHannWindow()

	return ap
}

// Process converts raw audio bytes (WAV format) to mel spectrogram features.
// Returns features in shape [1, time, n_mels] suitable for batch processing.
func (ap *AudioProcessor) Process(audioData []byte) ([]float32, int, error) {
	// Load WAV file
	samples, err := ap.loadWAV(audioData)
	if err != nil {
		return nil, 0, fmt.Errorf("loading audio: %w", err)
	}

	// Compute mel spectrogram
	melSpec, numFrames := ap.computeMelSpectrogram(samples)

	return melSpec, numFrames, nil
}

// ProcessSamples converts raw audio samples (float32, mono, at target sample rate)
// to mel spectrogram features.
func (ap *AudioProcessor) ProcessSamples(samples []float32) ([]float32, int) {
	return ap.computeMelSpectrogram(samples)
}

// loadWAV parses a WAV file and returns mono float32 samples at the target sample rate.
func (ap *AudioProcessor) loadWAV(data []byte) ([]float32, error) {
	reader := bytes.NewReader(data)

	// Read RIFF header
	var riffHeader [4]byte
	if _, err := io.ReadFull(reader, riffHeader[:]); err != nil {
		return nil, fmt.Errorf("reading RIFF header: %w", err)
	}
	if string(riffHeader[:]) != "RIFF" {
		return nil, fmt.Errorf("not a RIFF file")
	}

	// Skip file size
	var fileSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &fileSize); err != nil {
		return nil, fmt.Errorf("reading file size: %w", err)
	}

	// Read WAVE format
	var waveHeader [4]byte
	if _, err := io.ReadFull(reader, waveHeader[:]); err != nil {
		return nil, fmt.Errorf("reading WAVE header: %w", err)
	}
	if string(waveHeader[:]) != "WAVE" {
		return nil, fmt.Errorf("not a WAVE file")
	}

	// Parse chunks
	var audioFormat, numChannels uint16
	var sampleRate, byteRate uint32
	var blockAlign, bitsPerSample uint16
	var audioData []byte

	for {
		var chunkID [4]byte
		if _, err := io.ReadFull(reader, chunkID[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading chunk ID: %w", err)
		}

		var chunkSize uint32
		if err := binary.Read(reader, binary.LittleEndian, &chunkSize); err != nil {
			return nil, fmt.Errorf("reading chunk size: %w", err)
		}

		switch string(chunkID[:]) {
		case "fmt ":
			if err := binary.Read(reader, binary.LittleEndian, &audioFormat); err != nil {
				return nil, fmt.Errorf("reading audio format: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &numChannels); err != nil {
				return nil, fmt.Errorf("reading num channels: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &sampleRate); err != nil {
				return nil, fmt.Errorf("reading sample rate: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &byteRate); err != nil {
				return nil, fmt.Errorf("reading byte rate: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &blockAlign); err != nil {
				return nil, fmt.Errorf("reading block align: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &bitsPerSample); err != nil {
				return nil, fmt.Errorf("reading bits per sample: %w", err)
			}
			// Skip any extra format bytes
			remaining := int(chunkSize) - 16
			if remaining > 0 {
				_, _ = reader.Seek(int64(remaining), io.SeekCurrent)
			}

		case "data":
			audioData = make([]byte, chunkSize)
			if _, err := io.ReadFull(reader, audioData); err != nil {
				return nil, fmt.Errorf("reading audio data: %w", err)
			}

		default:
			// Skip unknown chunks
			_, _ = reader.Seek(int64(chunkSize), io.SeekCurrent)
		}
	}

	if audioData == nil {
		return nil, fmt.Errorf("no audio data found")
	}

	// Only support PCM format
	if audioFormat != 1 {
		return nil, fmt.Errorf("unsupported audio format %d (only PCM supported)", audioFormat)
	}

	// Convert to float32 samples
	samples, err := ap.bytesToSamples(audioData, int(bitsPerSample), int(numChannels))
	if err != nil {
		return nil, fmt.Errorf("converting to samples: %w", err)
	}

	// Resample if needed
	if int(sampleRate) != ap.Config.SampleRate {
		samples = ap.resample(samples, int(sampleRate), ap.Config.SampleRate)
	}

	return samples, nil
}

// bytesToSamples converts raw PCM bytes to float32 samples in range [-1, 1].
// Handles mono conversion if needed.
func (ap *AudioProcessor) bytesToSamples(data []byte, bitsPerSample, numChannels int) ([]float32, error) {
	bytesPerSample := bitsPerSample / 8
	numSamples := len(data) / (bytesPerSample * numChannels)
	samples := make([]float32, numSamples)

	reader := bytes.NewReader(data)

	for i := range numSamples {
		var sampleSum float64
		for range numChannels {
			var sample float64
			switch bitsPerSample {
			case 8:
				var s uint8
				_ = binary.Read(reader, binary.LittleEndian, &s)
				// 8-bit WAV is unsigned, center at 128
				sample = (float64(s) - 128) / 128.0
			case 16:
				var s int16
				_ = binary.Read(reader, binary.LittleEndian, &s)
				sample = float64(s) / 32768.0
			case 24:
				var buf [3]byte
				_, _ = reader.Read(buf[:])
				// Convert 24-bit to 32-bit signed
				s := int32(buf[0]) | int32(buf[1])<<8 | int32(buf[2])<<16
				if s&0x800000 != 0 {
					s |= -0x1000000 // Sign extend (equivalent to 0xFF000000 but fits int32)
				}
				sample = float64(s) / 8388608.0
			case 32:
				var s int32
				_ = binary.Read(reader, binary.LittleEndian, &s)
				sample = float64(s) / 2147483648.0
			default:
				return nil, fmt.Errorf("unsupported bits per sample: %d", bitsPerSample)
			}
			sampleSum += sample
		}
		// Average channels for mono output
		samples[i] = float32(sampleSum / float64(numChannels))
	}

	return samples, nil
}

// resample performs simple linear interpolation resampling.
func (ap *AudioProcessor) resample(samples []float32, fromRate, toRate int) []float32 {
	return audio.Resample(samples, fromRate, toRate)
}

// computeMelSpectrogram converts audio samples to a mel spectrogram.
// Returns the flattened spectrogram and number of time frames.
func (ap *AudioProcessor) computeMelSpectrogram(samples []float32) ([]float32, int) {
	nFft := ap.Config.NFft
	hopLength := ap.Config.HopLength
	nMels := ap.Config.NMels

	// Pad samples if needed for chunk length
	targetLen := ap.Config.ChunkLength * ap.Config.SampleRate
	if len(samples) < targetLen {
		padded := make([]float32, targetLen)
		copy(padded, samples)
		for i := len(samples); i < targetLen; i++ {
			padded[i] = ap.Config.PaddingValue
		}
		samples = padded
	} else if len(samples) > targetLen {
		samples = samples[:targetLen]
	}

	// Whisper uses center=True STFT, which pads by nFft//2 on each side
	// This gives exactly n_samples / hop_length frames
	padAmount := nFft / 2
	paddedSamples := make([]float32, len(samples)+2*padAmount)
	// Fill with padding value (zeros)
	for i := range padAmount {
		paddedSamples[i] = ap.Config.PaddingValue
	}
	copy(paddedSamples[padAmount:], samples)
	for i := padAmount + len(samples); i < len(paddedSamples); i++ {
		paddedSamples[i] = ap.Config.PaddingValue
	}
	samples = paddedSamples

	// Calculate number of frames: exactly n_samples / hop_length for Whisper
	// (original samples length before center padding)
	numFrames := max(targetLen/hopLength, 1)

	// Compute STFT power spectrum (magnitude squared, matching HuggingFace WhisperFeatureExtractor)
	nBins := nFft / 2 // Drop last bin to match HuggingFace's magnitudes[:, :-1]
	stftPower := make([][]float32, numFrames)

	for frame := range numFrames {
		start := frame * hopLength

		// Extract frame and apply window
		frameData := make([]float32, nFft)
		for i := 0; i < nFft && start+i < len(samples); i++ {
			frameData[i] = samples[start+i] * ap.hannWindow[i]
		}

		// Compute FFT
		fftResult := ap.fft(frameData)

		// Compute power spectrum (magnitude squared)
		stftPower[frame] = make([]float32, nBins)
		for i := range nBins {
			mag := cmplx.Abs(fftResult[i])
			stftPower[frame][i] = float32(mag * mag)
		}
	}

	// Apply mel filter bank to power spectrum
	melSpec := make([][]float32, numFrames)
	for frame := range numFrames {
		melSpec[frame] = make([]float32, nMels)
		for mel := range nMels {
			var sum float32
			for bin := 0; bin < nBins && bin < len(ap.melFilters[mel]); bin++ {
				sum += stftPower[frame][bin] * ap.melFilters[mel][bin]
			}
			melSpec[frame][mel] = sum
		}
	}

	// Convert to log scale (log mel spectrogram)
	const logFloor = 1e-10 // Minimum value before log to avoid log(0)

	// Check which normalization type to use
	useWhisperNorm := ap.Config.Normalization == backends.AudioNormWhisper ||
		ap.Config.Normalization == "" // Default to Whisper for backward compatibility

	if useWhisperNorm {
		// Whisper-specific normalization from HuggingFace WhisperFeatureExtractor:
		// 1. log10(max(1e-10, mel_spec))
		// 2. Clip minimum to (global_max - 8.0)
		// 3. Normalize: (x + 4.0) / 4.0

		// First pass: compute log10 and find global maximum
		var globalMax float32 = -1000.0
		for frame := range numFrames {
			for mel := range nMels {
				val := melSpec[frame][mel]
				if val < logFloor {
					val = logFloor
				}
				logVal := float32(math.Log10(float64(val)))
				melSpec[frame][mel] = logVal
				if logVal > globalMax {
					globalMax = logVal
				}
			}
		}

		// Second pass: clip to (globalMax - 8.0) and normalize
		minClip := globalMax - 8.0
		for frame := range numFrames {
			for mel := range nMels {
				logVal := melSpec[frame][mel]
				// Clip minimum
				if logVal < minClip {
					logVal = minClip
				}
				// Whisper normalization: (x + 4.0) / 4.0
				// Maps typical range [-4, 4] to [0, 2]
				melSpec[frame][mel] = (logVal + 4.0) / 4.0
			}
		}
	} else {
		// Simple log mel spectrogram for CLAP and other audio models
		// Use natural log without Whisper-specific normalization
		for frame := range numFrames {
			for mel := range nMels {
				val := melSpec[frame][mel]
				if val < logFloor {
					val = logFloor
				}
				melSpec[frame][mel] = float32(math.Log(float64(val)))
			}
		}
	}

	// Flatten to [frames * mels] for model input
	// Note: Whisper expects [batch, n_mels, time] but we'll transpose in the model
	// For now, return [time, n_mels] flattened
	result := make([]float32, numFrames*nMels)
	for frame := range numFrames {
		for mel := range nMels {
			result[frame*nMels+mel] = melSpec[frame][mel]
		}
	}

	return result, numFrames
}

// computeMelFilterBank creates triangular mel filter banks using the Slaney mel
// scale and Slaney normalization, matching librosa.filters.mel() defaults.
// This is required for Whisper compatibility.
func (ap *AudioProcessor) computeMelFilterBank() [][]float32 {
	nMels := ap.Config.NMels
	nFft := ap.Config.NFft
	sampleRate := ap.Config.SampleRate
	nBins := nFft/2 + 1

	// Slaney mel scale (librosa default, htk=False):
	// Linear below 1000 Hz, logarithmic above.
	fSp := 200.0 / 3.0          // linear spacing: 66.667 Hz per mel
	minLogHz := 1000.0          // transition frequency
	minLogMel := minLogHz / fSp // = 15.0
	logStep := math.Log(6.4) / 27.0

	freqToMel := func(f float64) float64 {
		if f >= minLogHz {
			return minLogMel + math.Log(f/minLogHz)/logStep
		}
		return f / fSp
	}
	melToFreq := func(m float64) float64 {
		if m >= minLogMel {
			return minLogHz * math.Exp(logStep*(m-minLogMel))
		}
		return fSp * m
	}

	// Create uniformly spaced mel points
	lowMel := freqToMel(0.0)
	highMel := freqToMel(float64(sampleRate) / 2.0)

	melPoints := make([]float64, nMels+2)
	for i := range nMels + 2 {
		melPoints[i] = lowMel + float64(i)*(highMel-lowMel)/float64(nMels+1)
	}

	// Convert mel points to Hz frequencies
	freqPoints := make([]float64, nMels+2)
	for i := range nMels + 2 {
		freqPoints[i] = melToFreq(melPoints[i])
	}

	// Convert frequencies to FFT bin indices (fractional)
	fftFreqs := make([]float64, nBins)
	for i := range nBins {
		fftFreqs[i] = float64(i) * float64(sampleRate) / float64(nFft)
	}

	// Create filter bank with triangular filters
	filters := make([][]float32, nMels)
	for mel := range nMels {
		filters[mel] = make([]float32, nBins)
		lower := freqPoints[mel]
		center := freqPoints[mel+1]
		upper := freqPoints[mel+2]

		for bin := range nBins {
			freq := fftFreqs[bin]
			if freq >= lower && freq < center && center != lower {
				filters[mel][bin] = float32((freq - lower) / (center - lower))
			} else if freq >= center && freq <= upper && upper != center {
				filters[mel][bin] = float32((upper - freq) / (upper - center))
			}
		}

		// Slaney normalization: divide by filter bandwidth in Hz
		// This ensures approximately constant energy per channel.
		bandwidth := freqPoints[mel+2] - freqPoints[mel]
		if bandwidth > 0 {
			norm := float32(2.0 / bandwidth)
			for bin := range nBins {
				filters[mel][bin] *= norm
			}
		}
	}

	return filters
}

// computeHannWindow creates a periodic Hann window of the given size.
// Uses periodic form (divide by N, not N-1) to match HuggingFace's
// np.hanning(n_fft + 1)[:-1] used by WhisperFeatureExtractor.
func (ap *AudioProcessor) computeHannWindow() []float32 {
	n := ap.Config.NFft
	window := make([]float32, n)
	for i := range n {
		window[i] = float32(0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(n))))
	}
	return window
}

// fft computes the Fast Fourier Transform of the input.
// Uses the Cooley-Tukey algorithm for power-of-2 sizes.
func (ap *AudioProcessor) fft(input []float32) []complex128 {
	n := len(input)

	// Pad to next power of 2 if needed
	nextPow2 := 1
	for nextPow2 < n {
		nextPow2 *= 2
	}

	// Convert to complex
	data := make([]complex128, nextPow2)
	for i := range n {
		data[i] = complex(float64(input[i]), 0)
	}

	// Bit-reversal permutation
	j := 0
	for i := 0; i < nextPow2-1; i++ {
		if i < j {
			data[i], data[j] = data[j], data[i]
		}
		k := nextPow2 / 2
		for k <= j {
			j -= k
			k /= 2
		}
		j += k
	}

	// Cooley-Tukey FFT
	for size := 2; size <= nextPow2; size *= 2 {
		halfSize := size / 2
		tableStep := nextPow2 / size
		for i := 0; i < nextPow2; i += size {
			for j := range halfSize {
				angle := -2 * math.Pi * float64(j*tableStep) / float64(nextPow2)
				w := complex(math.Cos(angle), math.Sin(angle))
				t := w * data[i+j+halfSize]
				data[i+j+halfSize] = data[i+j] - t
				data[i+j] = data[i+j] + t
			}
		}
	}

	return data
}

// ResolveAudioConfig returns the audio config from the model if available,
// otherwise returns the override config or defaults.
func ResolveAudioConfig(model backends.Model, override *backends.AudioConfig) *backends.AudioConfig {
	if override != nil {
		return override
	}

	// Check if model provides audio config
	type audioConfigProvider interface {
		AudioConfig() *backends.AudioConfig
	}
	if provider, ok := model.(audioConfigProvider); ok {
		if cfg := provider.AudioConfig(); cfg != nil {
			return cfg
		}
	}

	return backends.DefaultAudioConfig()
}
