package chunking

import (
	"math"
	"testing"

	"github.com/antflydb/antfly/pkg/termite/lib/audio"
)

// generateTestWAV creates a valid WAV file with a sine wave at the given
// sample rate, bit depth, and duration in milliseconds.
func generateTestWAV(t *testing.T, sampleRate, bitsPerSample, durationMs int) []byte {
	t.Helper()

	numSamples := sampleRate * durationMs / 1000
	samples := make([]float32, numSamples)

	// Generate a 440 Hz sine wave
	for i := range samples {
		samples[i] = float32(math.Sin(2.0 * math.Pi * 440.0 * float64(i) / float64(sampleRate)))
	}

	data, err := audio.EncodeWAV(samples, audio.Format{
		SampleRate:    sampleRate,
		BitsPerSample: bitsPerSample,
		NumChannels:   1,
	})
	if err != nil {
		t.Fatalf("encoding test WAV: %v", err)
	}
	return data
}
