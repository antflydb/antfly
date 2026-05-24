package audio

import (
	"math"
	"testing"
)

// generateSineWave creates a 440Hz sine wave at the given sample rate and duration.
func generateSineWave(sampleRate int, durationMs int) []float32 {
	numSamples := sampleRate * durationMs / 1000
	samples := make([]float32, numSamples)
	for i := range samples {
		samples[i] = float32(math.Sin(2.0 * math.Pi * 440.0 * float64(i) / float64(sampleRate)))
	}
	return samples
}

func TestWAVEncode_Roundtrip(t *testing.T) {
	sampleRate := 16000
	samples := generateSineWave(sampleRate, 1000) // 1 second

	format := Format{
		SampleRate:    sampleRate,
		BitsPerSample: 16,
		NumChannels:   1,
	}

	encoded, err := EncodeWAV(samples, format)
	if err != nil {
		t.Fatalf("EncodeWAV failed: %v", err)
	}

	decoded, parsedFormat, err := ParseWAV(encoded)
	if err != nil {
		t.Fatalf("ParseWAV failed: %v", err)
	}

	if parsedFormat.SampleRate != sampleRate {
		t.Errorf("SampleRate = %d, want %d", parsedFormat.SampleRate, sampleRate)
	}
	if parsedFormat.BitsPerSample != 16 {
		t.Errorf("BitsPerSample = %d, want 16", parsedFormat.BitsPerSample)
	}
	if parsedFormat.NumChannels != 1 {
		t.Errorf("NumChannels = %d, want 1", parsedFormat.NumChannels)
	}

	if len(decoded) != len(samples) {
		t.Fatalf("decoded sample count = %d, want %d", len(decoded), len(samples))
	}

	// 16-bit quantization introduces rounding error; tolerance of 0.01 is reasonable.
	const tolerance = 0.01
	for i := range samples {
		diff := float64(samples[i]) - float64(decoded[i])
		if math.Abs(diff) > tolerance {
			t.Errorf("sample[%d] differs: original=%f, decoded=%f, diff=%f",
				i, samples[i], decoded[i], diff)
			break
		}
	}
}

func TestWAVEncode_16bit(t *testing.T) {
	samples := generateSineWave(16000, 100) // 100ms

	format := Format{
		SampleRate:    16000,
		BitsPerSample: 16,
		NumChannels:   1,
	}

	encoded, err := EncodeWAV(samples, format)
	if err != nil {
		t.Fatalf("EncodeWAV failed: %v", err)
	}

	// Verify RIFF header: first 4 bytes
	if len(encoded) < 12 {
		t.Fatalf("encoded WAV too short: %d bytes", len(encoded))
	}
	if string(encoded[0:4]) != "RIFF" {
		t.Errorf("bytes [0:4] = %q, want %q", string(encoded[0:4]), "RIFF")
	}
	// Bytes 8-12 should be "WAVE"
	if string(encoded[8:12]) != "WAVE" {
		t.Errorf("bytes [8:12] = %q, want %q", string(encoded[8:12]), "WAVE")
	}
}

func TestWAVEncode_EmptySamples(t *testing.T) {
	format := Format{
		SampleRate:    16000,
		BitsPerSample: 16,
		NumChannels:   1,
	}

	_, err := EncodeWAV([]float32{}, format)
	if err == nil {
		t.Fatal("EncodeWAV with empty samples should return error")
	}
}

func TestParseWAV_InvalidData(t *testing.T) {
	garbage := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03}

	_, _, err := ParseWAV(garbage)
	if err == nil {
		t.Fatal("ParseWAV with garbage data should return error")
	}
}

func TestParseWAV_ValidHeader(t *testing.T) {
	sampleRate := 44100
	bitsPerSample := 16
	numChannels := 1

	samples := generateSineWave(sampleRate, 50) // 50ms

	format := Format{
		SampleRate:    sampleRate,
		BitsPerSample: bitsPerSample,
		NumChannels:   numChannels,
	}

	encoded, err := EncodeWAV(samples, format)
	if err != nil {
		t.Fatalf("EncodeWAV failed: %v", err)
	}

	_, parsedFormat, err := ParseWAV(encoded)
	if err != nil {
		t.Fatalf("ParseWAV failed: %v", err)
	}

	if parsedFormat.SampleRate != sampleRate {
		t.Errorf("SampleRate = %d, want %d", parsedFormat.SampleRate, sampleRate)
	}
	if parsedFormat.BitsPerSample != bitsPerSample {
		t.Errorf("BitsPerSample = %d, want %d", parsedFormat.BitsPerSample, bitsPerSample)
	}
	if parsedFormat.NumChannels != numChannels {
		t.Errorf("NumChannels = %d, want %d", parsedFormat.NumChannels, numChannels)
	}
}

func TestWAVEncode_Roundtrip_8bit(t *testing.T) {
	sampleRate := 16000
	samples := generateSineWave(sampleRate, 100) // 100ms

	format := Format{
		SampleRate:    sampleRate,
		BitsPerSample: 8,
		NumChannels:   1,
	}

	encoded, err := EncodeWAV(samples, format)
	if err != nil {
		t.Fatalf("EncodeWAV (8-bit) failed: %v", err)
	}

	decoded, parsedFormat, err := ParseWAV(encoded)
	if err != nil {
		t.Fatalf("ParseWAV (8-bit) failed: %v", err)
	}

	if parsedFormat.BitsPerSample != 8 {
		t.Errorf("BitsPerSample = %d, want 8", parsedFormat.BitsPerSample)
	}

	if len(decoded) != len(samples) {
		t.Fatalf("decoded sample count = %d, want %d", len(decoded), len(samples))
	}

	// 8-bit has lower precision, so use a larger tolerance.
	const tolerance = 0.02
	for i := range samples {
		diff := math.Abs(float64(samples[i]) - float64(decoded[i]))
		if diff > tolerance {
			t.Errorf("sample[%d] differs: original=%f, decoded=%f, diff=%f",
				i, samples[i], decoded[i], diff)
			break
		}
	}
}

func TestWAVEncode_InvalidSampleRate(t *testing.T) {
	format := Format{
		SampleRate:    0,
		BitsPerSample: 16,
		NumChannels:   1,
	}

	_, err := EncodeWAV([]float32{0.5, -0.5}, format)
	if err == nil {
		t.Fatal("EncodeWAV with zero sample rate should return error")
	}
}

func TestWAVEncode_InvalidBitsPerSample(t *testing.T) {
	format := Format{
		SampleRate:    16000,
		BitsPerSample: 12,
		NumChannels:   1,
	}

	_, err := EncodeWAV([]float32{0.5, -0.5}, format)
	if err == nil {
		t.Fatal("EncodeWAV with unsupported bits per sample should return error")
	}
}
