package audio

import (
	"os"
	"testing"
)

func TestParseMP3_ValidFile(t *testing.T) {
	data, err := os.ReadFile("testdata/sine_440hz_3s.mp3")
	if err != nil {
		t.Fatalf("reading test fixture: %v", err)
	}

	samples, format, err := ParseMP3(data)
	if err != nil {
		t.Fatalf("ParseMP3 failed: %v", err)
	}

	if format.SampleRate != 44100 {
		t.Errorf("SampleRate = %d, want 44100", format.SampleRate)
	}
	if format.BitsPerSample != 16 {
		t.Errorf("BitsPerSample = %d, want 16", format.BitsPerSample)
	}
	if format.NumChannels != 1 {
		t.Errorf("NumChannels = %d, want 1", format.NumChannels)
	}

	// 3 seconds at 44100 Hz — MP3 encoding may add/remove a few frames,
	// so allow some tolerance.
	expectedSamples := 44100 * 3
	tolerance := 44100 / 2 // within 0.5 seconds
	if len(samples) < expectedSamples-tolerance || len(samples) > expectedSamples+tolerance {
		t.Errorf("got %d samples, expected ~%d (tolerance %d)", len(samples), expectedSamples, tolerance)
	}

	// Verify samples are in valid range [-1, 1]
	for i, s := range samples {
		if s < -1.0 || s > 1.0 {
			t.Errorf("sample[%d] = %f, out of range [-1, 1]", i, s)
			break
		}
	}
}

func TestParseMP3_InvalidData(t *testing.T) {
	garbage := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03}

	_, _, err := ParseMP3(garbage)
	if err == nil {
		t.Fatal("ParseMP3 with garbage data should return error")
	}
}

func TestParseMP3_EmptyData(t *testing.T) {
	_, _, err := ParseMP3([]byte{})
	if err == nil {
		t.Fatal("ParseMP3 with empty data should return error")
	}
}
