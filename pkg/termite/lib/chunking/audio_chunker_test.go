package chunking

import (
	"context"
	"math"
	"os"
	"testing"

	"github.com/antflydb/antfly/pkg/libaf/chunking"
	"github.com/antflydb/antfly/pkg/termite/lib/audio"
)

func TestAudioChunker_BasicWindowing(t *testing.T) {
	wav := generateTestWAV(t, 16000, 16, 5000) // 5 seconds at 16kHz 16-bit

	ac := &AudioChunker{}
	chunks, err := ac.ChunkAudio(context.Background(), wav, chunking.ChunkOptions{
		Audio: chunking.AudioChunkOptions{WindowDurationMs: 2000},
	})
	if err != nil {
		t.Fatalf("ChunkAudio failed: %v", err)
	}

	// 5s / 2s window = 3 chunks (0-2s, 2-4s, 4-5s)
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(chunks))
	}

	for i, chunk := range chunks {
		if chunk.MimeType != "audio/wav" {
			t.Errorf("chunk[%d].MimeType = %q, want %q", i, chunk.MimeType, "audio/wav")
		}

		bc, err := chunk.AsBinaryContent()
		if err != nil {
			t.Fatalf("chunk[%d].AsBinaryContent() failed: %v", i, err)
		}

		if len(bc.Data) == 0 {
			t.Errorf("chunk[%d] has empty data", i)
		}

		// Verify timing
		expectedStartMs := float32(i * 2000)
		if math.Abs(float64(bc.StartTimeMs-expectedStartMs)) > 1.0 {
			t.Errorf("chunk[%d].StartTimeMs = %f, want %f", i, bc.StartTimeMs, expectedStartMs)
		}
	}

	// First chunk: 0-2s
	bc0, _ := chunks[0].AsBinaryContent()
	if math.Abs(float64(bc0.EndTimeMs-2000.0)) > 1.0 {
		t.Errorf("chunk[0].EndTimeMs = %f, want 2000", bc0.EndTimeMs)
	}

	// Last chunk: 4-5s
	bc2, _ := chunks[2].AsBinaryContent()
	if math.Abs(float64(bc2.EndTimeMs-5000.0)) > 1.0 {
		t.Errorf("chunk[2].EndTimeMs = %f, want 5000", bc2.EndTimeMs)
	}
}

func TestAudioChunker_Overlap(t *testing.T) {
	wav := generateTestWAV(t, 16000, 16, 4000) // 4 seconds

	ac := &AudioChunker{}
	chunks, err := ac.ChunkAudio(context.Background(), wav, chunking.ChunkOptions{
		Audio: chunking.AudioChunkOptions{WindowDurationMs: 2000, OverlapDurationMs: 1000},
	})
	if err != nil {
		t.Fatalf("ChunkAudio failed: %v", err)
	}

	// step = 2000 - 1000 = 1000ms = 16000 samples
	// totalSamples = 64000, offsets: 0, 16000, 32000, 48000 -> 4 chunks
	// [0-2s], [1-3s], [2-4s], [3-4s]
	if len(chunks) != 4 {
		t.Fatalf("got %d chunks, want 4", len(chunks))
	}

	// Verify overlapping: chunk[0] ends at 2s, chunk[1] starts at 1s
	bc0, _ := chunks[0].AsBinaryContent()
	bc1, _ := chunks[1].AsBinaryContent()

	if bc0.EndTimeMs <= bc1.StartTimeMs {
		t.Errorf("expected overlap: chunk[0] ends at %f, chunk[1] starts at %f",
			bc0.EndTimeMs, bc1.StartTimeMs)
	}
}

func TestAudioChunker_ShortAudio(t *testing.T) {
	wav := generateTestWAV(t, 16000, 16, 500) // 500ms

	ac := &AudioChunker{}
	chunks, err := ac.ChunkAudio(context.Background(), wav, chunking.ChunkOptions{
		Audio: chunking.AudioChunkOptions{WindowDurationMs: 2000},
	})
	if err != nil {
		t.Fatalf("ChunkAudio failed: %v", err)
	}

	if len(chunks) != 1 {
		t.Fatalf("got %d chunks, want 1 for short audio", len(chunks))
	}

	bc, err := chunks[0].AsBinaryContent()
	if err != nil {
		t.Fatalf("AsBinaryContent failed: %v", err)
	}

	if math.Abs(float64(bc.StartTimeMs)) > 1.0 {
		t.Errorf("StartTimeMs = %f, want 0", bc.StartTimeMs)
	}
	if math.Abs(float64(bc.EndTimeMs-500.0)) > 1.0 {
		t.Errorf("EndTimeMs = %f, want 500", bc.EndTimeMs)
	}
}

func TestAudioChunker_MaxChunks(t *testing.T) {
	wav := generateTestWAV(t, 16000, 16, 5000) // 5 seconds

	ac := &AudioChunker{}
	chunks, err := ac.ChunkAudio(context.Background(), wav, chunking.ChunkOptions{
		MaxChunks: 3,
		Audio:     chunking.AudioChunkOptions{WindowDurationMs: 1000},
	})
	if err != nil {
		t.Fatalf("ChunkAudio failed: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3 (max_chunks limit)", len(chunks))
	}
}

func TestAudioChunker_EmptyInput(t *testing.T) {
	ac := &AudioChunker{}
	_, err := ac.ChunkAudio(context.Background(), []byte{}, chunking.ChunkOptions{
		Audio: chunking.AudioChunkOptions{WindowDurationMs: 2000},
	})
	if err == nil {
		t.Fatal("ChunkAudio with empty input should return error")
	}
}

func TestAudioChunker_InvalidWAV(t *testing.T) {
	garbage := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03}

	ac := &AudioChunker{}
	_, err := ac.ChunkAudio(context.Background(), garbage, chunking.ChunkOptions{
		Audio: chunking.AudioChunkOptions{WindowDurationMs: 2000},
	})
	if err == nil {
		t.Fatal("ChunkAudio with invalid WAV should return error")
	}
}

func TestAudioChunker_RoundtripWAV(t *testing.T) {
	sampleRate := 16000
	durationMs := 3000
	wav := generateTestWAV(t, sampleRate, 16, durationMs)

	ac := &AudioChunker{}
	chunks, err := ac.ChunkAudio(context.Background(), wav, chunking.ChunkOptions{
		Audio: chunking.AudioChunkOptions{WindowDurationMs: 1000},
	})
	if err != nil {
		t.Fatalf("ChunkAudio failed: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(chunks))
	}

	expectedSamplesPerChunk := sampleRate // 1s window = 16000 samples

	for i, chunk := range chunks {
		bc, err := chunk.AsBinaryContent()
		if err != nil {
			t.Fatalf("chunk[%d].AsBinaryContent() failed: %v", i, err)
		}

		samples, format, err := audio.ParseWAV(bc.Data)
		if err != nil {
			t.Fatalf("ParseWAV on chunk[%d] failed: %v", i, err)
		}

		if format.SampleRate != sampleRate {
			t.Errorf("chunk[%d] SampleRate = %d, want %d", i, format.SampleRate, sampleRate)
		}
		if format.BitsPerSample != 16 {
			t.Errorf("chunk[%d] BitsPerSample = %d, want 16", i, format.BitsPerSample)
		}
		if format.NumChannels != 1 {
			t.Errorf("chunk[%d] NumChannels = %d, want 1", i, format.NumChannels)
		}

		if len(samples) != expectedSamplesPerChunk {
			t.Errorf("chunk[%d] has %d samples, want %d", i, len(samples), expectedSamplesPerChunk)
		}
	}
}

func TestChunkPCM_Direct(t *testing.T) {
	sampleRate := 16000
	durationMs := 3000
	numSamples := sampleRate * durationMs / 1000
	samples := make([]float32, numSamples)
	for i := range samples {
		samples[i] = float32(math.Sin(2.0 * math.Pi * 440.0 * float64(i) / float64(sampleRate)))
	}

	ac := &AudioChunker{}
	chunks, err := ac.ChunkPCM(context.Background(), samples, audio.Format{
		SampleRate:    sampleRate,
		BitsPerSample: 16,
		NumChannels:   1,
	}, chunking.ChunkOptions{
		Audio: chunking.AudioChunkOptions{WindowDurationMs: 1000},
	})
	if err != nil {
		t.Fatalf("ChunkPCM failed: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(chunks))
	}

	for i, chunk := range chunks {
		if chunk.MimeType != "audio/wav" {
			t.Errorf("chunk[%d].MimeType = %q, want %q", i, chunk.MimeType, "audio/wav")
		}
	}
}

func TestChunkMP3_Windowing(t *testing.T) {
	data, err := os.ReadFile("../audio/testdata/sine_440hz_3s.mp3")
	if err != nil {
		t.Fatalf("reading MP3 fixture: %v", err)
	}

	ac := &AudioChunker{}
	chunks, err := ac.ChunkMP3(context.Background(), data, chunking.ChunkOptions{
		Audio: chunking.AudioChunkOptions{WindowDurationMs: 1000},
	})
	if err != nil {
		t.Fatalf("ChunkMP3 failed: %v", err)
	}

	// 3s MP3 with 1s windows should produce 3-4 chunks (MP3 encoding may add padding)
	if len(chunks) < 3 || len(chunks) > 4 {
		t.Fatalf("got %d chunks, want 3-4", len(chunks))
	}

	for i, chunk := range chunks {
		if chunk.MimeType != "audio/wav" {
			t.Errorf("chunk[%d].MimeType = %q, want %q", i, chunk.MimeType, "audio/wav")
		}
		bc, err := chunk.AsBinaryContent()
		if err != nil {
			t.Fatalf("chunk[%d].AsBinaryContent() failed: %v", i, err)
		}
		if len(bc.Data) == 0 {
			t.Errorf("chunk[%d] has empty data", i)
		}
	}
}

func TestChunkMP3_MaxChunks(t *testing.T) {
	data, err := os.ReadFile("../audio/testdata/sine_440hz_3s.mp3")
	if err != nil {
		t.Fatalf("reading MP3 fixture: %v", err)
	}

	ac := &AudioChunker{}
	chunks, err := ac.ChunkMP3(context.Background(), data, chunking.ChunkOptions{
		MaxChunks: 2,
		Audio:     chunking.AudioChunkOptions{WindowDurationMs: 1000},
	})
	if err != nil {
		t.Fatalf("ChunkMP3 failed: %v", err)
	}

	if len(chunks) != 2 {
		t.Fatalf("got %d chunks, want 2 (max_chunks limit)", len(chunks))
	}
}
