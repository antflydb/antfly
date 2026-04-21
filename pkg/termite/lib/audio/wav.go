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

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// Format describes the audio format parameters of a WAV file.
type Format struct {
	SampleRate    int
	BitsPerSample int
	NumChannels   int
}

// ParseWAV parses a WAV file and returns mono float32 samples along with the format info.
// Unlike AudioProcessor.loadWAV, this does NOT resample — it returns samples at the original rate.
func ParseWAV(data []byte) ([]float32, Format, error) {
	reader := bytes.NewReader(data)

	// Read RIFF header
	var riffHeader [4]byte
	if _, err := io.ReadFull(reader, riffHeader[:]); err != nil {
		return nil, Format{}, fmt.Errorf("reading RIFF header: %w", err)
	}
	if string(riffHeader[:]) != "RIFF" {
		return nil, Format{}, fmt.Errorf("not a RIFF file")
	}

	// Skip file size
	var fileSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &fileSize); err != nil {
		return nil, Format{}, fmt.Errorf("reading file size: %w", err)
	}

	// Read WAVE format
	var waveHeader [4]byte
	if _, err := io.ReadFull(reader, waveHeader[:]); err != nil {
		return nil, Format{}, fmt.Errorf("reading WAVE header: %w", err)
	}
	if string(waveHeader[:]) != "WAVE" {
		return nil, Format{}, fmt.Errorf("not a WAVE file")
	}

	// Parse chunks
	var audioFormat, numChannels uint16
	var sampleRate uint32
	var bitsPerSample uint16
	var audioData []byte

	for {
		var chunkID [4]byte
		if _, err := io.ReadFull(reader, chunkID[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, Format{}, fmt.Errorf("reading chunk ID: %w", err)
		}

		var chunkSize uint32
		if err := binary.Read(reader, binary.LittleEndian, &chunkSize); err != nil {
			return nil, Format{}, fmt.Errorf("reading chunk size: %w", err)
		}

		switch string(chunkID[:]) {
		case "fmt ":
			if err := binary.Read(reader, binary.LittleEndian, &audioFormat); err != nil {
				return nil, Format{}, fmt.Errorf("reading audio format: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &numChannels); err != nil {
				return nil, Format{}, fmt.Errorf("reading num channels: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &sampleRate); err != nil {
				return nil, Format{}, fmt.Errorf("reading sample rate: %w", err)
			}
			// Skip byteRate and blockAlign
			var byteRate uint32
			var blockAlign uint16
			if err := binary.Read(reader, binary.LittleEndian, &byteRate); err != nil {
				return nil, Format{}, fmt.Errorf("reading byte rate: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &blockAlign); err != nil {
				return nil, Format{}, fmt.Errorf("reading block align: %w", err)
			}
			if err := binary.Read(reader, binary.LittleEndian, &bitsPerSample); err != nil {
				return nil, Format{}, fmt.Errorf("reading bits per sample: %w", err)
			}
			// Skip any extra format bytes
			remaining := int(chunkSize) - 16
			if remaining > 0 {
				_, _ = reader.Seek(int64(remaining), io.SeekCurrent)
			}

		case "data":
			audioData = make([]byte, chunkSize)
			if _, err := io.ReadFull(reader, audioData); err != nil {
				return nil, Format{}, fmt.Errorf("reading audio data: %w", err)
			}

		default:
			// Skip unknown chunks
			_, _ = reader.Seek(int64(chunkSize), io.SeekCurrent)
		}
	}

	if audioData == nil {
		return nil, Format{}, fmt.Errorf("no audio data found")
	}

	// Only support PCM format
	if audioFormat != 1 {
		return nil, Format{}, fmt.Errorf("unsupported audio format %d (only PCM supported)", audioFormat)
	}

	format := Format{
		SampleRate:    int(sampleRate),
		BitsPerSample: int(bitsPerSample),
		NumChannels:   int(numChannels),
	}

	// Convert to float32 mono samples
	samples, err := bytesToMonoSamples(audioData, int(bitsPerSample), int(numChannels))
	if err != nil {
		return nil, Format{}, fmt.Errorf("converting to samples: %w", err)
	}

	return samples, format, nil
}

// bytesToMonoSamples converts raw PCM bytes to float32 mono samples in range [-1, 1].
func bytesToMonoSamples(data []byte, bitsPerSample, numChannels int) ([]float32, error) {
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
				sample = (float64(s) - 128) / 128.0
			case 16:
				var s int16
				_ = binary.Read(reader, binary.LittleEndian, &s)
				sample = float64(s) / 32768.0
			case 24:
				var buf [3]byte
				_, _ = reader.Read(buf[:])
				s := int32(buf[0]) | int32(buf[1])<<8 | int32(buf[2])<<16
				if s&0x800000 != 0 {
					s |= -0x1000000
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
		samples[i] = float32(sampleSum / float64(numChannels))
	}

	return samples, nil
}

// EncodeWAV encodes float32 mono samples into a WAV file at the given sample rate and bit depth.
func EncodeWAV(samples []float32, format Format) ([]byte, error) {
	if len(samples) == 0 {
		return nil, fmt.Errorf("no samples to encode")
	}
	if format.SampleRate <= 0 {
		return nil, fmt.Errorf("invalid sample rate: %d", format.SampleRate)
	}
	if format.NumChannels <= 0 {
		format.NumChannels = 1
	}
	if format.BitsPerSample != 8 && format.BitsPerSample != 16 && format.BitsPerSample != 24 && format.BitsPerSample != 32 {
		return nil, fmt.Errorf("unsupported bits per sample: %d (must be 8, 16, 24, or 32)", format.BitsPerSample)
	}

	bytesPerSample := format.BitsPerSample / 8
	dataSize := len(samples) * bytesPerSample * format.NumChannels
	// RIFF header (12) + fmt chunk (24) + data chunk header (8) + data
	fileSize := 12 + 24 + 8 + dataSize

	buf := bytes.NewBuffer(make([]byte, 0, fileSize))

	// RIFF header
	buf.WriteString("RIFF")
	_ = binary.Write(buf, binary.LittleEndian, uint32(fileSize-8)) // file size minus RIFF header
	buf.WriteString("WAVE")

	// fmt chunk
	buf.WriteString("fmt ")
	_ = binary.Write(buf, binary.LittleEndian, uint32(16)) // chunk size
	_ = binary.Write(buf, binary.LittleEndian, uint16(1))  // PCM format
	_ = binary.Write(buf, binary.LittleEndian, uint16(format.NumChannels))
	_ = binary.Write(buf, binary.LittleEndian, uint32(format.SampleRate))
	byteRate := format.SampleRate * format.NumChannels * bytesPerSample
	_ = binary.Write(buf, binary.LittleEndian, uint32(byteRate))
	blockAlign := format.NumChannels * bytesPerSample
	_ = binary.Write(buf, binary.LittleEndian, uint16(blockAlign))
	_ = binary.Write(buf, binary.LittleEndian, uint16(format.BitsPerSample))

	// data chunk
	buf.WriteString("data")
	_ = binary.Write(buf, binary.LittleEndian, uint32(dataSize))

	// Write samples
	for _, s := range samples {
		// Clamp to [-1, 1]
		if s > 1.0 {
			s = 1.0
		} else if s < -1.0 {
			s = -1.0
		}

		// Duplicate mono sample across channels if needed
		for ch := 0; ch < format.NumChannels; ch++ {
			switch format.BitsPerSample {
			case 8:
				// 8-bit WAV is unsigned
				val := uint8(math.Round(float64(s)*127.0) + 128)
				_ = binary.Write(buf, binary.LittleEndian, val)
			case 16:
				val := int16(math.Round(float64(s) * 32767.0))
				_ = binary.Write(buf, binary.LittleEndian, val)
			case 24:
				val := int32(math.Round(float64(s) * 8388607.0))
				buf.Write([]byte{byte(val), byte(val >> 8), byte(val >> 16)})
			case 32:
				val := int32(math.Round(float64(s) * 2147483647.0))
				_ = binary.Write(buf, binary.LittleEndian, val)
			}
		}
	}

	return buf.Bytes(), nil
}
