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

	"github.com/hajimehoshi/go-mp3"
)

// ParseMP3 decodes an MP3 file and returns mono float32 samples in [-1, 1] along with format info.
// The decoder always outputs signed 16-bit little-endian stereo PCM at the MP3's sample rate.
// This function downmixes stereo to mono.
func ParseMP3(data []byte) ([]float32, Format, error) {
	decoder, err := mp3.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, Format{}, fmt.Errorf("creating MP3 decoder: %w", err)
	}

	pcm, err := io.ReadAll(decoder)
	if err != nil {
		return nil, Format{}, fmt.Errorf("decoding MP3: %w", err)
	}

	if len(pcm) == 0 {
		return nil, Format{}, fmt.Errorf("MP3 file contains no audio data")
	}

	sampleRate := decoder.SampleRate()

	// go-mp3 outputs signed 16-bit LE stereo (4 bytes per frame: 2 channels * 2 bytes)
	const (
		bytesPerSample = 2
		numChannels    = 2
		bytesPerFrame  = bytesPerSample * numChannels
	)

	numFrames := len(pcm) / bytesPerFrame
	samples := make([]float32, numFrames)

	reader := bytes.NewReader(pcm)
	for i := range numFrames {
		var left, right int16
		_ = binary.Read(reader, binary.LittleEndian, &left)
		_ = binary.Read(reader, binary.LittleEndian, &right)
		// Downmix stereo to mono and normalize to [-1, 1]
		samples[i] = float32((float64(left)+float64(right))/2.0) / 32768.0
	}

	format := Format{
		SampleRate:    sampleRate,
		BitsPerSample: 16,
		NumChannels:   1,
	}

	return samples, format, nil
}
