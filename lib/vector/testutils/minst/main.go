// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package main

import (
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/antflydb/antfly/lib/vector"
)

func ReadMNISTImages(r io.Reader) (images [][]byte, width, height int) {
	header := [4]int32{}
	_ = binary.Read(r, binary.BigEndian, &header)
	images = make([][]byte, header[1])
	width, height = int(header[2]), int(header[3])
	for i := range len(images) {
		images[i] = make([]byte, width*height)
		_, _ = r.Read(images[i])
	}
	return
}

func ImageString(buffer []byte, height, width int) (out string) {
	var outSb28 strings.Builder
	for i, y := 0, 0; y < height; y++ {
		var outSb29 strings.Builder
		for range width {
			if buffer[i] > 128 {
				outSb29.WriteString("#")
			} else {
				outSb29.WriteString(" ")
			}
			i++
		}
		out += outSb29.String()
		outSb28.WriteString("\n")
	}
	out += outSb28.String()
	return
}

func FlattenMNISTImages(images [][]byte, width, height int) []float32 {
	flattened := make([]float32, len(images)*width*height)
	for i, img := range images {
		for j, px := range img {
			flattened[i*width*height+j] = pixelWeight(px)
		}
	}
	return flattened
}

func OpenFile(path string) *os.File {
	file, err := os.Open(path) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", path, err)
	}
	return file
}

const pixelRange = 255

func pixelWeight(px byte) float32 {
	return float32(px)/pixelRange*0.9 + 0.1
}

const only1000 = false // Set to true to only read 1000 images
func main() {
	f := OpenFile("t10k-images-idx3-ubyte")
	i, w, h := ReadMNISTImages(f)
	dims := w * h // 28x28 = 784

	if only1000 {
		i = i[:1000] // Limit to 1000 images
	}

	data := FlattenMNISTImages(i, w, h)

	vectorSet := &vector.Set_builder{
		Dims:  int64(dims),
		Count: int64(len(i)),
		Data:  data,
	}
	// Determine output path
	outputPath := "fashionminst-784d-10k.gob"

	var filePath string
	// Get the absolute path of this test file.
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("Failed to get current file path")
	}

	// Point to the dataset file.
	parentDir := filepath.Dir(testFile)
	filePath = filepath.Join(parentDir, "..", "..", "testdata", outputPath)

	// Create testdata directory if it doesn't exist
	testdataDir := filepath.Dir(filePath)
	if err := os.MkdirAll(testdataDir, 0o755); err != nil { //nolint:gosec // G301: standard permissions for data directory
		log.Fatalf("Failed to create testdata directory: %v", err)
	}

	// Save to file
	file, err := os.Create(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer func() { _ = file.Close() }()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(vectorSet); err != nil {
		log.Fatalf("Failed to encode vector set: %v", err)
	}
}
