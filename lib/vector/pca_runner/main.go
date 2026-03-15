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
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/antflydb/antfly/lib/vector/stats"
)

func main() {
	vectorSet, err := LoadDataset("wikiarticles-768d-10k.gob")
	if err != nil {
		log.Fatalf("Could not load dataset: %v", err)
	}
	embeddings := stats.DenseMatrixFromVectorSet(vectorSet)

	// Perform PCA
	projPCA, err := stats.PCA(embeddings)
	if err != nil {
		log.Fatalf("Could not perform PCA: %v", err)
	}

	// Visualize PCA results
	err = stats.CreatePlot("pca_visualization.png", projPCA, nil)
	if err != nil {
		log.Fatalf("Could not create PCA plot: %v", err)
	}
	fmt.Println("Successfully generated PCA plot as 'pca_visualization.png'")

	// Perform t-SNE
	projTSNE := stats.TSNE(embeddings, 30.0, 200.0, 1000)

	// Visualize t-SNE results
	err = stats.CreatePlot("tsne_visualization.png", projTSNE, nil)
	if err != nil {
		log.Fatalf("Could not create t-SNE plot: %v", err)
	}
	fmt.Println("Successfully generated t-SNE plot as 'tsne_visualization.png'")
}

func LoadDataset(datasetName string) (*vector.Set, error) {
	// Resolve the dataset path relative to this source file's location.
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil, errors.New("could not get current file path")
	}
	filePath := filepath.Join(filepath.Dir(sourceFile), "..", "testdata", datasetName)

	f, err := os.Open(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return nil, fmt.Errorf("opening dataset file: %w", err)
	}
	defer func() { _ = f.Close() }()

	decoder := gob.NewDecoder(f)
	var vectors vector.Set_builder
	err = decoder.Decode(&vectors)
	if err != nil {
		return nil, fmt.Errorf("decoding dataset: %w", err)
	}

	return vectors.Build(), nil
}
