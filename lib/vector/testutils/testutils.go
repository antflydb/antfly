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

package testutils

import (
	"cmp"
	"encoding/gob"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/stretchr/testify/require"
)

// DbpediaDataset consists of 1K 1536 dimension embeddings from the DBpedia knowledge base,
// created using an OpenAI text embedding model.
//
// https://databus.dbpedia.org/dbpedia/collections/latest-core
const DbpediaDataset = "dbpedia-1536d-1k.gob"

// ImagesDataset consists of 10K 512 dimension image embeddings, created using an OpenAI image embedding model.
const ImagesDataset = "images-512d-10k.gob"

// WikiDataset consists of 10K 768 dimension vectors. Each vector is
// an embedding from the Tantivy Wiki Articles dataset, which contains
//
// http://fulmicoton.com/tantivy-files/wiki-articles-1000.json
//
// created using the nomic-embed-text model.
const WikiDataset = "wikiarticles-768d-10k.gob"

// LaionDataset consists embeddings from the open source Laion image dataset.
//
// https://laion.ai/projects/
// https://laion.ai/blog/laion-400-open-dataset/
//
// or https://www.kaggle.com/datasets/romainbeaumont/laion400m
//
// https://github.com/rom1504/img2dataset/blob/main/dataset_examples/laion400m.md
//
// img2dataset --url_list laion400m-meta --input_format "parquet"\
// --url_col "URL" --caption_col "TEXT" --output_format webdataset\
// --output_folder laion400m-data --processes_count 1 --thread_count 128 --image_size 256\
// --save_additional_columns '["NSFW","similarity","LICENSE"]'
//
// Created using the https://github.com/openai/CLIP model.
const LaionDatasetCLIP = "laionclip-768d-1k.gob"

// Created using the Google multimodal embedding model (multimodalembedding).
//
// First 1000 images
const LaionDatasetGemini1k = "laiongemini-1408d-1k.gob"

// Taken from first 20k images but tail -n 10000
const LaionDatasetGemini10k = "laiongemini-512d-10k.gob"

// FashionMinstDataset1k consists of 1K 28x28 greyscale image vectors (flattened to
// 784 = 28 x 28 dimensions of clothing items,  with pixel values ranging from 0 to 255.
//
// Download the dataset from
// https://github.com/zalandoresearch/fashion-mnist
//
// Used a script like this to convert the original idx files to a Go gob file:
// https://github.com/schuyler/neural-go/blob/master/mnist/mnist.go
// See minst/main.go and minst/t10k-images-idx3-ubyte.gz
//
// NOTE: This is a only portion of the full fashion-mnist-784-euclidean dataset.
//
// SKEW: Some pixels, especially those near the image borders, are zero across
// almost all images and therefore contain very little information. A minority of pixels
// near the center of the image often contain  most of the information. For skew like
// this, where the original values are only positive, the Random Orthogonal Transform
// will produce a distribution of positive and negative values across all dimensions.
const (
	FashionMinstDataset1k  = "fashionminst-784d-1k.gob"
	FashionMinstDataset10k = "fashionminst-784d-10k.gob"
)

// RandomDatasets were randomly generated, with float32 coordinates ranging between -14 to +14
const (
	RandomDataset20d = "random-20d-1k.gob"
	RandomDataset40d = "random-40d-10k.gob"
)

// High-dimensional random datasets for testing SME-accelerated batch operations.
// These datasets are used to verify performance improvements with dimension-aware dispatching.
const (
	RandomDataset2048d = "random-2048d-1k.gob" // 2048d × 1K vectors (11 MB)
	RandomDataset4096d = "random-4096d-1k.gob" // 4096d × 1K vectors (23 MB)
)

// RandomSparseDataset1k is a randomly generated sparse dataset with 1000 vectors,
// simulating SPLADE-style sparse embeddings with 10-100 non-zero entries per vector
// over a 30522-token vocabulary space.
const RandomSparseDataset1k = "random-sparse-1k.gob"

// SparseWikiDataset consists of sparse embeddings from the Tantivy Wiki Articles
// dataset (same source as WikiDataset), created using the SPLADE model
// (naver/splade-cocondenser-ensembledistil) via Termite.
const SparseWikiDataset = "sparse-wiki-1k.gob"

// SparseDataEntry is a gob-serializable sparse vector entry.
type SparseDataEntry struct {
	Indices []uint32
	Values  []float32
}

// SparseDataset is a gob-serializable container for sparse vector test data.
// Protobuf opaque types don't support gob encoding, so this struct provides
// a simple alternative for test data serialization.
type SparseDataset struct {
	Count   int64
	Vectors []SparseDataEntry
}

// LoadDataset loads a pre-built dataset serialized as a vector.Set in a .gob
// file on disk. This is useful for testing. See the dataset name constants for
// descriptions of the available datasets (e.g. LaionDataset).
func LoadDataset(t testing.TB, datasetName string) *vector.Set {
	var filePath string
	// Get the absolute path of this test file.
	_, testFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	// Point to the dataset file.
	parentDir := filepath.Dir(testFile)
	filePath = filepath.Join(parentDir, "..", "testdata", datasetName)

	f, err := os.Open(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	decoder := gob.NewDecoder(f)
	var vectors vector.Set_builder
	err = decoder.Decode(&vectors)
	require.NoError(t, err)

	return vectors.Build()
}

// LoadSparseDataset loads a pre-built sparse dataset serialized as a SparseDataset
// in a .gob file on disk. Returns a slice of *vector.SparseVector for use in tests.
func LoadSparseDataset(t testing.TB, datasetName string) []*vector.SparseVector {
	_, testFile, _, ok := runtime.Caller(0)
	require.True(t, ok)

	parentDir := filepath.Dir(testFile)
	filePath := filepath.Join(parentDir, "..", "testdata", datasetName)

	f, err := os.Open(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	decoder := gob.NewDecoder(f)
	var dataset SparseDataset
	err = decoder.Decode(&dataset)
	require.NoError(t, err)

	vecs := make([]*vector.SparseVector, len(dataset.Vectors))
	for i, entry := range dataset.Vectors {
		vecs[i] = vector.NewSparseVector(entry.Indices, entry.Values)
	}
	return vecs
}

// CalculateTruth calculates the top k true nearest data vectors for the given
// query vector. It returns the keys of the top k results, sorted by distance.
//
// For high-dimensional vectors (>= 3000d), this uses SME-accelerated batch processing
// on M4+ processors. For typical embeddings (< 3000d), it uses direct sequential
// processing to avoid batch API overhead.
func CalculateTruth[T comparable](
	k int,
	distMetric vector.DistanceMetric,
	queryVector vector.T,
	dataVectors *vector.Set,
	dataKeys []T,
) []T {
	distances := make([]float32, dataVectors.GetCount())
	offsets := make([]int, dataVectors.GetCount())

	count := int(dataVectors.GetCount())
	dims := int(dataVectors.GetDims())

	// For high dimensions (>= 3000), use batch API which will use SME on M4+
	// For low dimensions (< 3000), use direct sequential to avoid batch overhead
	if dims >= 3000 {
		// High-dimensional: use batch processing with SME acceleration
		switch distMetric {
		case vector.DistanceMetric_L2Squared:
			vector.BatchL2SquaredDistance(
				queryVector,
				dataVectors.GetData(),
				distances,
				count,
				dims,
			)
		case vector.DistanceMetric_InnerProduct:
			vector.BatchDot(queryVector, dataVectors.GetData(), distances, count, dims)
			for i := range distances {
				distances[i] = -distances[i]
			}
		case vector.DistanceMetric_Cosine:
			vector.BatchDot(queryVector, dataVectors.GetData(), distances, count, dims)
			for i := range distances {
				distances[i] = 1 - distances[i]
			}
		}
	} else {
		// Low-dimensional: use direct sequential processing (faster due to no batch overhead)
		for i := range dataVectors.GetCount() {
			distances[i] = vector.MeasureDistance(distMetric, queryVector, dataVectors.At(int(i)))
		}
	}

	for i := range dataVectors.GetCount() {
		offsets[i] = int(i)
	}

	sort.SliceStable(offsets, func(i int, j int) bool {
		if res := cmp.Compare(distances[offsets[i]], distances[offsets[j]]); res != 0 {
			return res < 0
		}
		// Break ties with offsets.
		return offsets[i] < offsets[j]
	})

	truth := make([]T, k)
	for i, offset := range offsets[:k] {
		truth[i] = dataKeys[offset]
	}
	return truth
}

// CalculateRecall returns the percentage overlap of the predicted set with the
// truth set. If the predicted set has fewer items than the truth set, it is
// treated as if the predicted set has missing/incorrect items that reduce the
// recall rate. Keys in the sets must be comparable.
func CalculateRecall[T comparable](prediction, truth []T) float64 {
	predictionMap := make(map[T]struct{}, len(prediction))
	for _, p := range prediction {
		predictionMap[p] = struct{}{}
	}

	var intersect float64
	for _, t := range truth {
		if _, ok := predictionMap[t]; ok {
			intersect++
		}
	}
	return intersect / float64(len(truth))
}
