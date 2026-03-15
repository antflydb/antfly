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

package stats

import (
	"errors"

	"github.com/antflydb/antfly/lib/vector"
	"github.com/danaugrs/go-tsne"
	"gonum.org/v1/gonum/mat"
	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func DenseMatrixFromVectorSet(vectorSet *vector.Set) *mat.Dense {
	flatData := vectorSet.GetData()

	matData := make([]float64, len(flatData))
	for i := range flatData {
		matData[i] = float64(flatData[i])
	}

	return mat.NewDense(int(vectorSet.GetCount()), int(vectorSet.GetDims()), matData)
}

func PCA(m *mat.Dense) (mat.Matrix, error) {
	standardized := standardize(m)

	var pc stat.PC
	ok := pc.PrincipalComponents(standardized, nil)
	if !ok {
		return nil, errors.New("PCA computation failed")
	}

	k := 2
	rows, _ := m.Dims()
	if rows == 0 {
		return nil, errors.New("input matrix has no rows")
	}
	var pc2 mat.Dense
	pc.VectorsTo(&pc2)
	if rows, _ := pc2.Dims(); rows == 0 {
		return nil, errors.New("PCA resulted in no components")
	}
	// Get first k principal components
	pcReduced := pc2.Slice(0, rows, 0, k)
	// Project the data
	proj := mat.NewDense(rows, k, nil)
	proj.Mul(standardized, pcReduced)
	return proj, nil
}

// standardize applies Z-score normalization to each column of the matrix.
func standardize(m *mat.Dense) *mat.Dense {
	rows, cols := m.Dims()
	standardized := mat.NewDense(rows, cols, nil)

	for j := range cols {
		col := mat.Col(nil, j, m)
		mean := stat.Mean(col, nil)
		stdDev := stat.StdDev(col, nil)

		if stdDev == 0 {
			// If a column has no variance, its standardized values will be 0.
			// Avoid division by zero.
			for i := range rows {
				standardized.Set(i, j, 0)
			}
			continue
		}

		for i := range rows {
			val := m.At(i, j)
			standardized.Set(i, j, (val-mean)/stdDev)
		}
	}
	return standardized
}

func MatrixToFlat(m mat.Matrix) []float64 {
	rows, cols := m.Dims()
	ret := make([]float64, rows*cols)
	for i := range rows {
		for j := range cols {
			ret[i*cols+j] = m.At(i, j)
		}
	}
	return ret
}

// TSNE performs t-SNE dimensionality reduction on the given matrix.
func TSNE(m *mat.Dense, perplexity, learningRate float64, iterations int) mat.Matrix {
	model := tsne.NewTSNE(2, perplexity, learningRate, iterations, true)
	return model.EmbedData(m, nil)
}

type Labeler interface {
	Labels() []string
}

// CreatePlot generates and saves a scatter plot from 2D projected data.
func CreatePlot(name string, data mat.Matrix, labelers []Labeler) error {
	rows, _ := data.Dims()
	pts := make(plotter.XYs, rows)

	for i := range rows {
		pts[i].X = data.At(i, 0)
		pts[i].Y = data.At(i, 1)
	}

	p := plot.New()

	p.Title.Text = "Visualization of Embeddings Index"
	p.X.Label.Text = "X Axis"
	p.Y.Label.Text = "Y Axis"

	s, err := plotter.NewScatter(pts)
	if err != nil {
		return err
	}
	s.Radius = vg.Points(3)
	p.Add(s)

	if len(labelers) > 0 && len(labelers) == rows {
		for i, labeler := range labelers {
			if labeler == nil || len(labeler.Labels()) == 0 {
				continue
			}
			l, err := plotter.NewLabels(plotter.XYLabels{
				XYs:    []plotter.XY{pts[i]},
				Labels: labeler.Labels(),
			})
			if err != nil {
				return err
			}
			p.Add(l)
		}
	}

	return p.Save(64*vg.Inch, 42*vg.Inch, name)
}
