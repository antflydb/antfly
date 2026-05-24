// Copyright 2026 Antfly, Inc.
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

package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/vector"
	"google.golang.org/protobuf/proto"
)

type config struct {
	input    string
	output   string
	dataset  string
	datasets string
	outDir   string
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.input, "input", "", "path to a gob-encoded vector.Set dataset")
	flag.StringVar(&cfg.output, "output", "", "path to write the protobuf VectorSet payload")
	flag.StringVar(&cfg.dataset, "dataset", "", "dataset basename from antfly/lib/vector/testdata")
	flag.StringVar(&cfg.datasets, "datasets", "", "comma-separated dataset basenames from antfly/lib/vector/testdata")
	flag.StringVar(&cfg.outDir, "out-dir", "", "output directory for -dataset/-datasets conversions")
	flag.Parse()

	switch {
	case cfg.input != "" && cfg.output != "":
		if err := exportOne(cfg.input, cfg.output); err != nil {
			fail(err)
		}
	case cfg.dataset != "":
		if cfg.outDir == "" {
			fail(fmt.Errorf("-out-dir is required with -dataset"))
		}
		input := antflyDatasetPath(cfg.dataset)
		output := filepath.Join(cfg.outDir, convertedName(cfg.dataset))
		if err := exportOne(input, output); err != nil {
			fail(err)
		}
	case cfg.datasets != "":
		if cfg.outDir == "" {
			fail(fmt.Errorf("-out-dir is required with -datasets"))
		}
		for _, dataset := range strings.Split(cfg.datasets, ",") {
			dataset = strings.TrimSpace(dataset)
			if dataset == "" {
				continue
			}
			input := antflyDatasetPath(dataset)
			output := filepath.Join(cfg.outDir, convertedName(dataset))
			if err := exportOne(input, output); err != nil {
				fail(err)
			}
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func exportOne(inputPath, outputPath string) error {
	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	var builder vector.Set_builder
	if err := gob.NewDecoder(f).Decode(&builder); err != nil {
		return err
	}
	set := builder.Build()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(set)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(outputPath, payload, 0o644); err != nil {
		return err
	}
	fmt.Printf("exported %s -> %s (%d dims, %d vectors)\n", inputPath, outputPath, set.GetDims(), set.GetCount())
	return nil
}

func antflyDatasetPath(name string) string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		fail(fmt.Errorf("unable to resolve tool location"))
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "antfly", "lib", "vector", "testdata", name)
}

func convertedName(name string) string {
	if strings.HasSuffix(name, ".gob") {
		name = strings.TrimSuffix(name, ".gob")
	}
	return name + ".pbvec"
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
