// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: grepstein stage-originals|fetch|fetch-archive|prepare-small|prepare|publish|load|enrich|wait|qualify|graph|grep|cloud-jobs|stats|explore|datasets")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "stage-originals":
		err = stageOriginalsCmd(os.Args[2:])
	case "fetch-archive":
		err = fetchArchiveCmd(os.Args[2:])
	case "qualify":
		err = qualifyCmd(os.Args[2:])
	case "wait":
		err = waitCmd(os.Args[2:])
	case "cloud-jobs":
		err = cloudJobsCmd(os.Args[2:])
	case "prepare-small":
		err = prepareSmallCmd(os.Args[2:])
	case "grep":
		err = grepCmd(os.Args[2:])
	case "fetch":
		err = fetchCmd(os.Args[2:])
	case "publish":
		err = publishCmd(os.Args[2:])
	case "enrich":
		err = enrichCmd(os.Args[2:])
	case "explore":
		err = exploreCmd(os.Args[2:])
	case "datasets":
		err = datasetsCmd(os.Args[2:])
	default:
		err = corpusCmd(os.Args[1:])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
