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
	"flag"
	"fmt"
	"os"
)

func main() {
	tracePath := flag.String("trace", "../../src/testing/testdata/differential_campaign_proposal.json", "path to a Zig trace JSON fixture")
	seedStart := flag.Uint64("seed-start", 0, "starting seed for seeded differential sweep")
	seedCount := flag.Int("seed-count", 0, "number of seeded traces to generate and compare")
	steps := flag.Int("steps", 24, "number of actions per seeded trace")
	zigBin := flag.String("zig", "", "path to zig binary for seeded trace generation")
	checkQuorum := flag.Bool("check-quorum", true, "whether seeded generated traces should enable check_quorum")
	preVote := flag.Bool("pre-vote", true, "whether seeded generated traces should enable pre_vote")
	profile := flag.String("profile", "stable", "seeded sweep profile: stable or stress")
	flag.Parse()

	if *seedCount > 0 {
		if err := RunSeededSweep(SeededSweepOptions{
			ZigBin:      *zigBin,
			SeedStart:   *seedStart,
			Count:       *seedCount,
			Steps:       *steps,
			CheckQuorum: *checkQuorum,
			PreVote:     *preVote,
			Profile:     *profile,
		}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		fmt.Printf(
			"seeded sweep matched: seeds %d..%d (%d traces, %d steps, check_quorum=%t, pre_vote=%t, profile=%s)\n",
			*seedStart,
			*seedStart+uint64(*seedCount)-1,
			*seedCount,
			*steps,
			*checkQuorum,
			*preVote,
			*profile,
		)
		return
	}

	if err := CompareTraceFile(*tracePath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("trace matched: %s\n", *tracePath)
}
