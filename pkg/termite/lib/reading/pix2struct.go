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

package reading

// Pix2Struct is a visual question answering model that takes an image and a question
// and produces a text answer. It uses variable-size image patches and supports
// DocVQA, ChartQA, InfographicsVQA, OCR-VQA, and other visual understanding tasks.

// Pix2StructDocVQAPrompt creates a document VQA prompt.
// Pix2Struct DocVQA models accept a natural language question as the prompt.
// Example: Pix2StructDocVQAPrompt("What is the total amount?")
func Pix2StructDocVQAPrompt(question string) string {
	return question
}

// Pix2StructChartQAPrompt creates a chart QA prompt.
// Pix2Struct ChartQA models accept a natural language question about chart content.
// Example: Pix2StructChartQAPrompt("What is the highest value?")
func Pix2StructChartQAPrompt(question string) string {
	return question
}

// Pix2StructInfographicsPrompt creates an infographics VQA prompt.
// Pix2Struct Infographics models accept a natural language question about infographic content.
// Example: Pix2StructInfographicsPrompt("What year had the most growth?")
func Pix2StructInfographicsPrompt(question string) string {
	return question
}
