// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationCore

@Test func outputModesRequireAnExplicitTextProcessor() throws {
  try DictationMode.verbatim.validateProcessor("")
  for mode in [DictationMode.clean, .rewrite] {
    #expect(throws: (any Error).self) { try mode.validateProcessor(" \n") }
    try mode.validateProcessor("generators/cleanup")
  }
}

@Test func freshPresetsDoNotPromiseUnconfiguredCleanup() {
  for preset in Configuration().presets {
    #expect(preset.outputMode == .verbatim)
    #expect(!preset.runsCleanup)
  }
}

@Test func missingAppleAssetsExplainsInstallationOnTheEngineHost() throws {
  let response = HTTPURLResponse(
    url: URL(string: "http://localhost:8080/ai/v1/dictate")!,
    statusCode: 503, httpVersion: nil, headerFields: nil)!
  let failure = ServerFailure(
    response: response,
    body: Data(
      #"{"error":{"code":"AppleSpeechAssetsMissing","message":"model unavailable"}}"#.utf8))
  #expect(failure.localizedDescription.contains("language assets"))
  #expect(failure.localizedDescription.contains("external server"))
  #expect(failure.localizedDescription.contains("download_assets"))
}
