// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationCore

@Test func systemSpeechLanguageUsesSupportedEquivalentAndOrderedPreferences() throws {
  #expect(
    try SpeechLanguages.system(preferred: ["en-BR", "fr-FR"], supported: ["fr_FR", "en_US"])
      == "en-US")
  #expect(
    try SpeechLanguages.system(preferred: ["xx-XX", "fr-FR"], supported: ["en-US", "fr_FR"])
      == "fr-FR")
  #expect(throws: (any Error).self) {
    try SpeechLanguages.system(preferred: ["xx-XX"], supported: ["en-US"])
  }
}

@Test func explicitAndLegacyLanguagesSurviveSystemDefaultsAndWhisperUsesLanguageCodes() throws {
  var preset = Preset(id: "test", name: "Test")
  #expect(preset.speechLanguageChoice == .system)
  preset.language = "fr_FR"
  #expect(preset.speechLanguageChoice == .custom)
  #expect(try preset.resolvingLanguage(preferred: ["en-US"]).language == "fr-FR")
  #expect(try preset.resolvingLanguage(preferred: ["en-US"], whisper: true).language == "fr")
  preset.speechLanguageChoice = .system
  #expect(try preset.resolvingLanguage(preferred: ["de-DE"], whisper: true).language == "de")
  #expect(preset.language == "fr_FR")
}

@Test func detectionRequiresEngineSupportAndServerDefaultStaysUnspecified() throws {
  var preset = Preset(id: "test", name: "Test")
  preset.speechLanguageChoice = .detect
  #expect(throws: (any Error).self) { try preset.resolvingLanguage() }
  #expect(try preset.resolvingLanguage(detectsLanguage: true).language.isEmpty)
  preset.speechLanguageChoice = .server
  #expect(try preset.resolvingLanguage().language.isEmpty)
}

@Test func liveNativeAppleSupportsIncrementalCleanupAndRewriteKeepsWholeUtterance() throws {
  var preset = Preset(id: "test", name: "Test")
  preset.model = "transcribers/apple-speech"
  preset.cleanupModel = "generators/cleanup"
  preset.outputMode = .clean
  preset.processingStrategy = .incremental
  #expect(throws: (any Error).self) { try ProcessingPlan(preset: preset, model: nil) }
  let live = try ProcessingPlan(preset: preset, model: nil, nativeAppleStreaming: true)
  #expect(live.session && live.incrementalCleanup)
  preset.outputMode = .rewrite
  preset.processingStrategy = .automatic
  let rewrite = try ProcessingPlan(preset: preset, model: nil, nativeAppleStreaming: true)
  #expect(rewrite.session && !rewrite.incrementalCleanup)
}

@Test func compactVocabularyPreservesSmallDictionariesAndRelevantPreferredSpellings() {
  #expect(
    CleanupVocabulary.relevant(["Antfly", "Colony", "Antfly"], to: "hello") == ["Antfly", "Colony"])
  let terms = (0..<64).map { "UnrelatedEntry" + String($0) } + ["Antfly"]
  #expect(CleanupVocabulary.relevant(terms, to: "What about ant fly now?") == ["Antfly"])
  #expect(CleanupVocabulary.relevant(terms, to: "unrecognized speech") == terms)
}
