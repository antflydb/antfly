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
  #expect(CleanupVocabulary.relevant(terms, to: "What about ant fly now?") == terms)
  #expect(CleanupVocabulary.relevant(terms, to: "unrecognized speech") == terms)
}

@Test func whisperSetupDoesNotRequireAppleLanguageSupport() throws {
  var preset = Preset(id: "test", name: "Test")
  preset.speechLanguageChoice = .detect
  #expect(try preset.setupLanguage(for: .whisper, supportedAppleLanguages: ["en-US"]) == "")
  #expect(throws: (any Error).self) {
    try preset.setupLanguage(for: .appleSpeech, supportedAppleLanguages: ["en-US"])
  }
  preset.speechLanguageChoice = .custom
  preset.language = "hi-IN"
  #expect(try preset.setupLanguage(for: .whisper, supportedAppleLanguages: ["en-US"]) == "")
  preset.language = "fr-FR"
  #expect(try preset.setupLanguage(for: .appleSpeech).hasPrefix("fr"))
}

@Test func dictionaryKeepsUnmatchedCorrectionsWhenOtherTermsMatch() {
  let terms = (0..<40).map { "Entry" + String($0) } + ["Colony", "Antfly", "Antfly"]
  #expect(CleanupVocabulary.relevant(terms, to: "Colony uses amplant") == Array(terms.dropLast()))
}

@Test func transcriptBoundariesPreserveWhitespaceAndUnspacedLanguages() {
  #expect(TranscriptAssembly.join(["Hello", " world", "."]) == "Hello world.")
  #expect(TranscriptAssembly.join(["hello", "world"]) == "hello world")
  #expect(TranscriptAssembly.join(["こんにちは", "世界"], language: "ja-JP") == "こんにちは世界")
  #expect(TranscriptAssembly.join(["你好", "世界"], language: "zh-CN") == "你好世界")
  #expect(TranscriptAssembly.join(["你好", "世界"]) == "你好世界")
  #expect(TranscriptAssembly.join(["Hello\n", "world"]) == "Hello\nworld")
  #expect(TranscriptAssembly.join(["Hello ", "world"]) == "Hello world")
}

@Test func appSpecificRecordingUsesOnlyItsOwnCleanupPreparation() {
  var config = Configuration()
  config.presets[0].outputMode = .clean
  config.presets[0].cleanupModel = "generators/default"
  var specific = Preset(id: "specific", name: "Specific")
  specific.appIDs = ["test.editor"]
  specific.outputMode = .verbatim
  config.presets.append(specific)
  #expect(config.preset(for: nil).canUseCleanupPreparation(for: "generators/default"))
  #expect(!config.preset(for: "test.editor").canUseCleanupPreparation(for: "generators/default"))
  specific.outputMode = .clean
  specific.cleanupModel = "generators/editor"
  config.presets[config.presets.count - 1] = specific
  #expect(!config.preset(for: "test.editor").canUseCleanupPreparation(for: "generators/default"))
  #expect(config.preset(for: "test.editor").canUseCleanupPreparation(for: "generators/editor"))
  #expect(!specific.canUseCleanupPreparation(for: nil))
}
