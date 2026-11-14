// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private final class PreferenceWriteCount: @unchecked Sendable {
  private let lock = NSLock()
  private var count = 0
  func increment() { lock.withLock { count += 1 } }
  var value: Int { lock.withLock { count } }
}

@Suite(.serialized) struct PreferenceAutosaveTests {
  private func directory() throws -> URL {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
    return root
  }

  @Test @MainActor func ordinaryEditsCoalesceAndSaveTheLatestSnapshot() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let writes = PreferenceWriteCount()
    let store = Store(
      directory: root, registerHotkey: false,
      settingsWriter: AtomicFileWriter(ownedDirectory: root, beforeWrite: { writes.increment() }))
    store.enableAutomaticSaving()
    store.config.dictionary = ["First"]
    store.config.dictionary = ["Latest"]
    #expect(writes.value == 0)
    await store.flushAutomaticSettings()
    let saved = try Configuration.decode(
      Data(contentsOf: root.appendingPathComponent("settings.json")))
    #expect(saved.dictionary == ["Latest"] && writes.value == 1)
    #expect(!store.hasUnsavedSettings && store.settingsSaveStatus == "Saved")
    await store.prepareToQuit()
    #expect(writes.value == 1)
  }

  @Test(.timeLimit(.minutes(1))) @MainActor
  func recordingDefersAutosaveUntilIdle() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    store.enableAutomaticSaving()
    store.state = .recording
    store.config.dictionary = ["After recording"]
    await store.flushAutomaticSettings()
    #expect(
      !FileManager.default.fileExists(atPath: root.appendingPathComponent("settings.json").path))
    store.state = .idle
    await store.automaticSaveTask?.value
    #expect(!store.hasUnsavedSettings)
    #expect(
      try Configuration.decode(Data(contentsOf: root.appendingPathComponent("settings.json")))
        .dictionary == ["After recording"])
    await store.prepareToQuit()
  }

  @Test @MainActor func retentionChangesCannotBeAutosavedOrFlushedByQuit() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    var original = Configuration()
    original.retainHistory = true
    original.retainAudio = true
    let settings = root.appendingPathComponent("settings.json")
    let audio = root.appendingPathComponent("last-recording.wav")
    try JSONEncoder().encode(original).write(to: settings)
    try Data([1, 2, 3]).write(to: audio)
    let store = Store(directory: root, registerHotkey: false)
    store.enableAutomaticSaving()
    store.config.retainAudio = false
    store.config.dictionary = ["Also pending"]
    store.runModelSetup(.whisper) {
      Issue.record("Setup must not implicitly approve pending retention changes")
      return PreparedModelSetup(
        configuration: original, result: ModelSetupResult(operation: .whisper))
    }
    #expect(store.modelSetupTask == nil)
    store.publish()
    #expect(!store.working)
    await store.flushAutomaticSettings()
    #expect(store.requiresRetentionConfirmation && store.hasUnsavedSettings)
    #expect(try Configuration.decode(Data(contentsOf: settings)) == original)
    await store.prepareToQuit()
    #expect(FileManager.default.fileExists(atPath: audio.path))
    #expect(try Configuration.decode(Data(contentsOf: settings)) == original)
  }

  @Test @MainActor func invalidEditsAndFailedWritesStayVisibleAndRecoverOnNextEdit() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    store.enableAutomaticSaving()
    store.config.presets[0].model = ""
    await store.flushAutomaticSettings()
    #expect(store.settingsSaveError != nil && store.hasUnsavedSettings)
    store.config.presets[0].model = "transcribers/whisper"
    store.config.dictionary = ["Corrected"]
    // Fail publication, then repair the destination and retry without losing edits.
    let settings = root.appendingPathComponent("settings.json")
    try FileManager.default.createDirectory(at: settings, withIntermediateDirectories: false)
    await store.flushAutomaticSettings()
    #expect(store.settingsSaveError != nil && store.settingsSaveStatus == "Changes not saved")
    #expect(store.automaticSaveTask == nil)
    try FileManager.default.removeItem(at: settings)
    await store.flushAutomaticSettings()
    #expect(store.settingsSaveError == nil && !store.hasUnsavedSettings)
    await store.prepareToQuit()
  }

  @Test(arguments: [false, true]) @MainActor
  func quitFlushesDebouncedOrdinaryEdits(evenDuringRecording: Bool) async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    store.enableAutomaticSaving()
    store.config.dictionary = ["Save before quit"]
    if evenDuringRecording { store.state = .recording }
    await store.prepareToQuit()
    #expect(
      try Configuration.decode(Data(contentsOf: root.appendingPathComponent("settings.json")))
        .dictionary == ["Save before quit"])
    #expect(store.automaticSaveTask == nil && !store.savingSettings)
  }

  @Test @MainActor func unreadableSettingsRequireExplicitRecoveryEvenOnQuit() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let settings = root.appendingPathComponent("settings.json")
    let history = root.appendingPathComponent("history.json")
    let corrupt = Data("{ broken settings".utf8)
    try corrupt.write(to: settings)
    try Data("[]".utf8).write(to: history)
    let store = Store(directory: root, registerHotkey: false)
    #expect(store.settingsLoadError != nil)
    store.enableAutomaticSaving()
    store.config.dictionary = ["Do not overwrite recovery data"]
    await store.flushAutomaticSettings()
    await store.prepareToQuit()
    #expect(try Data(contentsOf: settings) == corrupt)
    #expect(FileManager.default.fileExists(atPath: history.path))
    #expect(store.hasUnsavedSettings)
  }

  @Test @MainActor func processorRecoveryChangesOnlyTheSelectedPresetsOutputMode() async throws {
    let root = try directory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    store.config.presets[0].outputMode = .rewrite
    store.config.presets[0].instructions = "Keep my tone"
    store.config.presets[1].outputMode = .clean
    #expect(store.selectedPresetNeedsProcessor)
    store.useVerbatimForSelectedPreset()
    #expect(!store.selectedPresetNeedsProcessor)
    #expect(store.config.presets[0].instructions == "Keep my tone")
    #expect(store.config.presets[1].outputMode == .clean)
    await store.prepareToQuit()
  }
}
