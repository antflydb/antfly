// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private func setupDirectory(_ config: Configuration) throws -> URL {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  return directory
}
@Test @MainActor func fnSetupWaitsForPermissionAndAppliesAutomaticallyOnReturn() throws {
  var saved = Configuration()
  saved.effectiveShortcut = .controlOptionSpace
  let directory = try setupDirectory(saved)
  defer { try? FileManager.default.removeItem(at: directory) }
  var trusted = false
  var prompts = 0
  var panes: [String] = []
  let store = Store(
    directory: directory, registerHotkey: false, keyboardAccess: { trusted },
    keyboardAccessRequest: { prompts += 1 }, settingsOpener: { panes.append($0) })
  store.setUpFnShortcut()
  #expect(store.fnSetupPending && prompts == 1)
  #expect(store.config.effectiveShortcut == .controlOptionSpace)
  #expect(panes == ["com.apple.preference.security?Privacy_Accessibility"])
  let file = directory.appendingPathComponent("settings.json")
  #expect(try Configuration.decode(Data(contentsOf: file)).effectiveShortcut == .controlOptionSpace)
  trusted = true
  store.refreshShortcutAccess()
  #expect(!store.fnSetupPending && store.config.effectiveShortcut == .fn)
  #expect(try Configuration.decode(Data(contentsOf: file)).effectiveShortcut == .fn)
  #expect(store.fnSetupStatus.contains("enabled and saved"))
}
@Test @MainActor func fnSetupSavesOnlyTheShortcutAndDoesNotPersistUnsavedEdits() throws {
  var saved = Configuration()
  saved.server = "https://saved.example"
  saved.dictionary = ["Saved"]
  saved.effectiveShortcut = .controlOptionSpace
  let directory = try setupDirectory(saved)
  defer { try? FileManager.default.removeItem(at: directory) }
  let store = Store(
    directory: directory, registerHotkey: false, keyboardAccess: { true },
    keyboardAccessRequest: { Issue.record("Already authorized setup requested permission") },
    settingsOpener: { _ in Issue.record("Already authorized setup opened Settings") })
  store.config.server = "https://unsaved.example"
  store.config.dictionary = ["Unsaved"]
  store.token = "never-persist-this-token"
  store.setUpFnShortcut()
  let data = try Data(contentsOf: directory.appendingPathComponent("settings.json"))
  let result = try Configuration.decode(data)
  #expect(result.server == saved.server && result.dictionary == saved.dictionary)
  #expect(result.effectiveShortcut == .fn)
  #expect(
    store.config.server == "https://unsaved.example" && store.config.dictionary == ["Unsaved"])
  #expect(!String(decoding: data, as: UTF8.self).contains("never-persist-this-token"))
}
@Test @MainActor func cancelingFnSetupKeepsTheExistingShortcutAfterPermissionChanges() throws {
  var saved = Configuration()
  saved.effectiveShortcut = .controlOptionSpace
  let directory = try setupDirectory(saved)
  defer { try? FileManager.default.removeItem(at: directory) }
  var trusted = false
  let store = Store(
    directory: directory, registerHotkey: false, keyboardAccess: { trusted },
    keyboardAccessRequest: {}, settingsOpener: { _ in })
  store.setUpFnShortcut()
  store.cancelFnSetup()
  trusted = true
  store.refreshShortcutAccess()
  #expect(!store.fnSetupPending && store.config.effectiveShortcut == .controlOptionSpace)
  #expect(
    try Configuration.decode(Data(contentsOf: directory.appendingPathComponent("settings.json")))
      == saved)
}
@Test @MainActor func fnSetupDoesNotInterruptRecordingOrShortcutCapture() throws {
  let directory = try setupDirectory(Configuration())
  defer { try? FileManager.default.removeItem(at: directory) }
  let store = Store(
    directory: directory, registerHotkey: false, keyboardAccess: { false },
    keyboardAccessRequest: { Issue.record("Busy app requested permission") },
    settingsOpener: { _ in Issue.record("Busy app opened Settings") })
  store.state = .recording
  store.setUpFnShortcut()
  #expect(!store.fnSetupPending)
  store.state = .idle
  store.captureShortcut(true)
  store.setUpFnShortcut()
  #expect(!store.fnSetupPending)
  store.captureShortcut(false)
}
