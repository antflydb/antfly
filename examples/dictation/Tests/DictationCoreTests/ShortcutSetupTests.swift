// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

@MainActor private final class KeyboardTrust {
  var value = false
}

private func setupDirectory(_ config: Configuration) throws -> URL {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  try JSONEncoder().encode(config).write(to: directory.appendingPathComponent("settings.json"))
  return directory
}
@Test @MainActor func shortcutSetupWaitsForPermissionAndAppliesAutomaticallyOnReturn() async throws
{
  var saved = Configuration()
  saved.effectiveShortcut = .controlOptionSpace
  let directory = try setupDirectory(saved)
  defer { try? FileManager.default.removeItem(at: directory) }
  let trusted = KeyboardTrust()
  var prompts = 0
  var panes: [String] = []
  let store = Store(
    directory: directory, registerHotkey: false, keyboardAccess: { trusted.value },
    keyboardAccessRequest: { prompts += 1 }, settingsOpener: { panes.append($0) })
  store.config.effectiveShortcut = .fn
  store.setUpShortcut()
  await store.settingsTask?.value
  #expect(store.shortcutSetupPending && prompts == 1)
  #expect(store.config.effectiveShortcut == .fn)
  #expect(panes == ["com.apple.preference.security?Privacy_Accessibility"])
  let file = directory.appendingPathComponent("settings.json")
  #expect(try Configuration.decode(Data(contentsOf: file)).effectiveShortcut == .controlOptionSpace)
  trusted.value = true
  store.refreshShortcutAccess()
  await store.settingsTask?.value
  #expect(!store.shortcutSetupPending && store.config.effectiveShortcut == .fn)
  #expect(try Configuration.decode(Data(contentsOf: file)).effectiveShortcut == .fn)
  #expect(store.shortcutSetupStatus.contains("enabled and saved"))
}
@Test @MainActor func shortcutSetupSavesOnlyTheShortcutAndDoesNotPersistUnsavedEdits() async throws
{
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
  store.config.effectiveShortcut = .controlOption
  store.token = "never-persist-this-token"
  store.setUpShortcut()
  await store.settingsTask?.value
  let data = try Data(contentsOf: directory.appendingPathComponent("settings.json"))
  let result = try Configuration.decode(data)
  #expect(result.server == saved.server && result.dictionary == saved.dictionary)
  #expect(result.effectiveShortcut == .controlOption)
  #expect(
    store.config.server == "https://unsaved.example" && store.config.dictionary == ["Unsaved"])
  #expect(!String(decoding: data, as: UTF8.self).contains("never-persist-this-token"))
}
@Test @MainActor func cancelingShortcutSetupKeepsTheExistingShortcutAfterPermissionChanges()
  async throws
{
  var saved = Configuration()
  saved.effectiveShortcut = .controlOptionSpace
  let directory = try setupDirectory(saved)
  defer { try? FileManager.default.removeItem(at: directory) }
  let trusted = KeyboardTrust()
  let store = Store(
    directory: directory, registerHotkey: false, keyboardAccess: { trusted.value },
    keyboardAccessRequest: {}, settingsOpener: { _ in })
  store.setUpShortcut()
  await store.settingsTask?.value
  store.cancelShortcutSetup()
  trusted.value = true
  store.refreshShortcutAccess()
  await store.settingsTask?.value
  #expect(!store.shortcutSetupPending && store.config.effectiveShortcut == .controlOptionSpace)
  #expect(
    try Configuration.decode(Data(contentsOf: directory.appendingPathComponent("settings.json")))
      == saved)
}
@Test @MainActor func shortcutSetupDoesNotInterruptRecordingOrShortcutCapture() async throws {
  let directory = try setupDirectory(Configuration())
  defer { try? FileManager.default.removeItem(at: directory) }
  let store = Store(
    directory: directory, registerHotkey: false, keyboardAccess: { false },
    keyboardAccessRequest: { Issue.record("Busy app requested permission") },
    settingsOpener: { _ in Issue.record("Busy app opened Settings") })
  store.state = .recording
  store.setUpShortcut()
  await store.settingsTask?.value
  #expect(!store.shortcutSetupPending)
  store.state = .idle
  store.captureShortcut(true)
  store.setUpShortcut()
  await store.settingsTask?.value
  #expect(!store.shortcutSetupPending)
  store.captureShortcut(false)
}

@Test @MainActor func shortcutOnlySaveUpdatesDirtyBaselineAndKeepsOtherEditsPending() async throws {
  var saved = Configuration()
  saved.effectiveShortcut = .controlOptionSpace
  let directory = try setupDirectory(saved)
  defer { try? FileManager.default.removeItem(at: directory) }
  let store = Store(
    directory: directory, registerHotkey: false, keyboardAccess: { true },
    keyboardAccessRequest: {}, settingsOpener: { _ in })
  #expect(!store.hasUnsavedSettings)
  store.config.effectiveShortcut = .fn
  #expect(store.hasUnsavedSettings)
  store.setUpShortcut()
  await store.settingsTask?.value
  #expect(!store.hasUnsavedSettings)
  store.config.dictionary.append("PendingWord")
  store.config.effectiveShortcut = .controlOption
  store.setUpShortcut()
  await store.settingsTask?.value
  #expect(store.hasUnsavedSettings)
  let disk = try Configuration.decode(
    Data(contentsOf: directory.appendingPathComponent("settings.json")))
  #expect(disk.effectiveShortcut == .controlOption && disk.dictionary == saved.dictionary)
  store.config.dictionary = saved.dictionary
  #expect(!store.hasUnsavedSettings)
  await store.prepareToQuit()
}
