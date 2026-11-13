// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private final class WriteGate: @unchecked Sendable {
  private let lock = NSLock()
  private let release = DispatchSemaphore(value: 0)
  private var entered = false
  private var onMain = false
  func block() {
    lock.withLock {
      entered = true
      onMain = Thread.isMainThread
    }
    release.wait()
  }
  func unblock() { release.signal() }
  var calledOnMain: Bool { lock.withLock { onMain } }
  func waitUntilEntered() async throws {
    while !lock.withLock({ entered }) { try await Task.sleep(for: .milliseconds(5)) }
  }
}
private func persistenceDirectory() throws -> URL {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
  return root
}

@Suite(.serialized) struct PersistenceTests {
  @Test(.timeLimit(.minutes(1))) @MainActor
  func durableSaveRunsOffMainAndQuitDrainsItsKnownOutcome()
    async throws
  {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    let gate = WriteGate()
    defer { gate.unblock() }
    let writer = AtomicFileWriter(ownedDirectory: root, beforeWrite: { gate.block() })
    let store = Store(directory: root, registerHotkey: false, settingsWriter: writer)
    store.config.dictionary = ["Saved during quit"]
    store.save()
    try await gate.waitUntilEntered()
    #expect(store.savingSettings && !gate.calledOnMain)
    // The main actor remains responsive while the filesystem worker is blocked.
    store.message = "UI is responsive"
    #expect(store.message == "UI is responsive")
    store.settingsTask?.cancel()  // An approved write must still apply its disk outcome.
    var quitDone = false
    let quit = Task {
      await store.prepareToQuit()
      quitDone = true
    }
    await Task.yield()
    #expect(!quitDone)
    gate.unblock()
    await quit.value
    let saved = try Configuration.decode(
      Data(contentsOf: root.appendingPathComponent("settings.json")))
    #expect(saved.dictionary == ["Saved during quit"])
    #expect(store.config == saved && !store.savingSettings)
  }

  @Test(.timeLimit(.minutes(1))) @MainActor
  func slowExportDoesNotBlockSettingsOrRecordingAdmissionAndIsDrainedOnQuit() async throws {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    let exports = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: exports) }
    let gate = WriteGate()
    defer { gate.unblock() }
    let writer = AtomicFileWriter(ownedDirectory: root, beforeWrite: { gate.block() })
    let store = Store(directory: root, registerHotkey: false, exportWriter: writer)
    let destination = exports.appendingPathComponent("transcript.txt")
    store.exportSnapshot(Data("Captured export".utf8), to: destination)
    try await gate.waitUntilEntered()
    #expect(store.exporting && !store.isBusy && !gate.calledOnMain)
    store.config.dictionary = ["Independent settings write"]
    store.save()
    await store.settingsTask?.value
    #expect(
      try Configuration.decode(Data(contentsOf: root.appendingPathComponent("settings.json")))
        .dictionary == store.config.dictionary)
    var quitDone = false
    let quit = Task {
      await store.prepareToQuit()
      quitDone = true
    }
    await Task.yield()
    #expect(!quitDone)
    gate.unblock()
    await quit.value
    #expect(try String(contentsOf: destination, encoding: .utf8) == "Captured export")
    #expect(!store.exporting)
  }

  @Test func recoveryOnlyRemovesOwnedRegularStagingFiles() async throws {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    let foreign = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: foreign) }
    let stale = root.appendingPathComponent(".dictation-settings-\(UUID().uuidString).tmp")
    let malformed = root.appendingPathComponent(".dictation-settings-not-a-uuid.tmp")
    let directory = root.appendingPathComponent(".dictation-settings-\(UUID().uuidString).tmp")
    let symlink = root.appendingPathComponent(".dictation-settings-\(UUID().uuidString).tmp")
    let external = foreign.appendingPathComponent(".dictation-settings-\(UUID().uuidString).tmp")
    let settings = root.appendingPathComponent("settings.json")
    try Data("stale".utf8).write(to: stale)
    try Data("keep".utf8).write(to: malformed)
    try Data("foreign".utf8).write(to: external)
    try Data("settings".utf8).write(to: settings)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false)
    try FileManager.default.createSymbolicLink(at: symlink, withDestinationURL: external)
    try await AtomicFileWriter(ownedDirectory: root).recoverAbandonedWrites()
    #expect(!FileManager.default.fileExists(atPath: stale.path))
    for file in [malformed, directory, symlink, external, settings] {
      #expect(FileManager.default.fileExists(atPath: file.path))
    }
  }

  @Test(.timeLimit(.minutes(1))) @MainActor func recoverySkipsAWriterHoldingTheDirectoryLock()
    async throws
  {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    let gate = WriteGate()
    defer { gate.unblock() }
    let live = root.appendingPathComponent(".dictation-settings-\(UUID().uuidString).tmp")
    let writer = AtomicFileWriter(
      ownedDirectory: root,
      beforeWrite: {
        try Data("live".utf8).write(to: live)
        gate.block()
      })
    let write = Task {
      try await writer.write(
        Data("committed".utf8), to: root.appendingPathComponent("settings.json"))
    }
    try await gate.waitUntilEntered()
    let cleaner = AtomicFileWriter(ownedDirectory: root)
    try await cleaner.recoverAbandonedWrites()
    #expect(FileManager.default.fileExists(atPath: live.path))
    gate.unblock()
    try await write.value
    try await cleaner.recoverAbandonedWrites()
    #expect(!FileManager.default.fileExists(atPath: live.path))
    #expect(
      try String(contentsOf: root.appendingPathComponent("settings.json"), encoding: .utf8)
        == "committed")
  }

  @Test @MainActor func storeStartupRecoversAbandonedWrites() async throws {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    let stale = root.appendingPathComponent(".dictation-settings-\(UUID().uuidString).tmp")
    try Data("old configuration".utf8).write(to: stale)
    let store = Store(directory: root, registerHotkey: false)
    await store.settingsCleanupTask?.value
    #expect(!FileManager.default.fileExists(atPath: stale.path))
    await store.prepareToQuit()
  }

  @MainActor private final class MutableShortcut: ShortcutManaging {
    var changed: ((Bool) -> Void)?
    var interrupted: (() -> Void)?
    var isAvailable = true
    func register(_ shortcut: KeyboardShortcut) throws { isAvailable = true }
    func unregister() { isAvailable = false }
    func setSuspended(_ value: Bool) {}
  }

  @Test(.timeLimit(.minutes(1))) @MainActor
  func shortcutPermissionLossDuringSaveDoesNotClaimAnActiveShortcut() async throws {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    let gate = WriteGate()
    defer { gate.unblock() }
    let writer = AtomicFileWriter(ownedDirectory: root, beforeWrite: { gate.block() })
    let shortcut = MutableShortcut()
    var trusted = true
    let store = Store(
      directory: root, registerHotkey: false, settingsWriter: writer,
      shortcutManager: shortcut, keyboardAccess: { trusted })
    store.setUpShortcut()
    try await gate.waitUntilEntered()
    trusted = false
    shortcut.isAvailable = false
    store.refreshPermissions()
    gate.unblock()
    await store.settingsTask?.value
    #expect(store.shortcutSetupStatus.contains("saved but not active"))
    #expect(!store.shortcutReady)
    #expect(
      try Configuration.decode(Data(contentsOf: root.appendingPathComponent("settings.json")))
        .effectiveShortcut == store.config.effectiveShortcut)
    await store.prepareToQuit()
  }

  @Test @MainActor func importingAnOriginNeverForwardsThePreviousOriginsUnsavedCredential()
    async throws
  {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    var writes: [(String, String)] = []
    let store = Store(
      directory: root, registerHotkey: false,
      credentialReader: { origin in
        origin == "https://new.example" ? "new-origin-saved-token" : "old-origin-saved-token"
      }, credentialWriter: { token, origin in writes.append((token, origin)) })
    store.config.connectionMode = .external
    store.config.server = "https://old.example"
    store.token = "old-origin-unsaved-token"
    var imported = Configuration()
    imported.connectionMode = .external
    imported.server = "https://new.example"
    let file = root.appendingPathComponent("import.json")
    try JSONEncoder().encode(imported).write(to: file)
    store.importConfiguration(from: file)
    await store.settingsTask?.value
    #expect(writes.count == 1)
    #expect(writes.first?.0 == "new-origin-saved-token" && writes.first?.1 == "https://new.example")
    #expect(store.config.server == "https://new.example")
    await store.prepareToQuit()
  }

  @Test(.timeLimit(.minutes(1))) @MainActor
  func completedInstallationFinishesItsOwnedSaveBeforeReleasingSetup() async throws {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    let gate = WriteGate()
    defer { gate.unblock() }
    let writer = AtomicFileWriter(ownedDirectory: root, beforeWrite: { gate.block() })
    let store = Store(directory: root, registerHotkey: false, settingsWriter: writer)
    var candidate = store.config
    candidate.dictionary = ["Installed and committed"]
    let result = ModelSetupResult(operation: .whisper)
    store.runModelSetup(.whisper) { PreparedModelSetup(configuration: candidate, result: result) }
    try await gate.waitUntilEntered()
    #expect(store.savingSettings && !store.canCancelModelSetup && !store.hasPendingSetupCommit)
    store.cancelModelSetup()  // The final commit has a known outcome, not an interruptible download.
    #expect(store.settingUpModel)
    gate.unblock()
    await store.modelSetupTask?.value
    #expect(store.modelSetup == .ready(result) && store.config == candidate)
    #expect(!store.savingSettings)
    await store.prepareToQuit()
  }

  @Test @MainActor func importedProcessingLocationIsReconciledWithoutAVisibleSettingsPicker()
    async throws
  {
    let root = try persistenceDirectory()
    defer { try? FileManager.default.removeItem(at: root) }
    let store = Store(directory: root, registerHotkey: false)
    store.config.connectionMode = .external
    let imported = Configuration()  // Managed mode, even without mounting the SwiftUI settings form.
    let file = root.appendingPathComponent("import.json")
    try JSONEncoder().encode(imported).write(to: file)
    store.importConfiguration(from: file)
    await store.settingsTask?.value
    #expect(store.config.usesManagedRuntime && store.runtimeModeTask != nil)
    await store.runtimeModeTask?.value
    await store.prepareToQuit()
  }

}
