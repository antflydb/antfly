// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

private func releaseInfo() -> [String: Any] {
  [
    "AntflyUpdatesEnabled": true,
    "SUPublicEDKey": Data(repeating: 1, count: 32).base64EncodedString(),
    "SUFeedURL": "https://updates.example/stable.xml",
    "AntflyBetaFeedURL": "https://updates.example/beta.xml",
  ]
}
@Test func updatesRequireAnInstalledReleaseWithRealSigningMetadata() {
  #expect(UpdateConfiguration(info: releaseInfo(), installed: true) != nil)
  #expect(UpdateConfiguration(info: releaseInfo(), installed: false) == nil)
  for (key, value) in [
    ("AntflyUpdatesEnabled", false as Any),
    ("SUPublicEDKey", Data(repeating: 0, count: 32).base64EncodedString()),
    ("SUPublicEDKey", "placeholder"),
    ("SUFeedURL", "http://updates.example/stable.xml"),
    ("AntflyBetaFeedURL", "https://user:password@updates.example/beta.xml"),
    ("SUFeedURL", "https://updates.example/stable.xml?token=secret"),
  ] {
    var info = releaseInfo()
    info[key] = value
    #expect(UpdateConfiguration(info: info, installed: true) == nil)
  }
}
@MainActor private final class UpdateShortcut: ShortcutManaging {
  var changed: ((Bool) -> Void)?
  var interrupted: (() -> Void)?
  var isAvailable = false
  func register(_ shortcut: KeyboardShortcut) throws { isAvailable = true }
  func unregister() { isAvailable = false }
  func setSuspended(_ value: Bool) {}
}
private actor UpdateSignal {
  var sent = false
  private var waiters: [CheckedContinuation<Void, Never>] = []
  func send() {
    sent = true
    let pending = waiters
    waiters = []
    for waiter in pending { waiter.resume() }
  }
  func wait() async { if !sent { await withCheckedContinuation { waiters.append($0) } } }
}
@Test(.timeLimit(.minutes(1))) @MainActor
func updateInstallationWaitsForIdleAndClipboardRestoration() async throws {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: directory) }
  let restoring = UpdateSignal()
  let release = UpdateSignal()
  let installed = UpdateSignal()
  let shortcut = UpdateShortcut()
  let store = Store(
    directory: directory, registerHotkey: false, shortcutManager: shortcut,
    clipboardRestoration: {
      await restoring.send()
      await release.wait()
    },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  let updates = AppUpdates()
  updates.start(store: store)
  #expect(!updates.enabled && !updates.canCheck)  // Test/development bundles never contact a feed.
  store.state = .processing
  updates.postponeInstallation { Task { await installed.send() } }
  for _ in 0..<20 { await Task.yield() }
  #expect(await restoring.sent == false)
  #expect(await installed.sent == false)
  store.state = .idle
  store.capturingShortcut = true
  for _ in 0..<20 { await Task.yield() }
  #expect(await restoring.sent == false)
  store.capturingShortcut = false
  await restoring.wait()
  #expect(store.working)
  #expect(await installed.sent == false)
  await release.send()
  await installed.wait()
  #expect(store.state == .idle && store.working)
  #expect(!shortcut.isAvailable)
  await updates.abortInstallation(NSError(domain: "fixture", code: 1))?.value
  #expect(!store.working && shortcut.isAvailable)
}
@Test @MainActor func betaPreferenceIsLocalAndDoesNotChangeDictationSettings() {
  let suite = "dictation-update-tests-" + UUID().uuidString
  let defaults = UserDefaults(suiteName: suite)!
  defer { defaults.removePersistentDomain(forName: suite) }
  let updates = AppUpdates(defaults: defaults)
  #expect(!updates.includeBeta)
  updates.includeBeta = true
  #expect(AppUpdates(defaults: defaults).includeBeta)
}

@Test(.timeLimit(.minutes(1))) @MainActor
func canceledUpdateCannotInvokeAStaleInstallHandlerOrLeaveTheAppDisabled() async throws {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: directory) }
  let restoring = UpdateSignal()
  let release = UpdateSignal()
  let store = Store(
    directory: directory, registerHotkey: false,
    clipboardRestoration: {
      await restoring.send()
      await release.wait()
    },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  let updates = AppUpdates()
  updates.start(store: store)
  var installed = false
  updates.postponeInstallation { installed = true }
  await restoring.wait()
  let recovery = updates.abortInstallation(CancellationError())
  #expect(store.working && !installed)
  await release.send()
  await recovery?.value
  #expect(!store.working && !installed)
  #expect(updates.status.contains("Update unavailable"))
}

@Test @MainActor func canceledPendingUpdateDoesNotWaitForANewRecordingToFinish() async throws {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: directory) }
  let store = Store(
    directory: directory, registerHotkey: false,
    clipboardRestoration: { Issue.record("Canceled pending update reached shutdown") },
    credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  let updates = AppUpdates()
  updates.start(store: store)
  store.state = .processing
  updates.postponeInstallation { Issue.record("Canceled update installed") }
  await updates.abortInstallation(CancellationError())?.value
  store.state = .idle
  for _ in 0..<20 { await Task.yield() }
  #expect(!store.working)
}
