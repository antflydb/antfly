// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

@MainActor @Test func managedCleanupRetryKeepsHistoryOnLocalEngine() async throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: root) }
  var config = Configuration()
  config.connectionMode = .managed
  config.server = "https://previous-external.example"
  config.retainHistory = true
  config.presets[0].cleanupModel = "generators/cleanup"
  try JSONEncoder().encode(config).write(to: root.appendingPathComponent("settings.json"))
  let settings = URLSessionConfiguration.ephemeral
  settings.protocolClasses = [StoreProtocol.self]
  let client = try AntflyClient(
    server: ManagedRuntime.origin, session: URLSession(configuration: settings))
  let store = Store(
    directory: root, registerHotkey: false, historyUpload: { _, _ in },
    clientFactory: { _ in client }, credentialReader: { _ in "" }, credentialWriter: { _, _ in })
  store.preview = "raw"
  store.canRecoverRaw = true
  store.retryCleanup()
  await store.requestTask?.value
  await store.historyPersistenceTask?.value
  #expect(store.result == "Clean.")
  #expect(store.history.first?.destination?.server == ManagedRuntime.origin)
  await store.prepareToQuit()
}
