// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import DictationCore
import Foundation
import Testing

@testable import DictationApp

@Test func newInstallUsesBundledRuntimeButLegacyServerSettingsStayExternal() throws {
  let fresh = Configuration()
  #expect(fresh.usesManagedRuntime)
  var encoded =
    try JSONSerialization.jsonObject(with: JSONEncoder().encode(fresh)) as! [String: Any]
  encoded.removeValue(forKey: "connectionMode")
  encoded["server"] = "https://my-antfly.example"
  let legacy = try Configuration.decode(JSONSerialization.data(withJSONObject: encoded))
  #expect(!legacy.usesManagedRuntime)
  #expect(legacy.server == "https://my-antfly.example")
  #expect(try Configuration.decode(JSONEncoder().encode(fresh)).usesManagedRuntime)
}

@MainActor @Test func missingManagedCredentialNeverResetsExistingData() async throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  let data = root.appendingPathComponent("Runtime/data")
  try FileManager.default.createDirectory(at: data, withIntermediateDirectories: true)
  let sentinel = data.appendingPathComponent("preserved")
  try Data("existing history".utf8).write(to: sentinel)
  defer { try? FileManager.default.removeItem(at: root) }
  var writes = 0
  let runtime = ManagedRuntime(
    directory: root, runtime: URL(fileURLWithPath: "/usr/bin/false"),
    launcher: URL(fileURLWithPath: "/usr/bin/false"), credentialRead: { "" },
    credentialWrite: { _ in writes += 1 })
  runtime.start()
  let deadline = ContinuousClock.now.advanced(by: .seconds(3))
  while runtime.busy && ContinuousClock.now < deadline {
    try await Task.sleep(for: .milliseconds(10))
  }
  #expect(!runtime.ready)
  #expect(runtime.status.contains("Keychain credential is unavailable"))
  #expect(writes == 0)
  #expect(try Data(contentsOf: sentinel) == Data("existing history".utf8))
  await runtime.stop()
}

@MainActor @Test func managedCredentialStorageFailureStopsBeforeLaunchingAnEngine() async throws {
  let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  defer { try? FileManager.default.removeItem(at: root) }
  let runtime = ManagedRuntime(
    directory: root, runtime: URL(fileURLWithPath: "/usr/bin/false"),
    launcher: URL(fileURLWithPath: "/usr/bin/false"), credentialRead: { "" },
    credentialWrite: { secret in
      #expect(Data(base64Encoded: secret)?.count == 32)
      throw DictationError.invalid("Synthetic locked Keychain")
    })
  runtime.start()
  let deadline = ContinuousClock.now.advanced(by: .seconds(3))
  while runtime.busy && ContinuousClock.now < deadline {
    try await Task.sleep(for: .milliseconds(10))
  }
  #expect(!runtime.ready)
  #expect(runtime.status == "Synthetic locked Keychain")
  #expect(
    !FileManager.default.fileExists(atPath: root.appendingPathComponent("Runtime/antfly.json").path)
  )
  await runtime.stop()
}
