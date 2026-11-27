// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationApp

struct InstallationTests {
  @Test func installationLocationAcceptsBothApplicationFoldersOnly() {
    let directories = [
      URL(fileURLWithPath: "/Applications"), URL(fileURLWithPath: "/Users/test/Applications"),
    ]
    for directory in directories {
      #expect(
        InstallationLocation.isInstalled(
          directory.appendingPathComponent("Antfly Dictation.app"), directories: directories))
    }
    for path in [
      "/Users/test/Downloads/Antfly Dictation.app", "/Applications-other/App.app",
      "/Applications/nested/App.app", "/private/var/AppTranslocation/test/d/Antfly Dictation.app",
    ] {
      #expect(
        !InstallationLocation.isInstalled(URL(fileURLWithPath: path), directories: directories))
    }
  }

  private func fixture() throws -> (URL, URL, URL) {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    let source = root.appendingPathComponent("Downloads/Test.app")
    try FileManager.default.createDirectory(at: source, withIntermediateDirectories: true)
    try Data("synthetic app".utf8).write(to: source.appendingPathComponent("data"))
    return (root, source, root.appendingPathComponent("Applications"))
  }

  @Test func unsignedBundleIsRejectedBeforeInstalling() throws {
    let (root, source, directory) = try fixture()
    defer { try? FileManager.default.removeItem(at: root) }
    #expect(throws: (any Error).self) {
      try SignedApplicationInstaller.copy(source: source, directory: directory)
    }
    #expect(!FileManager.default.fileExists(atPath: directory.path))
  }

  @Test func installationPreservesSourceAndValidatesStagedCopy() throws {
    let (root, source, directory) = try fixture()
    defer { try? FileManager.default.removeItem(at: root) }
    try FileManager.default.createSymbolicLink(
      atPath: source.appendingPathComponent("link").path, withDestinationPath: "data")
    var verified: [URL] = []
    let destination = try SignedApplicationInstaller.copy(source: source, directory: directory) {
      verified.append($0)
    }
    #expect(
      try FileManager.default.destinationOfSymbolicLink(
        atPath: destination.appendingPathComponent("link").path) == "data")
    #expect(verified.count == 2)
    #expect(verified.first == source)
    #expect(
      verified.last?.deletingLastPathComponent().lastPathComponent.hasPrefix(".antfly-install-")
        == true)
    #expect(
      try Data(contentsOf: destination.appendingPathComponent("data")) == Data("synthetic app".utf8)
    )
    #expect(FileManager.default.fileExists(atPath: source.path))
    #expect(
      try FileManager.default.contentsOfDirectory(atPath: directory.path) == [
        "Antfly Dictation.app"
      ])
  }

  @Test func existingInstallationIsNeverOverwritten() throws {
    let (root, source, directory) = try fixture()
    defer { try? FileManager.default.removeItem(at: root) }
    let existing = directory.appendingPathComponent("Antfly Dictation.app")
    try FileManager.default.createDirectory(at: existing, withIntermediateDirectories: true)
    try Data("newer installation".utf8).write(to: existing.appendingPathComponent("data"))
    #expect(throws: (any Error).self) {
      try SignedApplicationInstaller.copy(
        source: source, directory: directory, verifyBundle: { _ in })
    }
    #expect(
      try Data(contentsOf: existing.appendingPathComponent("data"))
        == Data("newer installation".utf8))
    #expect(
      try FileManager.default.contentsOfDirectory(atPath: directory.path) == [
        "Antfly Dictation.app"
      ])
  }

  @Test func failedStagedVerificationCleansUpWithoutPublishing() throws {
    let (root, source, directory) = try fixture()
    defer { try? FileManager.default.removeItem(at: root) }
    struct Rejected: Error {}
    #expect(throws: Rejected.self) {
      try SignedApplicationInstaller.copy(source: source, directory: directory) { bundle in
        if bundle != source { throw Rejected() }
      }
    }
    #expect(try FileManager.default.contentsOfDirectory(atPath: directory.path).isEmpty)
    #expect(FileManager.default.fileExists(atPath: source.path))
  }
}
