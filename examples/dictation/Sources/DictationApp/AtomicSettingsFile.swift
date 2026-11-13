// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Darwin
import DictationCore
import Foundation

/// All fallible preparation occurs before rename publishes the new settings.
/// A failed commit leaves the previous file intact and removes its staging file.
enum AtomicSettingsFile {
  static func write(_ data: Data, to destination: URL) throws {
    let staging = destination.deletingLastPathComponent()
      .appendingPathComponent(".dictation-settings-\(UUID().uuidString).tmp")
    let descriptor = open(staging.path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, mode_t(0o600))
    guard descriptor >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
    let handle = FileHandle(fileDescriptor: descriptor, closeOnDealloc: true)
    defer {
      try? handle.close()
      try? FileManager.default.removeItem(at: staging)
    }
    try handle.write(contentsOf: data)
    try handle.synchronize()
    try handle.close()
    guard rename(staging.path, destination.path) == 0 else {
      throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
  }
}

/// Separate instances keep exports independent from preference writes. The
/// directory lock coordinates recovery and writes across app instances too.
final class AtomicFileWriter: @unchecked Sendable {
  let ownedDirectory: URL
  private let beforeWrite: @Sendable () throws -> Void
  private let queue = DispatchQueue(
    label: "io.antfly.dictation.files." + UUID().uuidString, qos: .utility)
  init(ownedDirectory: URL, beforeWrite: @escaping @Sendable () throws -> Void = {}) {
    self.ownedDirectory = ownedDirectory.standardizedFileURL
    self.beforeWrite = beforeWrite
  }
  private func execute<T: Sendable>(_ action: @escaping @Sendable () throws -> T) async throws -> T
  {
    try await withCheckedThrowingContinuation { continuation in
      queue.async {
        do { continuation.resume(returning: try action()) } catch {
          continuation.resume(throwing: error)
        }
      }
    }
  }
  func write(_ data: Data, to destination: URL) async throws {
    try await execute { [self] in
      if destination.deletingLastPathComponent().resolvingSymlinksInPath()
        == ownedDirectory.resolvingSymlinksInPath()
      {
        try withDirectoryLock(nonblocking: false) {
          try beforeWrite()
          try AtomicSettingsFile.write(data, to: destination)
        }
      } else {
        try beforeWrite()
        try AtomicSettingsFile.write(data, to: destination)
      }
    }
  }
  func readConfiguration(at file: URL) async throws -> DictationCore.Configuration {
    try await execute { try DictationCore.Configuration.decode(Data(contentsOf: file)) }
  }
  func readConfigurationIfPresent(at file: URL) async throws -> DictationCore.Configuration? {
    try await execute {
      guard FileManager.default.fileExists(atPath: file.path) else { return nil }
      return try DictationCore.Configuration.decode(Data(contentsOf: file))
    }
  }
  func recoverAbandonedWrites() async throws {
    try await execute { [self] in
      // Every owned-directory writer locks before staging and through publication.
      // Recovery skips other processes' active writes without waiting for them.
      try withDirectoryLock(nonblocking: true) {
        for file in try FileManager.default.contentsOfDirectory(
          at: ownedDirectory, includingPropertiesForKeys: nil)
        {
          let name = file.lastPathComponent
          let prefix = ".dictation-settings-"
          let suffix = ".tmp"
          guard name.hasPrefix(prefix), name.hasSuffix(suffix),
            UUID(uuidString: String(name.dropFirst(prefix.count).dropLast(suffix.count))) != nil
          else { continue }
          var info = stat()
          guard lstat(file.path, &info) == 0, info.st_mode & S_IFMT == S_IFREG else { continue }
          try FileManager.default.removeItem(at: file)
        }
      }
    }
  }
  // Called only on this instance's queue. Immutable fields plus queue confinement
  // let blocking filesystem operations stay out of Swift's cooperative pool.
  private func withDirectoryLock(nonblocking: Bool, _ action: () throws -> Void) throws {
    let file = ownedDirectory.appendingPathComponent(".dictation-settings.lock")
    let descriptor = open(
      file.path, O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, mode_t(0o600))
    guard descriptor >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
    defer { _ = close(descriptor) }
    var info = stat()
    guard fstat(descriptor, &info) == 0, info.st_mode & S_IFMT == S_IFREG else {
      throw POSIXError(.EINVAL)
    }
    guard flock(descriptor, LOCK_EX | (nonblocking ? LOCK_NB : 0)) == 0 else {
      if nonblocking && (errno == EWOULDBLOCK || errno == EAGAIN) { return }
      throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    try action()
  }
}
