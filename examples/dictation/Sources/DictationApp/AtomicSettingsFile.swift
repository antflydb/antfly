// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Darwin
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
