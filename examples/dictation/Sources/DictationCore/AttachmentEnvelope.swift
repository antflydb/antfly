// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// AFATT001: metadata remains small; audio is sent once as binary, without base64.
public enum AttachmentEnvelope {
  public static let contentType = "application/vnd.antfly.attachments.v1"
  public static func audio(metadata: Data, recording: Data, mime: String = "audio/wav") throws
    -> Data
  {
    guard metadata.count <= 1_048_576, recording.count <= 128 * 1024 * 1024,
      !mime.isEmpty, mime.utf8.count <= 1024
    else { throw DictationError.invalid("Audio envelope exceeds its size limit.") }
    var result = Data(capacity: 40 + metadata.count + mime.utf8.count + recording.count)
    func integer<T: FixedWidthInteger>(_ value: T) {
      var little = value.littleEndian
      withUnsafeBytes(of: &little) { result.append(contentsOf: $0) }
    }
    result.append(contentsOf: "AFATT001".utf8)
    integer(UInt64(metadata.count))
    integer(UInt32(1))
    integer(UInt32(0))
    integer(UInt32(mime.utf8.count))
    integer(UInt32(0))
    integer(UInt64(recording.count))
    result.append(metadata)
    result.append(contentsOf: mime.utf8)
    result.append(recording)
    return result
  }
}
