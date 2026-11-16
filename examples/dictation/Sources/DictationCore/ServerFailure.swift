// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Preserve the server's diagnostic without retaining unbounded error pages.
public struct ServerFailure: LocalizedError, Sendable {
  public static let maxBodyBytes = 8192
  public let status: Int
  public let message: String
  public let code: String?
  public let retryAfter: String?
  init(response: URLResponse, status: Int? = nil, body: Data) {
    let http = response as? HTTPURLResponse
    self.status = status ?? http?.statusCode ?? 0
    retryAfter = http?.value(forHTTPHeaderField: "Retry-After")
    let bounded = Data(body.prefix(Self.maxBodyBytes))
    let object = (try? JSONSerialization.jsonObject(with: bounded)) as? [String: Any]
    let nested = object?["error"] as? [String: Any]
    code = nested?["code"] as? String ?? object?["code"] as? String
    message = String(
      (nested?["message"] as? String ?? object?["message"] as? String
        ?? object?["error"] as? String ?? String(decoding: bounded, as: UTF8.self))
        .trimmingCharacters(in: .whitespacesAndNewlines).prefix(2048))
  }
  public var errorDescription: String? {
    if message.contains("AppleSpeechAssetsMissing") || code == "AppleSpeechAssetsMissing" {
      return
        "Apple speech needs language assets on the Mac running Antfly. For the bundled engine, enable Apple speech downloads in Settings → Speech & text, then try dictation again. For an external server, ask its administrator to enable download_assets."
    }
    var text = "Antfly HTTP \(status)" + (code.map { " (\($0))" } ?? "")
    if !message.isEmpty { text += ": " + message }
    switch status {
    case 401: text += " Check the saved bearer token."
    case 403: text += " Check server permissions and public inference enablement."
    case 429: text += " The server is busy; retry after capacity becomes available."
    case 503: text += " Check model readiness on the server."
    default: break
    }
    if let retryAfter { text += " Retry-After: \(retryAfter)." }
    return text
  }
}
