// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// Release metadata is embedded in the signed bundle, never in imported settings.
public struct UpdateConfiguration: Sendable {
  public let stableFeed: URL
  public let betaFeed: URL
  public let initialBeta: Bool
  public init?(info: [String: Any], installed: Bool) {
    guard installed, info["AntflyUpdatesEnabled"] as? Bool == true,
      let key = info["SUPublicEDKey"] as? String,
      let decoded = Data(base64Encoded: key), decoded.count == 32,
      decoded.contains(where: { $0 != 0 }),
      let stable = Self.feed(info["SUFeedURL"]),
      let beta = Self.feed(info["AntflyBetaFeedURL"])
    else { return nil }
    stableFeed = stable
    betaFeed = beta
    initialBeta = info["AntflyReleaseChannel"] as? String == "beta"
  }
  private static func feed(_ value: Any?) -> URL? {
    guard let string = value as? String, let parts = URLComponents(string: string),
      parts.scheme == "https", parts.host != nil, parts.user == nil, parts.password == nil,
      parts.query == nil, parts.fragment == nil, let url = parts.url
    else { return nil }
    return url
  }
}
