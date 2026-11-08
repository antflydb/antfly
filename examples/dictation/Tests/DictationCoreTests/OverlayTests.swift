// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import SwiftUI
import Testing

@testable import DictationApp

@MainActor private final class OverlayProbe {
  var overlay: RecordingOverlay?
}
@MainActor private final class OverlayOwner: ObservableObject {
  let overlay: RecordingOverlay
  init(probe: OverlayProbe) {
    overlay = RecordingOverlay(cancel: {}, copy: {}, insert: {}, retry: {})
    probe.overlay = overlay
  }
}
private struct OverlayStartupHost: View {
  @StateObject private var owner: OverlayOwner
  init(probe: OverlayProbe) {
    _owner = StateObject(wrappedValue: OverlayOwner(probe: probe))
  }
  var body: some View { Text(owner.overlay.hasNativeWindow ? "Created" : "Deferred") }
}

@Test @MainActor func overlayInitializationDoesNotReenterTheStateObjectViewGraph() {
  let probe = OverlayProbe()
  let host = NSHostingView(rootView: OverlayStartupHost(probe: probe))
  host.frame = NSRect(x: 0, y: 0, width: 400, height: 100)
  host.layoutSubtreeIfNeeded()
  #expect(probe.overlay != nil)
  #expect(probe.overlay?.hasNativeWindow == false)
}
@Test @MainActor func hidingQueuedFeedbackDoesNotCreateOrReopenAnOverlay() async {
  let overlay = RecordingOverlay(cancel: {}, copy: {}, insert: {}, retry: {})
  overlay.show(phase: .listening, raw: "hello", draft: "", message: "", note: "", recover: false)
  #expect(!overlay.hasNativeWindow)
  overlay.hide()
  await withCheckedContinuation { continuation in
    DispatchQueue.main.async { continuation.resume() }
  }
  #expect(!overlay.hasNativeWindow)
}
