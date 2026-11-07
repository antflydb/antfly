// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import DictationCore
import SwiftUI

private final class FeedbackPanel: NSPanel {
  override var canBecomeKey: Bool { false }
  override var canBecomeMain: Bool { false }
}
@MainActor final class RecordingOverlay {
  private let panel: FeedbackPanel
  private let hosting: NSHostingView<FeedbackView>
  private let cancel: () -> Void
  private let copy: () -> Void
  private let insert: () -> Void
  private let retry: () -> Void
  init(
    cancel: @escaping () -> Void, copy: @escaping () -> Void, insert: @escaping () -> Void,
    retry: @escaping () -> Void
  ) {
    self.cancel = cancel
    self.copy = copy
    self.insert = insert
    self.retry = retry
    hosting = NSHostingView(
      rootView: FeedbackView(
        phase: .idle, raw: "", draft: "", message: "", note: "", recover: false, cancel: cancel,
        copy: copy, insert: insert, retry: retry))
    panel = FeedbackPanel(
      contentRect: NSRect(x: 0, y: 0, width: 420, height: 180),
      styleMask: [.borderless, .nonactivatingPanel], backing: .buffered, defer: false)
    panel.level = .floating
    panel.collectionBehavior = [.canJoinAllSpaces, .fullScreenAuxiliary]
    panel.hidesOnDeactivate = false
    panel.isReleasedWhenClosed = false
    panel.isOpaque = false
    panel.backgroundColor = .clear
    panel.hasShadow = true
    panel.contentView = hosting
  }
  func hide() { panel.orderOut(nil) }
  func show(
    phase: DictationPhase, raw: String, draft: String, message: String, note: String, recover: Bool
  ) {
    guard phase != .idle && phase != .canceled else {
      hide()
      return
    }
    hosting.rootView = FeedbackView(
      phase: phase, raw: raw, draft: draft, message: message, note: note,
      recover: recover, cancel: cancel, copy: copy, insert: insert, retry: retry)
    hosting.layoutSubtreeIfNeeded()
    panel.setContentSize(NSSize(width: 420, height: max(80, min(300, hosting.fittingSize.height))))
    if let screen = NSWorkspace.shared.frontmostApplication.flatMap({ _ in NSScreen.main })
      ?? NSScreen.screens.first
    {
      let area = screen.visibleFrame
      panel.setFrameOrigin(NSPoint(x: area.midX - 210, y: area.minY + 32))
    }
    panel.orderFrontRegardless()
  }
}
private struct FeedbackView: View {
  let phase: DictationPhase
  let raw: String
  let draft: String
  let message: String
  let note: String
  let recover: Bool
  let cancel: () -> Void
  let copy: () -> Void
  let insert: () -> Void
  let retry: () -> Void
  var body: some View {
    VStack(alignment: .leading, spacing: 8) {
      HStack {
        Image(
          systemName: phase == .listening
            ? "mic.fill" : phase == .failed ? "exclamationmark.circle" : "waveform"
        )
        .foregroundStyle(phase == .listening ? Color.red : Color.primary)
        Text(phase.label).font(.headline)
        Spacer()
        Button(
          phase == .failed || phase == .ready || phase == .noSpeech ? "Dismiss" : "Cancel",
          action: cancel)
      }
      if !note.isEmpty { Text(note).font(.caption).lineLimit(2) }
      if phase == .failed || phase == .ready { Text(message).font(.caption).lineLimit(2) }
      if !raw.isEmpty { Text(raw).font(.caption).lineLimit(3) }
      if !draft.isEmpty {
        Text("Cleanup preview: " + draft).lineLimit(2).foregroundStyle(.secondary)
      }
      if phase == .ready && !raw.isEmpty { Button("Copy", action: copy) }
      if recover {
        HStack {
          Button("Insert original", action: insert)
          Button("Copy original", action: copy)
          Button("Retry cleanup", action: retry)
        }
      }
    }.padding(16).frame(width: 388, alignment: .leading)
      .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 16))
      .accessibilityElement(children: .contain)
      .accessibilityLabel(phase.label)
  }
}
