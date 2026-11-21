// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const $ = (id) => document.getElementById(id);
let me = null,
  cursor = null,
  replyTo = null,
  editorId = null,
  folder = "inbox",
  local = null,
  localOpening = null;
// On-device search is optional and loaded lazily; see local-index.js.
const LOCAL_FLAGS = "antfly-mail-local-v1";
// Minimal identity for opening an opted-in copy offline; removed on sign-out.
const OFFLINE_IDENTITY = "antfly-mail-offline-identity-v1";
// Set while a sign-out wipe is unfinished, so the next load retries it.
const WIPE_PENDING = "antfly-mail-local-wipe-pending-v1";
const localModule = () => import("/local-index.js");
const localSupported = () =>
  typeof Worker !== "undefined" &&
  typeof WebAssembly !== "undefined" &&
  typeof navigator.storage?.getDirectory === "function";
function localFlags() {
  try {
    return JSON.parse(localStorage.getItem(LOCAL_FLAGS) ?? "{}") ?? {};
  } catch {
    return {};
  }
}
// Sign-out on a shared machine removes every on-device mailbox copy. This needs
// no runtime download, so it works even when the local module cannot load.
async function wipeLocal() {
  // Never allow offline reopening after sign-out, even if deletion fails.
  try {
    localStorage.removeItem(OFFLINE_IDENTITY);
    localStorage.setItem(WIPE_PENDING, "1");
  } catch {
    /* Storage disabled: nothing was ever stored. */
  }
  const current = local;
  local = null;
  localReady(null);
  // The open copy's worker holds exclusive OPFS handles: have it release them
  // and delete every copy before the worker is stopped.
  if (current) {
    try {
      await current.wipeAll();
    } finally {
      await current.close();
    }
  } else if (typeof navigator.storage?.getDirectory === "function") {
    try {
      await (await navigator.storage.getDirectory()).removeEntry("antfly-mail-local", { recursive: true });
    } catch (error) {
      if (error?.name !== "NotFoundError") throw error;
    }
  }
  // Forget the opt-in only once the data is gone, so a failed wipe is retried.
  try {
    localStorage.removeItem(LOCAL_FLAGS);
    localStorage.removeItem(WIPE_PENDING);
  } catch {
    /* Storage disabled: nothing was ever stored. */
  }
}
function status(text, error = false) {
  $("status").textContent = text;
  $("status").classList.toggle("error", error);
}
async function api(path, body, method = body ? "POST" : "GET") {
  const result = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const value = await result.json();
  if (!result.ok) throw Error(value.message ?? value.error ?? "Request failed");
  return value;
}
function guarded(fn) {
  return async (event) => {
    event?.preventDefault();
    try {
      await fn(event);
    } catch (error) {
      status(error.message, true);
    }
  };
}
function text(tag, value) {
  const element = document.createElement(tag);
  element.textContent = value ?? "";
  return element;
}
async function load(path = "/mail/messages") {
  folder = path.startsWith("/mail/outbox") ? "outbox" : "inbox";
  const result = await api(path);
  cursor = result.cursor;
  render(result.messages);
}
function render(messages) {
  $("next").hidden = !cursor;
  $("messages").replaceChildren();
  for (const message of messages) {
    const button = document.createElement("button");
    button.className = "mail-row";
    button.append(
      text("strong", message.subject || "(No subject)"),
      text("small", message.from),
      text(
        "small",
        (message.state ? message.state + " · " : "") +
          new Date(message.received_at ?? message.created_at).toLocaleString()
      )
    );
    if (message.local) {
      const badge = text("small", "Found on this device");
      badge.className = "local";
      button.append(badge);
    }
    button.onclick = guarded(() =>
      folder === "outbox" ? showOutbound(message) : openMessage(message.id)
    );
    $("messages").append(button);
  }
  if (!messages.length) $("messages").append(text("p", "No messages."));
}
function showOutbound(message) {
  $("message").replaceChildren(
    text("h2", message.subject || "(No subject)"),
    text("p", "To: " + message.to),
    text("p", "Status: " + message.state),
    text("pre", message.text)
  );
  if (message.state === "unknown")
    $("message").append(
      text("p", "Submission outcome is unknown. This message will not be resent automatically.")
    );
}
async function openMessage(id) {
  let message;
  try {
    message = await api("/mail/messages/" + encodeURIComponent(id));
  } catch (error) {
    // Offline: read the on-device copy when there is one.
    message = local ? await local.message(id) : null;
    if (!message) throw error;
    message.offline = true;
  }
  const article = $("message");
  article.replaceChildren(
    text("h2", message.subject || "(No subject)"),
    text("p", "From: " + message.from),
    text("p", "To: " + message.to),
    text("pre", message.text || "(No plain-text content)")
  );
  for (const part of message.attachments ?? []) {
    const link = text("a", part.filename + " (" + part.size + " bytes)");
    link.href = "/mail/messages/" + id + "/attachments/" + part.index;
    link.download = part.filename;
    article.append(link, document.createElement("br"));
  }
  if (message.html_available)
    article.append(text("small", "This message also contains HTML. Only plain text is displayed."));
  if (message.offline)
    article.append(
      text("small", "Shown from this device's copy" + (message.text_truncated ? " (long text shortened)." : "."))
    );
  const reply = text("button", "Reply");
  reply.onclick = () => edit(message);
  article.append(document.createElement("br"), reply);
  // Deleting needs the server; an offline copy is read-only.
  if (!message.offline) {
    const remove = text("button", "Delete");
    remove.className = "danger";
    remove.onclick = guarded(() => deleteMessage(id));
    article.append(remove);
  }
}
async function deleteMessage(id) {
  if (!confirm("Delete this message? It is removed from your mailbox, its attachments and every device's copy."))
    return;
  await api("/mail/messages/" + encodeURIComponent(id), null, "DELETE");
  // The next sync also carries the deletion; drop the local copy right away.
  if (local)
    await local
      .forget([id])
      .then(() => localReady(local?.state))
      .catch(() => {});
  $("message").replaceChildren(text("p", "Message deleted."));
  status("Message deleted.");
  await load();
}
function edit(message) {
  replyTo = message?.id ?? null;
  editorId = crypto.randomUUID();
  $("editor").reset();
  $("editor").hidden = false;
  $("editor").elements.to.value = message?.from ?? "";
  $("editor").elements.to.readOnly = !!message;
  $("editor").elements.subject.value = message
    ? message.subject.startsWith("Re:")
      ? message.subject
      : "Re: " + message.subject
    : "";
  $("editor").elements.text.focus();
}
function offlineIdentity() {
  try {
    const saved = JSON.parse(localStorage.getItem(OFFLINE_IDENTITY) ?? "null");
    return saved && localFlags()[saved.mailbox?.id] === true ? saved : null;
  } catch {
    return null;
  }
}
function rememberIdentity() {
  try {
    if (localFlags()[me.mailbox.id] === true)
      localStorage.setItem(
        OFFLINE_IDENTITY,
        JSON.stringify({ user: { email: me.user.email }, mailbox: { id: me.mailbox.id, address: me.mailbox.address } })
      );
  } catch {
    /* Offline reopening is a convenience. */
  }
}
async function refresh() {
  let offline = false;
  try {
    me = await api("/api/me");
  } catch (error) {
    // A network failure (not a 401) with an opted-in copy opens it offline.
    me = error instanceof TypeError ? offlineIdentity() : null;
    offline = !!me;
  }
  // A sign-out left a copy on disk (its wipe failed, e.g. another tab held it): retry.
  let wipePending = false;
  try {
    wipePending = localStorage.getItem(WIPE_PENDING) === "1";
  } catch {
    /* Storage disabled: nothing was ever stored. */
  }
  // Finish it before anything can reopen the copy, whoever signs in next.
  if (!offline && wipePending)
    await wipeLocal().catch(() => {
      /* Retried on the next load or sign-out. */
    });
  $("account").hidden = !!me;
  $("mail").hidden = !me;
  $("logout").hidden = !me;
  $("identity").textContent = me?.user.email ?? "";
  if (me) {
    $("address").textContent = "Your mailbox: " + me.mailbox.address;
    $("local").hidden = !localSupported();
    let stillPending = false;
    try {
      stillPending = localStorage.getItem(WIPE_PENDING) === "1";
    } catch {
      /* Storage disabled. */
    }
    $("local-toggle").checked = !stillPending && localFlags()[me.mailbox.id] === true;
    if (stillPending) localStatus("A previous sign-out's on-device copy is still being removed. Close other tabs of this app.");
    if ($("local-toggle").checked) {
      rememberIdentity();
      startLocal(false);
    }
    if (offline) {
      status("You are offline. Searching this device's copy of your mailbox.");
      $("messages").replaceChildren(text("p", "Offline: use search to find messages on this device."));
    } else await load();
  }
}
function localStatus(message, progress) {
  $("local-status").textContent = message;
  $("local-progress").hidden = !progress;
  if (progress) {
    if (progress.total) {
      $("local-progress").max = progress.total;
      $("local-progress").value = progress.loaded ?? 0;
    } else $("local-progress").removeAttribute("value");
  }
}
function localReady(state) {
  $("local-badge").textContent = state ? "· " + (state.messages ?? 0) + " messages on this device" : "";
  $("search-where").hidden = !state;
  $("local-sync").hidden = !state;
  $("local-remove").hidden = !state;
}
const megabytes = (bytes) => (bytes / 1024 ** 2).toFixed(bytes < 10 * 1024 ** 2 ? 1 : 0) + " MB";
// Open this mailbox's on-device copy, downloading the model on first use, then sync.
function startLocal(confirmDownload) {
  localOpening ??= (async () => {
    const mailboxId = me.mailbox.id;
    try {
      const module = await localModule();
      const config = await module.localConfig();
      if (
        confirmDownload &&
        !confirm(
          "On-device search downloads the " + config.catalog.name + " model (" +
            megabytes(config.downloadBytes) + ") once, then indexes this mailbox in the background. Continue?"
        )
      ) {
        $("local-toggle").checked = false;
        return;
      }
      local = await module.LocalMailIndex.open(mailboxId, {
        config,
        onStatus: (s) =>
          localStatus(
            s.stage === "model-download" || s.stage === "model-verify"
              ? "Downloading the on-device model…"
              : "Preparing the on-device model…",
            s
          ),
      });
      // Signed out or opted out while the model was loading: discard the copy.
      if (me?.mailbox.id !== mailboxId || !$("local-toggle").checked) {
        const opened = local;
        local = null;
        await opened.remove();
        return;
      }
      rememberIdentity();
      localReady(local.state);
      if (navigator.onLine !== false) await syncLocal();
      else localStatus("Offline. " + (local.state.messages ?? 0) + " messages available on this device.");
    } catch (error) {
      localStatus("On-device search is unavailable: " + error.message);
    } finally {
      localOpening = null;
    }
  })();
  return localOpening;
}
async function syncLocal() {
  if (!local) return;
  localStatus("Indexing new messages on this device…", {});
  const state = await local.sync({
    onStatus: (s) => {
      if (s.stage === "synced") localReady(local.state);
      localStatus("Indexing new messages on this device… " + (s.loaded ?? 0) + " so far", {});
    },
  });
  localReady(state);
  localStatus("Up to date. " + megabytes(state.bytes ?? 0) + " stored on this device.");
}
async function stopLocal() {
  const current = local;
  local = null;
  localReady(null);
  if (current) await current.remove();
  else if (me) await (await localModule()).removeMailbox(me.mailbox.id);
  localStatus("Removed this mailbox's copy from this device.");
}
$("login").onsubmit = guarded(async (event) => {
  const form = event.currentTarget,
    e = form.elements,
    action = event.submitter.value;
  if (action === "recover") {
    await api("/api/auth/request-password-reset", {
      email: e.email.value,
      redirectTo: location.origin + "/reset-password",
    });
    status("If the account exists, a password reset email is on its way.");
    return;
  }
  const body = {
    email: e.email.value,
    password: e.password.value,
    ...(action === "signup"
      ? { name: e.name.value || e.email.value, callbackURL: location.origin + "/" }
      : {}),
  };
  await api("/api/auth/" + (action === "signup" ? "sign-up/email" : "sign-in/email"), body);
  await refresh();
  status(me ? "Signed in." : "Check your email to verify your account, then sign in.");
});
$("logout").onclick = guarded(async () => {
  let wipeError = null;
  if (local || Object.keys(localFlags()).length) await wipeLocal().catch((error) => (wipeError = error));
  // Always end the session, even if the on-device copy could not be removed.
  await api("/api/auth/sign-out", {});
  await refresh(); // also retries a failed wipe once
  let stillPending = false;
  try {
    stillPending = localStorage.getItem(WIPE_PENDING) === "1";
  } catch {
    /* Storage disabled. */
  }
  if (wipeError && stillPending)
    status("Signed out, but this device's mailbox copy could not be removed: " + wipeError.message +
      ". Close other tabs of this app and sign in and out again to retry.", true);
  else status("Signed out.");
});
$("search").onsubmit = guarded(async (event) => {
  const e = event.currentTarget.elements,
    query = e.query.value;
  if (local && (e.where.value === "device" || !navigator.onLine)) {
    folder = "inbox";
    cursor = null;
    render(await local.search(query));
    return;
  }
  await load("/mail/search?q=" + encodeURIComponent(query));
});
$("local-toggle").onchange = guarded(async (event) => {
  if (event.currentTarget.checked) await startLocal(true);
  else await stopLocal();
});
$("local-sync").onclick = guarded(syncLocal);
$("local-remove").onclick = guarded(async () => {
  $("local-toggle").checked = false;
  await stopLocal();
});
addEventListener("online", () => {
  if (local) syncLocal().catch((error) => localStatus(error.message));
});
if ("serviceWorker" in navigator)
  navigator.serviceWorker.register("/sw.js").catch(() => {
    /* The app still works online without the offline shell. */
  });
$("inbox").onclick = guarded(() => load());
$("outbox").onclick = guarded(() => load("/mail/outbox"));
$("next").onclick = guarded(() =>
  load(
    (folder === "outbox" ? "/mail/outbox" : "/mail/messages") +
      "?cursor=" +
      encodeURIComponent(cursor)
  )
);
$("compose").onclick = () => edit();
$("cancel").onclick = () => {
  $("editor").hidden = true;
};
$("editor").onsubmit = guarded(async (event) => {
  const e = event.currentTarget.elements,
    button = event.submitter;
  button.disabled = true;
  try {
    const result = await api("/mail/outbox", {
      id: editorId,
      to: e.to.value,
      subject: e.subject.value,
      text: e.text.value,
      ...(replyTo ? { reply_to: replyTo } : {}),
    });
    $("editor").hidden = true;
    status("Message queued.");
    for (let attempt = 0; attempt < 10; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 3000));
      const current = await api("/mail/outbox/" + result.id);
      if (["accepted", "unknown", "rejected"].includes(current.state)) {
        status(
          current.state === "accepted"
            ? "Email service accepted the message. Delivery is not yet confirmed."
            : current.state === "unknown"
              ? "Submission outcome is unknown. The message will not be resent automatically."
              : "Email service rejected the message.",
          current.state !== "accepted"
        );
        break;
      }
    }
  } finally {
    button.disabled = false;
  }
});
$("reset-form").onsubmit = guarded(async (event) => {
  const token = new URL(location.href).searchParams.get("token");
  if (!token) throw Error("This reset link is incomplete.");
  await api("/api/auth/reset-password", {
    token,
    newPassword: event.currentTarget.elements.password.value,
  });
  history.replaceState(null, "", "/");
  $("reset").hidden = true;
  await refresh();
  status("Password changed. You can sign in.");
});
if (location.pathname === "/reset-password") {
  $("account").hidden = true;
  $("reset").hidden = false;
} else await guarded(refresh)();
