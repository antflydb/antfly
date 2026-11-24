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
// Mailboxes whose owner turned the switch on, recorded before the copy is
// usable so the switch survives a reload mid-setup. LOCAL_FLAGS stays the
// record of a usable copy (offline start, sign-out wipe).
const LOCAL_WANTED = "antfly-mail-local-wanted-v1";
// Minimal identity for opening an opted-in copy offline; removed on sign-out.
const OFFLINE_IDENTITY = "antfly-mail-offline-identity-v1";
// Set while a sign-out wipe is unfinished, so the next load retries it.
const WIPE_PENDING = "antfly-mail-local-wipe-pending-v1";
const localModule = () => import("/local-index.js");
const localSupported = () =>
  typeof Worker !== "undefined" &&
  typeof WebAssembly !== "undefined" &&
  typeof navigator.storage?.getDirectory === "function";
function localFlags(key = LOCAL_FLAGS) {
  try {
    return JSON.parse(localStorage.getItem(key) ?? "{}") ?? {};
  } catch {
    return {};
  }
}
function setLocalWanted(mailboxId, on) {
  try {
    const wanted = localFlags(LOCAL_WANTED);
    if (on) wanted[mailboxId] = true;
    else delete wanted[mailboxId];
    if (Object.keys(wanted).length) localStorage.setItem(LOCAL_WANTED, JSON.stringify(wanted));
    else localStorage.removeItem(LOCAL_WANTED);
  } catch {
    /* Storage disabled: the switch lasts for this page only. */
  }
}
// Sign-out on a shared machine removes every on-device mailbox copy. This needs
// no runtime download, so it works even when the local module cannot load.
async function wipeLocal() {
  // Never allow offline reopening after sign-out, even if deletion fails.
  try {
    localStorage.removeItem(OFFLINE_IDENTITY);
    localStorage.removeItem(LOCAL_WANTED);
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
// Status is a toast: confirmations clear themselves, errors stay until replaced.
let statusTimer = null;
function status(text, error = false, timeout = error ? 0 : 5000) {
  clearTimeout(statusTimer);
  $("status").textContent = text;
  $("status").classList.toggle("error", error);
  if (text && timeout) statusTimer = setTimeout(() => ($("status").textContent = ""), timeout);
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
function placeholder(message) {
  const box = document.createElement("div");
  box.className = "empty";
  box.append(text("p", message));
  $("message").replaceChildren(box);
  selected = null;
  reading(false);
}
// Reading or composing shows the reader pane; on phones it replaces the list.
function reading(on) {
  $("mail").classList.toggle("reading", on);
}
// "Ada Lovelace <ada@example.com>" → { name, address }; a bare address has no name.
function sender(value = "") {
  const match = /^\s*"?([^"<]*?)"?\s*<([^>]+)>\s*$/.exec(value);
  return match && match[1] ? { name: match[1], address: match[2] } : { name: "", address: value.trim() };
}
// Today: time. This year: month and day. Older: the year too.
function shortDate(time) {
  const date = new Date(time),
    now = new Date();
  if (date.toDateString() === now.toDateString())
    return date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  return date.toLocaleDateString([], {
    month: "short",
    day: "numeric",
    ...(date.getFullYear() === now.getFullYear() ? {} : { year: "numeric" }),
  });
}
const fullDate = (time) =>
  new Date(time).toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
function size(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 ** 2) return Math.round(bytes / 1024) + " KB";
  return (bytes / 1024 ** 2).toFixed(1) + " MB";
}
const outboundLabels = {
  queued: "Sending",
  pending: "Sending",
  accepted: "Sent",
  unknown: "Not confirmed",
  rejected: "Not sent",
};
const outboundTone = { accepted: "ok", unknown: "warn", rejected: "bad" };
const folderPaths = { inbox: "/mail/messages", outbox: "/mail/outbox", trash: "/mail/trash" };
const folderTitles = { inbox: "Inbox", outbox: "Outbox", trash: "Trash" };
let selected = null;
function showFolder(name, title = folderTitles[name]) {
  folder = name;
  for (const id of ["inbox", "outbox", "trash"])
    if (id === name && !title.startsWith("Results")) $(id).setAttribute("aria-current", "page");
    else $(id).removeAttribute("aria-current");
  $("list-title").textContent = title;
}
async function load(path = "/mail/messages") {
  const search = path.startsWith("/mail/search");
  showFolder(
    path.startsWith("/mail/outbox") ? "outbox" : path.startsWith("/mail/trash") ? "trash" : "inbox",
    search ? "Results" : undefined
  );
  const result = await api(path);
  cursor = result.cursor;
  $("trash-banner").hidden = folder !== "trash";
  if (folder === "trash") {
    $("trash-retention").textContent =
      "Messages that have been in Trash more than " + result.retention_days + " days will be deleted automatically.";
    $("empty-trash").hidden = !result.messages.length;
  }
  render(result.messages);
}
const daysLeft = (time) => Math.max(0, Math.ceil((time - Date.now()) / 86400000));
function render(messages) {
  $("next").hidden = !cursor;
  $("messages").replaceChildren();
  for (const message of messages) {
    const button = document.createElement("button");
    button.className = "mail-row";
    const outbound = folder === "outbox";
    const who = sender(outbound ? message.to : message.from);
    const when = document.createElement("time");
    when.textContent = shortDate(message.received_at ?? message.created_at);
    const name = span("who", (outbound ? "To " : "") + (who.name || who.address));
    const subject = text("span", message.subject || "(No subject)");
    subject.className = "subject";
    button.append(avatar(who.name || who.address), name, when, subject);
    const preview = (message.text ?? "").replace(/\s+/g, " ").trim().slice(0, 160);
    if (preview) {
      const snippet = text("span", preview);
      snippet.className = "snippet";
      button.append(snippet);
    }
    const tag = (label, tone = "") => {
      const element = text("span", label);
      element.className = "tag " + tone;
      button.append(element);
    };
    if (outbound && message.state)
      tag(outboundLabels[message.state] ?? message.state, outboundTone[message.state]);
    if (message.delete_at) tag("Deleted forever in " + daysLeft(message.delete_at) + " days", "warn");
    if (message.local) tag("Matched by meaning");
    if (message.id === selected) button.setAttribute("aria-current", "true");
    button.onclick = guarded(async () => {
      if (!discardDraft()) return;
      selected = message.id;
      for (const row of $("messages").children) row.removeAttribute?.("aria-current");
      button.setAttribute("aria-current", "true");
      if (outbound) showOutbound(message);
      else await openMessage(message.id);
    });
    $("messages").append(button);
  }
  if (!messages.length) {
    const empty = text("p", folder === "trash" ? "Trash is empty." : "No messages.");
    empty.className = "list-empty";
    $("messages").append(empty);
  }
}
// Subject, then a sender block: initials, name and address, date, recipient.
function header(subject, person, when, line) {
  const meta = document.createElement("div");
  meta.className = "meta";
  const who = span("who", "");
  who.append(span("from", person.label));
  if (person.address) who.append(span("addr", person.address));
  const time = document.createElement("time");
  time.textContent = when ? fullDate(when) : "";
  meta.append(avatar(person.initialsFrom, true), who, time, span("to", line));
  return [text("h2", subject || "(No subject)"), meta];
}
// One or two initials from a display name, else the address's first letter.
function avatar(value = "", large = false) {
  const words = value.replace(/@.*/, "").split(/[\s._-]+/).filter((w) => /\p{L}|\p{N}/u.test(w));
  const initials = (words.length > 1 && !value.includes("@") ? words[0][0] + words.at(-1)[0] : (words[0] ?? "?")[0]) ?? "?";
  const element = span("avatar" + (large ? " large" : ""), initials.toUpperCase());
  element.setAttribute("aria-hidden", "true");
  return element;
}
function iconButton(label, icon, className = "ghost") {
  const button = document.createElement("button");
  button.type = "button";
  button.className = className;
  button.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-' + icon + '"/></svg>';
  button.append(label);
  return button;
}
function span(className, value) {
  const element = text("span", value);
  element.className = className;
  return element;
}
function showOutbound(message) {
  closeEditor();
  reading(true);
  const to = sender(message.to);
  $("message").replaceChildren(
    ...header(
      message.subject,
      { label: to.name || to.address, address: to.name ? to.address : "", initialsFrom: to.name || to.address },
      message.created_at ?? message.received_at,
      "Sent from your mailbox"
    )
  );
  const notes = document.createElement("div");
  notes.className = "notes";
  const state =
    message.state === "unknown"
      ? text("p", "The email service didn't confirm this message. It will not be resent automatically.")
      : message.state === "rejected"
        ? text("p", "The email service rejected this message.")
        : text("p", "Status: " + (outboundLabels[message.state] ?? message.state));
  state.className = "note" + (message.state in outboundTone && message.state !== "accepted" ? " warn" : "");
  notes.append(state);
  $("message").append(notes, text("pre", message.text));
}
async function openMessage(id) {
  let message;
  try {
    message = await api("/mail/messages/" + encodeURIComponent(id));
  } catch (error) {
    // Fall back only when the server could not be reached.
    message = error instanceof TypeError && local ? await local.message(id) : null;
    if (!message) throw error;
    message.offline = true;
  }
  closeEditor();
  reading(true);
  const article = $("message");
  // Actions first: reply, then delete (or restore) — the last two need the server.
  const toolbar = document.createElement("div");
  toolbar.className = "toolbar";
  const reply = iconButton("Reply", "reply", "primary");
  reply.onclick = () => edit(message);
  toolbar.append(reply, span("spacer", ""));
  if (!message.offline) {
    if (message.trashed_at != null) {
      const restore = iconButton("Restore", "restore");
      restore.onclick = guarded(() => restoreMessage(id));
      const forever = iconButton("Delete forever", "trash", "ghost danger");
      forever.onclick = guarded(() => deleteForever(id));
      toolbar.append(restore, forever);
    } else {
      const remove = iconButton("Delete", "trash");
      remove.onclick = guarded(() => trashMessage(id));
      toolbar.append(remove);
    }
  }
  const from = sender(message.from);
  article.replaceChildren(
    toolbar,
    ...header(
      message.subject,
      { label: from.name || from.address, address: from.name ? from.address : "", initialsFrom: from.name || from.address },
      message.received_at,
      "To " + message.to
    )
  );
  const notes = document.createElement("div");
  notes.className = "notes";
  const note = (value, tone = "") => {
    const element = text("p", value);
    element.className = "note " + tone;
    notes.append(element);
  };
  if (message.trashed_at != null && !message.offline)
    note("This message is in Trash and will be deleted forever on " + new Date(message.delete_at).toLocaleDateString() + ".", "warn");
  if (message.offline)
    note("Shown from this device's copy" + (message.text_truncated ? " (long text shortened)." : "."));
  if (message.html_available) note("This message also has an HTML version. Only plain text is shown.");
  if (notes.children.length) article.append(notes);
  article.append(text("pre", message.text || "(No plain-text content)"));
  if (message.attachments?.length) {
    const list = document.createElement("div");
    list.className = "attachments";
    for (const part of message.attachments) {
      const link = document.createElement("a");
      link.className = "attachment";
      link.href = "/mail/messages/" + id + "/attachments/" + part.index;
      link.download = part.filename;
      link.innerHTML = '<svg class="icon" aria-hidden="true"><use href="#i-clip"/></svg>';
      link.append(text("span", part.filename), span("size", size(part.size)));
      list.append(link);
    }
    article.append(list);
  }
}
// The device copy holds the inbox only: drop messages leaving it right away
// (the next sync carries the same change).
async function dropLocal(id) {
  if (local)
    await local
      .forget([id])
      .then(() => localReady(local?.state))
      .catch(() => {});
}
function statusWithAction(message, label, action) {
  status(message, false, 10000);
  const button = text("button", label);
  button.type = "button";
  button.onclick = guarded(action);
  $("status").append(button);
}
async function trashMessage(id) {
  await api("/mail/messages/" + encodeURIComponent(id) + "/trash", {});
  await dropLocal(id);
  placeholder("Message moved to Trash.");
  await load(folderPaths[folder] ?? folderPaths.inbox);
  statusWithAction("Message moved to Trash.", "Undo", () => restoreMessage(id));
}
async function restoreMessage(id) {
  await api("/mail/messages/" + encodeURIComponent(id) + "/restore", {});
  placeholder("Message restored to the inbox.");
  await load(folderPaths[folder] ?? folderPaths.inbox);
  status("Message restored to the inbox.");
  // The restored message is re-announced to devices; fetch and embed it now.
  if (local) syncLocal().catch((error) => localStatus(error.message));
}
async function deleteForever(id) {
  if (!confirm("Delete this message forever? You can't undo this.")) return;
  await api("/mail/messages/" + encodeURIComponent(id), null, "DELETE");
  await dropLocal(id);
  placeholder("Message deleted forever.");
  status("Message deleted forever.");
  await load(folderPaths.trash);
}
async function emptyTrash() {
  if (!confirm("Delete all messages in Trash forever? You can't undo this.")) return;
  const result = await api("/mail/trash/empty", {});
  placeholder("Select a message.");
  status(result.done ? "Trash emptied." : "Emptying Trash… larger Trash folders finish in the background.");
  await load(folderPaths.trash);
}
function edit(message) {
  replyTo = message?.id ?? null;
  editorId = crypto.randomUUID();
  $("editor").reset();
  // The composer takes the reader's place; the message being answered stays
  // visible below it as context (it is not added to the reply's text).
  $("editor").hidden = false;
  $("message").hidden = true;
  reading(true);
  $("editor-title").textContent = message ? "Reply" : "New message";
  $("quote").hidden = !message;
  if (message) {
    $("quote-meta").textContent =
      (message.received_at ? fullDate(message.received_at) + " · " : "") + message.from;
    $("quote-text").textContent = message.text ?? "";
  }
  // The server sends as "Name <mailbox>" unless the name is empty or an address.
  const name = (me?.user.name ?? "").trim();
  $("editor-from").textContent =
    name && !name.includes("@") ? name + " <" + me.mailbox.address + ">" : me?.mailbox.address ?? "";
  $("editor").elements.to.value = message?.from ?? "";
  $("editor").elements.to.readOnly = !!message;
  $("editor").elements.subject.value = message
    ? message.subject.startsWith("Re:")
      ? message.subject
      : "Re: " + message.subject
    : "";
  $("editor").elements[message ? "text" : "to"].focus();
}
// Opening something else while a draft has text asks before throwing it away.
function discardDraft() {
  const form = $("editor");
  return form.hidden || !form.elements.text.value.trim() || confirm("Discard this draft?");
}
function closeEditor() {
  $("editor").hidden = true;
  $("message").hidden = false;
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
  $("identity").textContent = $("identity").title = me?.user.email ?? "";
  if (me) {
    $("address").textContent = $("address").title = me.mailbox.address;
    $("local").hidden = !localSupported();
    let stillPending = false;
    try {
      stillPending = localStorage.getItem(WIPE_PENDING) === "1";
    } catch {
      /* Storage disabled. */
    }
    const id = me.mailbox.id;
    $("local-toggle").checked = !stillPending && (localFlags()[id] === true || localFlags(LOCAL_WANTED)[id] === true);
    if (stillPending) localStatus("A previous sign-out's on-device copy is still being removed. Close other tabs of this app.");
    if ($("local-toggle").checked) {
      rememberIdentity();
      startLocal(false);
    }
    if (offline) {
      status("You're offline. Search finds messages in this device's copy.", false, 0);
      const note = text("p", "Offline. Use search to find messages on this device.");
      note.className = "list-empty";
      $("messages").replaceChildren(note);
    } else await load();
  }
}
function localStatus(message, progress) {
  $("local-status").textContent = message;
  $("local-progress").hidden = !progress;
  // The switch pulses while the model downloads or messages are indexed.
  $("local-toggle").classList.toggle("busy", !!progress);
  if (progress) {
    if (progress.total) {
      $("local-progress").max = progress.total;
      $("local-progress").value = progress.loaded ?? 0;
    } else $("local-progress").removeAttribute("value");
  }
}
function localReady(state) {
  $("local-badge").textContent = state ? (state.messages ?? 0) + " messages on this device" : "Off";
  $("search-note-text").textContent = state ? "Search by meaning on this device, by Antfly" : "Search by Antfly";
  $("search-where").hidden = !state;
  $("local-sync").hidden = !state;
}
const megabytes = (bytes) => (bytes / 1024 ** 2).toFixed(bytes < 10 * 1024 ** 2 ? 1 : 0) + " MB";
// Open this mailbox's on-device copy, downloading the model on first use, then sync.
function startLocal(confirmDownload) {
  localOpening ??= (async () => {
    const mailboxId = me.mailbox.id;
    $("local-badge").textContent = "Setting up…";
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
        $("local-badge").textContent = "Off";
        setLocalWanted(mailboxId, false);
        return;
      }
      setLocalWanted(mailboxId, true);
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
      // local-index.js errors that already say "on-device" read fine on their own.
      localStatus(/on-device/i.test(error.message) ? error.message + "." : "On-device search is unavailable: " + error.message);
      // Setup never finished: turn the switch back off rather than retrying on
      // every load. A copy that was usable before keeps its switch on.
      if (!local && localFlags()[mailboxId] !== true) {
        if (me?.mailbox.id === mailboxId) $("local-toggle").checked = false;
        setLocalWanted(mailboxId, false);
        $("local-badge").textContent = "Off";
      } else if (!local) $("local-badge").textContent = "Unavailable";
    } finally {
      localOpening = null;
    }
  })();
  return localOpening;
}
async function syncLocal() {
  const index = local;
  if (!index) return;
  localStatus("Indexing new messages on this device…", {});
  const state = await index.sync({
    onStatus: (s) => {
      if (local !== index) return;
      if (s.stage === "synced") localReady(index.state);
      localStatus("Indexing new messages on this device… " + (s.loaded ?? 0) + " so far", {});
    },
  });
  if (local !== index) return;
  localReady(state);
  localStatus("Up to date. " + megabytes(state.bytes ?? 0) + " stored on this device.");
}
async function stopLocal() {
  if (me) setLocalWanted(me.mailbox.id, false);
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
$("copy-address").onclick = async () => {
  const button = $("copy-address");
  const use = button.querySelector("use");
  try {
    await navigator.clipboard.writeText($("address").textContent);
  } catch {
    status("Couldn't copy. Select the address and copy it instead.", true);
    return;
  }
  status("Address copied.");
  use.setAttribute("href", "#i-check");
  button.setAttribute("aria-label", "Copied");
  clearTimeout(button.reset);
  button.reset = setTimeout(() => {
    use.setAttribute("href", "#i-copy");
    button.setAttribute("aria-label", "Copy address");
  }, 2000);
};
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
    showFolder("inbox", "Results by meaning, on this device");
    $("trash-banner").hidden = true;
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
addEventListener("online", () => {
  if (local) syncLocal().catch((error) => localStatus(error.message));
});
if ("serviceWorker" in navigator)
  navigator.serviceWorker.register("/sw.js").catch(() => {
    /* The app still works online without the offline shell. */
  });
$("inbox").onclick = guarded(() => load());
$("outbox").onclick = guarded(() => load("/mail/outbox"));
$("trash").onclick = guarded(() => load("/mail/trash"));
$("empty-trash").onclick = guarded(emptyTrash);
$("next").onclick = guarded(() => load((folderPaths[folder] ?? folderPaths.inbox) + "?cursor=" + encodeURIComponent(cursor)));
$("compose").onclick = () => edit();
$("cancel").onclick = () => {
  closeEditor();
  if (!selected) reading(false);
};
$("back").onclick = () => {
  closeEditor();
  reading(false);
};
// Sign in and Create account share one form; the mode decides which fields show.
$("mode-switch").onclick = () => {
  const signup = $("login").dataset.mode === "signin";
  $("login").dataset.mode = signup ? "signup" : "signin";
  $("auth-title").textContent = signup ? "Create your account" : "Sign in";
  $("mode-prompt").textContent = signup ? "Already have an account?" : "New here?";
  $("mode-switch").textContent = signup ? "Sign in" : "Create an account";
  $("login").elements.password.autocomplete = signup ? "new-password" : "current-password";
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
    closeEditor();
    if (!selected) placeholder("Select a message to read it.");
    status("Sending…", false, 0);
    let settled = false;
    for (let attempt = 0; attempt < 10; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 3000));
      const current = await api("/mail/outbox/" + result.id);
      if (["accepted", "unknown", "rejected"].includes(current.state)) {
        status(
          current.state === "accepted"
            ? "Sent."
            : current.state === "unknown"
              ? "Couldn't confirm the message was sent. It won't be resent automatically."
              : "The email service rejected the message.",
          current.state !== "accepted"
        );
        settled = true;
        break;
      }
    }
    if (!settled) status("Still sending. Check the Outbox for its status.");
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
