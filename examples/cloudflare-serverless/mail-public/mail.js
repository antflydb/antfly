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
  folder = "inbox";
function status(text, error = false) {
  $("status").textContent = text;
  $("status").classList.toggle("error", error);
}
async function api(path, body) {
  const result = await fetch(path, {
    method: body ? "POST" : "GET",
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
  $("next").hidden = !cursor;
  $("messages").replaceChildren();
  for (const message of result.messages) {
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
    button.onclick = guarded(() =>
      folder === "outbox" ? showOutbound(message) : openMessage(message.id)
    );
    $("messages").append(button);
  }
  if (!result.messages.length) $("messages").append(text("p", "No messages."));
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
  const message = await api("/mail/messages/" + encodeURIComponent(id));
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
  const reply = text("button", "Reply");
  reply.onclick = () => edit(message);
  article.append(document.createElement("br"), reply);
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
async function refresh() {
  try {
    me = await api("/api/me");
  } catch {
    me = null;
  }
  $("account").hidden = !!me;
  $("mail").hidden = !me;
  $("logout").hidden = !me;
  $("identity").textContent = me?.user.email ?? "";
  if (me) {
    $("address").textContent = "Your mailbox: " + me.mailbox.address;
    await load();
  }
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
  await api("/api/auth/sign-out", {});
  await refresh();
  status("Signed out.");
});
$("search").onsubmit = guarded(async (event) => {
  await load("/mail/search?q=" + encodeURIComponent(event.currentTarget.elements.query.value));
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
