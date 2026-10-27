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

package sources

import (
	"context"
	"encoding/base64"
	"fmt"
	"mime"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/html/charset"
)

type gmailPart struct {
	PartID   string                         `json:"partId"`
	MIME     string                         `json:"mimeType"`
	Filename string                         `json:"filename"`
	Headers  []struct{ Name, Value string } `json:"headers"`
	Body     struct {
		AttachmentID string `json:"attachmentId"`
		Data         string `json:"data"`
		Size         int64  `json:"size"`
	} `json:"body"`
	Parts []gmailPart `json:"parts"`
}
type gmailMessage struct {
	ID           string    `json:"id"`
	ThreadID     string    `json:"threadId"`
	HistoryID    string    `json:"historyId"`
	LabelIDs     []string  `json:"labelIds"`
	InternalDate string    `json:"internalDate"`
	Payload      gmailPart `json:"payload"`
}

func (c *CollaborationConnector) gmailAccount(ctx context.Context) (string, error) {
	var p struct {
		Email string `json:"emailAddress"`
	}
	err := c.get(ctx, "gmail", "/profile", &p)
	if err != nil {
		return "", err
	}
	if p.Email == "" {
		return "", fmt.Errorf("Gmail account identity missing")
	}
	return p.Email, nil
}
func (c *CollaborationConnector) calendarID(ctx context.Context, id string) (string, error) {
	var entry struct {
		ID         string `json:"id"`
		AccessRole string `json:"accessRole"`
	}
	if err := c.get(ctx, "calendar", "/users/me/calendarList/"+url.PathEscape(id), &entry); err != nil {
		return "", err
	}
	if entry.ID == "" || (entry.AccessRole != "reader" && entry.AccessRole != "writer" && entry.AccessRole != "owner") {
		return "", fmt.Errorf("calendar requires event read access")
	}
	if c.calendarRoles == nil {
		c.calendarRoles = map[string]string{}
	}
	c.calendarRoles[entry.ID] = entry.AccessRole
	return entry.ID, nil
}
func (c *CollaborationConnector) workspaceRefs(ctx context.Context, config CollaborationConfig) ([]collaborationRef, error) {
	refs := []collaborationRef{}
	if config.Resource == "gmail" {
		account, err := c.gmailAccount(ctx)
		if err != nil {
			return nil, err
		}
		// Verify every explicit label, including labels whose selection is empty.
		for _, label := range config.LabelIDs {
			var v map[string]any
			if err := c.get(ctx, "gmail", "/labels/"+url.PathEscape(label), &v); err != nil {
				return nil, err
			}
		}
		page := ""
		seen := map[string]bool{}
		for {
			q := url.Values{"maxResults": {"500"}, "includeSpamTrash": {"true"}}
			if config.preview {
				q.Set("maxResults", "1")
			}
			for _, label := range config.LabelIDs {
				q.Add("labelIds", label)
			}
			if page != "" {
				q.Set("pageToken", page)
			}
			var result struct {
				Messages []struct {
					ID string `json:"id"`
				} `json:"messages"`
				Next string `json:"nextPageToken"`
			}
			if err := c.get(ctx, "gmail", "/messages?"+q.Encode(), &result); err != nil {
				return nil, err
			}
			for _, message := range result.Messages {
				if !googleFileID(message.ID) {
					return nil, ErrInventoryIncomplete
				}
				refs = append(refs, collaborationRef{Resource: "gmail", Container: account, ID: message.ID})
				if config.preview {
					return refs, nil
				}
				if len(refs) > maxCollaborationSourceObjects {
					return nil, ErrInventoryIncomplete
				}
			}
			page = result.Next
			if page == "" {
				break
			}
			if err := paginationToken(seen, page); err != nil {
				return nil, err
			}
		}
	} else if config.Resource == "calendar" {
		for _, selected := range config.CalendarIDs {
			id, err := c.calendarID(ctx, selected)
			if err != nil {
				return nil, err
			}
			page := ""
			seen := map[string]bool{}
			for {
				// Preserve recurring masters and exceptions without expanding an
				// unbounded recurrence. Canceled events are tombstones, not text.
				q := url.Values{"maxResults": {"2500"}, "singleEvents": {"false"}, "showDeleted": {"true"}}
				if config.preview {
					q.Set("maxResults", "1")
					q.Set("showDeleted", "false")
				}
				if page != "" {
					q.Set("pageToken", page)
				}
				var result struct {
					Items []calendarEvent `json:"items"`
					Next  string          `json:"nextPageToken"`
					Token string          `json:"nextSyncToken"`
				}
				if err := c.get(ctx, "calendar", "/calendars/"+url.PathEscape(id)+"/events?"+q.Encode(), &result); err != nil {
					return nil, err
				}
				if result.Next == "" && config.tokens != nil {
					config.tokens["calendar:"+id] = result.Token
				}
				for _, event := range result.Items {
					if event.Status == "cancelled" && event.RecurringEventID == "" {
						continue
					}
					if !googleFileID(event.ID) {
						return nil, ErrInventoryIncomplete
					}
					refs = append(refs, collaborationRef{Resource: "calendar", Container: id, ID: event.ID})
					if config.preview {
						return refs, nil
					}
					if len(refs) > maxCollaborationSourceObjects {
						return nil, ErrInventoryIncomplete
					}
				}
				page = result.Next
				if page == "" {
					break
				}
				if err := paginationToken(seen, page); err != nil {
					return nil, err
				}
			}
		}
	} else {
		return nil, fmt.Errorf("invalid Workspace listing resource")
	}
	return refs, nil
}
func (c *CollaborationConnector) gmailLoad(ctx context.Context, config CollaborationConfig, ref collaborationRef) (collaborationMaterial, error) {
	var message gmailMessage
	if err := c.get(ctx, "gmail", "/messages/"+url.PathEscape(ref.ID)+"?format=full", &message); err != nil {
		return collaborationMaterial{}, err
	}
	if message.ID != ref.ID {
		return collaborationMaterial{}, fmt.Errorf("Gmail message identity changed")
	}
	for _, label := range config.LabelIDs {
		found := false
		for _, actual := range message.LabelIDs {
			if label == actual {
				found = true
			}
		}
		if !found {
			return collaborationMaterial{}, ErrInventoryIncomplete
		}
	}
	var text strings.Builder
	meta := map[string]any{"mailbox": ref.Container, "thread_id": message.ThreadID, "history_id": message.HistoryID, "label_ids": message.LabelIDs, "internal_date": message.InternalDate}
	subject := ""
	for _, header := range message.Payload.Headers {
		switch strings.ToLower(header.Name) {
		case "subject", "from", "to", "cc", "date":
			text.WriteString(header.Name + ": " + header.Value + "\n")
			if strings.EqualFold(header.Name, "subject") {
				subject = header.Value
			}
		case "message-id", "in-reply-to", "references":
			meta[strings.ToLower(header.Name)] = header.Value
		}
	}
	files := []collaborationFile{}
	links := []SourceLink{}
	decodedBytes := 0
	var walk func(gmailPart, int) error
	walk = func(part gmailPart, depth int) error {
		if depth > 50 {
			return fmt.Errorf("Gmail MIME nesting exceeds limit")
		}
		attachment := part.Filename != "" || (part.Body.AttachmentID != "" && !strings.HasPrefix(part.MIME, "text/"))
		if attachment && !config.Attachments && part.MIME != "text/calendar" {
			return nil
		}
		data := []byte{}
		if part.Body.Size > maxImportSourceObjectBytes {
			return fmt.Errorf("Gmail MIME part exceeds byte limit")
		}
		if part.Body.AttachmentID != "" {
			var body struct {
				Data string `json:"data"`
				Size int64  `json:"size"`
			}
			if err := c.get(ctx, "gmail", "/messages/"+url.PathEscape(ref.ID)+"/attachments/"+url.PathEscape(part.Body.AttachmentID), &body); err != nil {
				return err
			}
			var err error
			data, err = base64.RawURLEncoding.DecodeString(strings.TrimRight(body.Data, "="))
			if err != nil {
				return err
			}
		} else if part.Body.Data != "" {
			var err error
			data, err = base64.RawURLEncoding.DecodeString(strings.TrimRight(part.Body.Data, "="))
			if err != nil {
				return err
			}
		}
		if len(data) > maxImportSourceObjectBytes {
			return fmt.Errorf("Gmail part exceeds decoded limit")
		}
		decodedBytes += len(data)
		if decodedBytes > maxImportSourceObjectBytes {
			return fmt.Errorf("Gmail message and attachments exceed extraction budget")
		}
		if part.MIME == "text/calendar" {
			meta["calendar_invitation"] = true
			meta["calendar_uids"] = calendarInvitationUIDs(data)
		}
		if attachment && config.Attachments {
			id := part.PartID
			if id == "" {
				id = part.Body.AttachmentID
			}
			if id == "" {
				return fmt.Errorf("Gmail attachment identity missing")
			}
			files = append(files, collaborationFile{ID: id, Name: part.Filename, MIME: part.MIME, Data: data})
		} else if !attachment && (part.MIME == "text/plain" || part.MIME == "text/html" || part.MIME == "text/calendar") {
			for _, header := range part.Headers {
				if strings.EqualFold(header.Name, "Content-Type") {
					_, params, err := mime.ParseMediaType(header.Value)
					if err != nil {
						return err
					}
					if encoding := params["charset"]; encoding != "" {
						reader, err := charset.NewReaderLabel(encoding, strings.NewReader(string(data)))
						if err != nil {
							return err
						}
						data, err = readBounded(reader)
						if err != nil {
							return err
						}
					}
				}
			}
			links = append(links, SourceLinks(data)...)
			if part.MIME == "text/html" {
				data = htmlText(data)
			}
			text.WriteByte('\n')
			text.Write(data)
			if part.MIME == "text/calendar" {
				meta["calendar_invitation"] = true
				meta["calendar_uids"] = calendarInvitationUIDs(data)
			}
		}
		if text.Len() > maxImportSourceObjectBytes || len(files) > 1000 {
			return fmt.Errorf("Gmail message exceeds extraction limit")
		}
		children := part.Parts
		if part.MIME == "multipart/alternative" {
			// Preserve the readable plain variant when present, avoiding duplicate
			// search text. HTML resource links remain explicit relationship metadata.
			for _, child := range part.Parts {
				if child.MIME == "text/plain" {
					for _, alternate := range part.Parts {
						if alternate.MIME == "text/html" && alternate.Body.Data != "" {
							raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(alternate.Body.Data, "="))
							if err != nil {
								return err
							}
							links = append(links, SourceLinks(raw)...)
						}
					}
					children = []gmailPart{child}
					for _, other := range part.Parts {
						if other.MIME == "text/calendar" {
							children = append(children, other)
						}
					}
					break
				}
			}
		}
		for _, child := range children {
			if err := walk(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(message.Payload, 0); err != nil {
		return collaborationMaterial{}, err
	}
	meta["related_resources"] = links
	return collaborationMaterial{Text: []byte(text.String()), URL: "https://mail.google.com/mail/u/" + url.PathEscape(ref.Container) + "/#all/" + message.ID, Title: subject, Metadata: meta, Files: files}, nil
}

func calendarInvitationUIDs(data []byte) []string {
	// RFC 5545 folds physical lines with a leading space or tab.
	unfolded := strings.ReplaceAll(strings.ReplaceAll(string(data), "\r\n ", ""), "\r\n\t", "")
	uids := []string{}
	seen := map[string]bool{}
	for _, line := range strings.Split(unfolded, "\n") {
		line = strings.TrimSuffix(line, "\r")
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(key, ";")
		if strings.EqualFold(name, "UID") && value != "" && !seen[value] {
			seen[value] = true
			uids = append(uids, value)
		}
	}
	return uids
}

type calendarEvent struct {
	ID                string           `json:"id"`
	ETag              string           `json:"etag"`
	Visibility        string           `json:"visibility"`
	Status            string           `json:"status"`
	Summary           string           `json:"summary"`
	Description       string           `json:"description"`
	Location          string           `json:"location"`
	HTMLLink          string           `json:"htmlLink"`
	ICalUID           string           `json:"iCalUID"`
	RecurringEventID  string           `json:"recurringEventId"`
	Start             map[string]any   `json:"start"`
	End               map[string]any   `json:"end"`
	OriginalStartTime map[string]any   `json:"originalStartTime"`
	Recurrence        []string         `json:"recurrence"`
	Organizer         map[string]any   `json:"organizer"`
	Attendees         []map[string]any `json:"attendees"`
	AttendeesOmitted  bool             `json:"attendeesOmitted"`
	Attachments       []struct {
		FileID  string `json:"fileId"`
		FileURL string `json:"fileUrl"`
		Title   string `json:"title"`
		MIME    string `json:"mimeType"`
	} `json:"attachments"`
}

func calendarPrivateVisibility(visibility string) bool {
	return visibility == "private" || visibility == "confidential"
}

func (c *CollaborationConnector) calendarLoad(ctx context.Context, config CollaborationConfig, ref collaborationRef) (collaborationMaterial, error) {
	var event calendarEvent
	if err := c.get(ctx, "calendar", "/calendars/"+url.PathEscape(ref.Container)+"/events/"+url.PathEscape(ref.ID), &event); err != nil {
		return collaborationMaterial{}, err
	}
	if event.ID != ref.ID || (event.ETag == "" && event.Status != "cancelled") || (event.Status == "cancelled" && event.RecurringEventID == "") || event.AttendeesOmitted {
		return collaborationMaterial{}, ErrInventoryIncomplete
	}
	meta := map[string]any{"calendar_id": ref.Container, "etag": event.ETag, "ical_uid": event.ICalUID, "recurring_event_id": event.RecurringEventID, "start": event.Start, "end": event.End, "original_start_time": event.OriginalStartTime, "recurrence": event.Recurrence, "organizer": event.Organizer, "attendees": event.Attendees}
	meta["status"] = event.Status
	meta["visibility"] = event.Visibility
	meta["details_visible"] = !calendarPrivateVisibility(event.Visibility) || c.calendarRoles[ref.Container] == "writer" || c.calendarRoles[ref.Container] == "owner"
	text := event.Summary + "\n" + string(htmlText([]byte(event.Description))) + "\n" + event.Location
	if event.Status == "cancelled" {
		text = "Canceled occurrence\n" + text
	}
	links := SourceLinks([]byte(event.Description))
	for _, attachment := range event.Attachments {
		if attachment.FileID != "" {
			links = append(links, SourceLink{Kind: "drive", ID: attachment.FileID, URL: "https://drive.google.com/file/d/" + url.PathEscape(attachment.FileID) + "/view"})
		}
		text += "\nAttachment: " + attachment.Title
	}
	// Drive links are relationships, never an authorization shortcut. Their text
	// is acquired by the independently selected Drive source, under Drive access.
	meta["related_resources"] = links
	files := []collaborationFile{}
	if config.Attachments && len(event.Attachments) > 0 {
		driveAccess := false
		for _, service := range c.Services {
			if service == "drive" {
				driveAccess = true
			}
		}
		if !driveAccess {
			return collaborationMaterial{}, fmt.Errorf("calendar attachment extraction requires Google Workspace Drive access")
		}
		selected := event.Attachments
		if config.preview && len(selected) > PreviewItemLimit-1 {
			selected = selected[:PreviewItemLimit-1]
		}
		for _, attachment := range selected {
			if !googleFileID(attachment.FileID) {
				return collaborationMaterial{}, fmt.Errorf("calendar external attachments require an independently authorized source")
			}
			// Defer Drive metadata and bytes until materialItems has applied the
			// preview count and the remaining aggregate extraction budget.
			files = append(files, collaborationFile{ID: attachment.FileID, Name: attachment.Title, MIME: attachment.MIME, driveFile: true})
		}
	}
	return collaborationMaterial{Text: []byte(text), URL: event.HTMLLink, Title: event.Summary, Metadata: meta, Files: files}, nil
}

func (c *CollaborationConnector) workspaceDriveFile(ctx context.Context, id string, limit int) (collaborationFile, error) {
	drive := c.drive()
	var file driveSourceFile
	if err := drive.get(ctx, c.HTTPClient, "/files/"+url.PathEscape(id)+"?fields="+url.QueryEscape(driveSourceFields)+"&supportsAllDrives=true", &file); err != nil {
		return collaborationFile{}, err
	}
	if file.ID != id || file.Version == "" {
		return collaborationFile{}, fmt.Errorf("Drive attachment identity or revision missing")
	}
	size := int64(maxImportSourceObjectBytes)
	if file.Size != "" {
		var err error
		size, err = strconv.ParseInt(file.Size, 10, 64)
		if err != nil || size < 0 || size > maxImportSourceObjectBytes {
			return collaborationFile{}, fmt.Errorf("Drive attachment size invalid")
		}
	}
	export := googleWorkspaceTextExport(file.MIME)
	if export == "" && file.Size != "" && size > int64(limit) {
		return collaborationFile{}, fmt.Errorf("%w: Drive attachment exceeds remaining byte budget", ErrInvalidInput)
	}
	item := Item{ID: "drive://attachment/" + id, Path: file.Name, Filename: file.Name, Revision: file.Version, Size: size, Metadata: map[string]any{"file_id": id, "mime_type": file.MIME, "export_mime_type": export, "md5_checksum": file.MD5, "size_is_upper_bound": export != "" || file.Size == ""}}
	data, err := drive.readWithin(ctx, c.HTTPClient, Request{}, item, limit)
	if err != nil {
		return collaborationFile{}, err
	}
	mime := file.MIME
	if export != "" {
		mime = export
	}
	return collaborationFile{ID: id, Revision: file.Version, Name: file.Name, MIME: mime, Data: data, CanonicalURL: "https://drive.google.com/file/d/" + url.PathEscape(id) + "/view"}, nil
}
func (c *CollaborationConnector) readWorkspaceDrive(ctx context.Context, r Request, item Item, extractBinary bool) ([]byte, error) {
	data, err := c.drive().Read(ctx, r, item)
	if err != nil {
		return nil, err
	}
	if configString(item.Metadata, "export_mime_type") != "" || !extractBinary {
		return data, nil
	}
	extract := c.Extractor
	if extract == nil {
		extract = ExtractAttachmentText
	}
	return extract(ctx, item.Filename, configString(item.Metadata, "mime_type"), data)
}

func (c *CollaborationConnector) validateRef(ctx context.Context, config CollaborationConfig, ref collaborationRef) error {
	if ref.ID == "" || ref.Container == "" || strings.ContainsAny(ref.ID, "/\\\x00\r\n?#") {
		return fmt.Errorf("invalid acquisition identity")
	}
	switch c.ProviderName {
	case ProviderSlack:
		if ref.Resource != ProviderSlack {
			return fmt.Errorf("provider acquisition mismatch")
		}
		return c.slackScope(ctx, config, ref.Container)
	case ProviderDiscord:
		if ref.Resource != ProviderDiscord || !discordID(ref.ID) {
			return fmt.Errorf("provider acquisition mismatch")
		}
		return c.discordScope(ctx, config, ref.Container)
	case ProviderGoogleWorkspace:
		if ref.Resource != config.Resource {
			return fmt.Errorf("Workspace acquisition mismatch")
		}
		if ref.Resource == "gmail" {
			account, err := c.gmailAccount(ctx)
			if err != nil {
				return err
			}
			if account != ref.Container {
				return fmt.Errorf("Gmail account changed")
			}
			return nil
		}
		for _, selected := range config.CalendarIDs {
			id, err := c.calendarID(ctx, selected)
			if err != nil {
				return err
			}
			if id == ref.Container {
				return nil
			}
		}
	}
	return fmt.Errorf("resource is outside selected source scope")
}
