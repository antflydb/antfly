// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package audio

// MIMEType returns the MIME type for the audio format.
func (f AudioFormat) MIMEType() string {
	switch f {
	case AudioFormatMp3:
		return "audio/mpeg"
	case AudioFormatWav:
		return "audio/wav"
	case AudioFormatOgg:
		return "audio/ogg"
	case AudioFormatOpus:
		return "audio/opus"
	case AudioFormatFlac:
		return "audio/flac"
	case AudioFormatPcm:
		return "audio/pcm"
	case AudioFormatAac:
		return "audio/aac"
	case AudioFormatWebm:
		return "audio/webm"
	default:
		return "application/octet-stream"
	}
}

// Extension returns the file extension for the audio format.
func (f AudioFormat) Extension() string {
	switch f {
	case AudioFormatMp3:
		return ".mp3"
	case AudioFormatWav:
		return ".wav"
	case AudioFormatOgg:
		return ".ogg"
	case AudioFormatOpus:
		return ".opus"
	case AudioFormatFlac:
		return ".flac"
	case AudioFormatPcm:
		return ".pcm"
	case AudioFormatAac:
		return ".aac"
	case AudioFormatWebm:
		return ".webm"
	default:
		return ""
	}
}

// FormatFromMIME returns the AudioFormat for a given MIME type.
func FormatFromMIME(mime string) AudioFormat {
	switch mime {
	case "audio/mpeg", "audio/mp3":
		return AudioFormatMp3
	case "audio/wav", "audio/wave", "audio/x-wav":
		return AudioFormatWav
	case "audio/ogg":
		return AudioFormatOgg
	case "audio/opus":
		return AudioFormatOpus
	case "audio/flac":
		return AudioFormatFlac
	case "audio/pcm", "audio/L16":
		return AudioFormatPcm
	case "audio/aac":
		return AudioFormatAac
	case "audio/webm":
		return AudioFormatWebm
	default:
		return ""
	}
}
