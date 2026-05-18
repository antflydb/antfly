//! HTTP Status Codes for httpx.zig
//!
//! Comprehensive implementation of HTTP status codes as defined in RFC 7231,
//! RFC 7232, RFC 7233, RFC 7235, and related specifications. Provides:
//!
//! - All standard status code constants
//! - Status code classification (informational, success, redirect, etc.)
//! - Standard reason phrases
//! - Semantic helper methods for common checks

const std = @import("std");

/// Common HTTP status code constants.
pub const StatusCode = struct {
    pub const CONTINUE: u16 = 100;
    pub const SWITCHING_PROTOCOLS: u16 = 101;
    pub const PROCESSING: u16 = 102;
    pub const EARLY_HINTS: u16 = 103;

    pub const OK: u16 = 200;
    pub const CREATED: u16 = 201;
    pub const ACCEPTED: u16 = 202;
    pub const NON_AUTHORITATIVE_INFORMATION: u16 = 203;
    pub const NO_CONTENT: u16 = 204;
    pub const RESET_CONTENT: u16 = 205;
    pub const PARTIAL_CONTENT: u16 = 206;
    pub const MULTI_STATUS: u16 = 207;
    pub const ALREADY_REPORTED: u16 = 208;
    pub const IM_USED: u16 = 226;

    pub const MULTIPLE_CHOICES: u16 = 300;
    pub const MOVED_PERMANENTLY: u16 = 301;
    pub const FOUND: u16 = 302;
    pub const SEE_OTHER: u16 = 303;
    pub const NOT_MODIFIED: u16 = 304;
    pub const USE_PROXY: u16 = 305;
    pub const TEMPORARY_REDIRECT: u16 = 307;
    pub const PERMANENT_REDIRECT: u16 = 308;

    pub const BAD_REQUEST: u16 = 400;
    pub const UNAUTHORIZED: u16 = 401;
    pub const PAYMENT_REQUIRED: u16 = 402;
    pub const FORBIDDEN: u16 = 403;
    pub const NOT_FOUND: u16 = 404;
    pub const METHOD_NOT_ALLOWED: u16 = 405;
    pub const NOT_ACCEPTABLE: u16 = 406;
    pub const PROXY_AUTHENTICATION_REQUIRED: u16 = 407;
    pub const REQUEST_TIMEOUT: u16 = 408;
    pub const CONFLICT: u16 = 409;
    pub const GONE: u16 = 410;
    pub const LENGTH_REQUIRED: u16 = 411;
    pub const PRECONDITION_FAILED: u16 = 412;
    pub const PAYLOAD_TOO_LARGE: u16 = 413;
    pub const URI_TOO_LONG: u16 = 414;
    pub const UNSUPPORTED_MEDIA_TYPE: u16 = 415;
    pub const RANGE_NOT_SATISFIABLE: u16 = 416;
    pub const EXPECTATION_FAILED: u16 = 417;
    pub const IM_A_TEAPOT: u16 = 418;
    pub const MISDIRECTED_REQUEST: u16 = 421;
    pub const UNPROCESSABLE_ENTITY: u16 = 422;
    pub const LOCKED: u16 = 423;
    pub const FAILED_DEPENDENCY: u16 = 424;
    pub const TOO_EARLY: u16 = 425;
    pub const UPGRADE_REQUIRED: u16 = 426;
    pub const PRECONDITION_REQUIRED: u16 = 428;
    pub const TOO_MANY_REQUESTS: u16 = 429;
    pub const REQUEST_HEADER_FIELDS_TOO_LARGE: u16 = 431;
    pub const UNAVAILABLE_FOR_LEGAL_REASONS: u16 = 451;

    pub const INTERNAL_SERVER_ERROR: u16 = 500;
    pub const NOT_IMPLEMENTED: u16 = 501;
    pub const BAD_GATEWAY: u16 = 502;
    pub const SERVICE_UNAVAILABLE: u16 = 503;
    pub const GATEWAY_TIMEOUT: u16 = 504;
    pub const HTTP_VERSION_NOT_SUPPORTED: u16 = 505;
    pub const VARIANT_ALSO_NEGOTIATES: u16 = 506;
    pub const INSUFFICIENT_STORAGE: u16 = 507;
    pub const LOOP_DETECTED: u16 = 508;
    pub const NOT_EXTENDED: u16 = 510;
    pub const NETWORK_AUTHENTICATION_REQUIRED: u16 = 511;
};

/// HTTP response status with code and reason phrase.
pub const Status = struct {
    code: u16,
    phrase: []const u8,

    const Self = @This();

    /// Creates a status from a numeric code with the standard reason phrase.
    pub fn fromCode(code: u16) Self {
        return .{ .code = code, .phrase = reasonPhrase(code) };
    }

    /// Creates a status with a custom reason phrase.
    pub fn withPhrase(code: u16, phrase: []const u8) Self {
        return .{ .code = code, .phrase = phrase };
    }

    /// Returns true if the status indicates a successful response (2xx).
    pub fn isSuccess(self: Self) bool {
        return self.code >= 200 and self.code < 300;
    }

    /// Returns true if the status indicates a redirect (3xx).
    pub fn isRedirect(self: Self) bool {
        return self.code >= 300 and self.code < 400;
    }

    /// Returns true if the status indicates a client error (4xx).
    pub fn isClientError(self: Self) bool {
        return self.code >= 400 and self.code < 500;
    }

    /// Returns true if the status indicates a server error (5xx).
    pub fn isServerError(self: Self) bool {
        return self.code >= 500 and self.code < 600;
    }

    /// Returns true if the status indicates any error (4xx or 5xx).
    pub fn isError(self: Self) bool {
        return self.code >= 400;
    }

    /// Returns true if the status is informational (1xx).
    pub fn isInformational(self: Self) bool {
        return self.code >= 100 and self.code < 200;
    }

    /// Returns true if this is a redirect that should change method to GET.
    pub fn redirectChangesMethod(self: Self) bool {
        return self.code == 301 or self.code == 302 or self.code == 303;
    }

    /// Returns true if the response may have a body.
    pub fn mayHaveBody(self: Self) bool {
        return self.code != 204 and self.code != 304 and !self.isInformational();
    }
};

/// Returns the standard reason phrase for an HTTP status code.
pub fn reasonPhrase(code: u16) []const u8 {
    return switch (code) {
        100 => "Continue",
        101 => "Switching Protocols",
        102 => "Processing",
        103 => "Early Hints",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        203 => "Non-Authoritative Information",
        204 => "No Content",
        205 => "Reset Content",
        206 => "Partial Content",
        207 => "Multi-Status",
        208 => "Already Reported",
        226 => "IM Used",
        300 => "Multiple Choices",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        305 => "Use Proxy",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        407 => "Proxy Authentication Required",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Range Not Satisfiable",
        417 => "Expectation Failed",
        418 => "I'm a teapot",
        421 => "Misdirected Request",
        422 => "Unprocessable Entity",
        423 => "Locked",
        424 => "Failed Dependency",
        425 => "Too Early",
        426 => "Upgrade Required",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        451 => "Unavailable For Legal Reasons",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        506 => "Variant Also Negotiates",
        507 => "Insufficient Storage",
        508 => "Loop Detected",
        510 => "Not Extended",
        511 => "Network Authentication Required",
        else => "Unknown Status",
    };
}

test "Status classification" {
    const ok = Status.fromCode(200);
    try std.testing.expect(ok.isSuccess());
    try std.testing.expect(!ok.isError());

    const redirect = Status.fromCode(301);
    try std.testing.expect(redirect.isRedirect());

    const client_err = Status.fromCode(404);
    try std.testing.expect(client_err.isClientError());
    try std.testing.expect(client_err.isError());

    const server_err = Status.fromCode(500);
    try std.testing.expect(server_err.isServerError());
    try std.testing.expect(server_err.isError());
}

test "Status reason phrases" {
    try std.testing.expectEqualStrings("OK", reasonPhrase(200));
    try std.testing.expectEqualStrings("Not Found", reasonPhrase(404));
    try std.testing.expectEqualStrings("Internal Server Error", reasonPhrase(500));
}

test "Status body detection" {
    const no_content = Status.fromCode(204);
    try std.testing.expect(!no_content.mayHaveBody());

    const ok = Status.fromCode(200);
    try std.testing.expect(ok.mayHaveBody());
}
