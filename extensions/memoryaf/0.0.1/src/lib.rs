use std::alloc::{alloc, dealloc, Layout};
use std::slice;
use std::str;

#[repr(C)]
pub struct WasmBuffer {
    ptr: *mut u8,
    len: usize,
}

#[no_mangle]
pub extern "C" fn memoryaf_alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return std::ptr::null_mut();
    }
    let layout = match Layout::array::<u8>(len) {
        Ok(layout) => layout,
        Err(_) => return std::ptr::null_mut(),
    };
    unsafe { alloc(layout) }
}

#[no_mangle]
pub unsafe extern "C" fn memoryaf_dealloc(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    if let Ok(layout) = Layout::array::<u8>(len) {
        dealloc(ptr, layout);
    }
}

#[no_mangle]
pub unsafe extern "C" fn memoryaf_free_buffer(buffer: WasmBuffer) {
    memoryaf_dealloc(buffer.ptr, buffer.len);
}

#[no_mangle]
pub unsafe extern "C" fn memoryaf_call_tool(
    name_ptr: *const u8,
    name_len: usize,
    request_ptr: *const u8,
    request_len: usize,
) -> WasmBuffer {
    let name = read_utf8(name_ptr, name_len);
    let request = read_utf8(request_ptr, request_len);
    let response = match (name, request) {
        (Err(message), _) => error_json("invalid_tool_name", message),
        (_, Err(message)) => error_json("invalid_request", message),
        (Ok("store_memory"), Ok(request)) => store_memory(request),
        (Ok("search_memories"), Ok(request)) => search_memories(request),
        (Ok("list_memories"), Ok(request)) => list_memories(request),
        (Ok(other), Ok(_)) => {
            error_json("unknown_tool", &format!("unknown memoryaf tool: {other}"))
        }
    };
    into_buffer(response)
}

unsafe fn read_utf8<'a>(ptr: *const u8, len: usize) -> Result<&'a str, &'static str> {
    if ptr.is_null() && len != 0 {
        return Err("non-empty buffer had null pointer");
    }
    let bytes = if len == 0 {
        &[]
    } else {
        slice::from_raw_parts(ptr, len)
    };
    str::from_utf8(bytes).map_err(|_| "buffer was not valid utf-8")
}

fn into_buffer(value: String) -> WasmBuffer {
    let mut bytes = value.into_bytes();
    let buffer = WasmBuffer {
        ptr: bytes.as_mut_ptr(),
        len: bytes.len(),
    };
    std::mem::forget(bytes);
    buffer
}

fn store_memory(request_json: &str) -> String {
    let content = json_string_field(request_json, "content").unwrap_or_default();
    let memory_type = json_string_field(request_json, "memory_type").unwrap_or("semantic");
    if content.is_empty() {
        return error_json("invalid_request", "store_memory requires non-empty content");
    }
    format!(
        "{{\"ok\":true,\"tool\":\"store_memory\",\"status\":\"planned\",\"memory\":{{\"content\":{},\"memory_type\":{},\"visibility\":{},\"project\":{}}},\"host_calls\":[\"db.write(memory_record)\",\"ai.embed(content)\"]}}",
        json_quote(content),
        json_quote(memory_type),
        json_quote(json_string_field(request_json, "visibility").unwrap_or("team")),
        json_quote(json_string_field(request_json, "project").unwrap_or("default"))
    )
}

fn search_memories(request_json: &str) -> String {
    let query = json_string_field(request_json, "query").unwrap_or_default();
    if query.is_empty() {
        return error_json(
            "invalid_request",
            "search_memories requires non-empty query",
        );
    }
    format!(
        "{{\"ok\":true,\"tool\":\"search_memories\",\"status\":\"planned\",\"query\":{},\"limit\":{},\"host_calls\":[\"ai.embed(query)\",\"db.query(memory_record)\"]}}",
        json_quote(query),
        json_number_field(request_json, "limit").unwrap_or("10")
    )
}

fn list_memories(request_json: &str) -> String {
    format!(
        "{{\"ok\":true,\"tool\":\"list_memories\",\"status\":\"planned\",\"project\":{},\"limit\":{},\"offset\":{},\"host_calls\":[\"db.query(memory_record)\"]}}",
        json_quote(json_string_field(request_json, "project").unwrap_or("")),
        json_number_field(request_json, "limit").unwrap_or("20"),
        json_number_field(request_json, "offset").unwrap_or("0")
    )
}

fn error_json(code: &str, message: &str) -> String {
    format!(
        "{{\"ok\":false,\"error\":{{\"code\":{},\"message\":{}}}}}",
        json_quote(code),
        json_quote(message)
    )
}

fn json_string_field<'a>(input: &'a str, field: &str) -> Option<&'a str> {
    let needle = format!("\"{field}\"");
    let after_name = input.split_once(&needle)?.1;
    let after_colon = after_name.split_once(':')?.1.trim_start();
    let mut chars = after_colon.char_indices();
    if chars.next()?.1 != '"' {
        return None;
    }
    let mut escaped = false;
    for (idx, ch) in chars {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '"' => return Some(&after_colon[1..idx]),
            _ => {}
        }
    }
    None
}

fn json_number_field<'a>(input: &'a str, field: &str) -> Option<&'a str> {
    let needle = format!("\"{field}\"");
    let after_name = input.split_once(&needle)?.1;
    let after_colon = after_name.split_once(':')?.1.trim_start();
    let end = after_colon
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(after_colon.len());
    if end == 0 {
        return None;
    }
    Some(&after_colon[..end])
}

fn json_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_memory_plans_host_calls() {
        let response = store_memory(r#"{"content":"Use wasm components","project":"antfly"}"#);
        assert!(response.contains(r#""ok":true"#));
        assert!(response.contains(r#""db.write(memory_record)""#));
        assert!(response.contains(r#""Use wasm components""#));
    }

    #[test]
    fn search_requires_query() {
        let response = search_memories("{}");
        assert!(response.contains(r#""ok":false"#));
        assert!(response.contains("requires non-empty query"));
    }
}
