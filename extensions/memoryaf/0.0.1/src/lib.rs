wit_bindgen::generate!({
    path: "wit",
    world: "extension",
});

struct MemoryAf;

impl Guest for MemoryAf {
    fn init(_config_json: String) -> Result<(), String> {
        Ok(())
    }

    fn call_tool(
        name: String,
        request_json: String,
    ) -> Result<antfly::extension::mcp::ToolResult, String> {
        let content_json = match name.as_str() {
            "store_memory" => store_memory(&request_json),
            "search_memories" => search_memories(&request_json),
            "list_memories" => list_memories(&request_json),
            other => error_json("unknown_tool", &format!("unknown memoryaf tool: {other}")),
        };
        Ok(antfly::extension::mcp::ToolResult { content_json })
    }
}

export!(MemoryAf);

fn store_memory(request_json: &str) -> String {
    let content = json_string_field(request_json, "content").unwrap_or_default();
    let memory_type = json_string_field(request_json, "memory_type").unwrap_or("semantic");
    if content.is_empty() {
        return error_json("invalid_request", "store_memory requires non-empty content");
    }
    let embedding = host_embed("default", content);
    let write_result = host_db_write(
        "memory_record",
        &format!(
            "{{\"id\":{},\"content\":{},\"memory_type\":{},\"visibility\":{},\"project\":{}}}",
            json_quote(&memory_id(content)),
            json_quote(content),
            json_quote(memory_type),
            json_quote(json_string_field(request_json, "visibility").unwrap_or("team")),
            json_quote(json_string_field(request_json, "project").unwrap_or("default"))
        ),
    );
    format!(
        "{{\"ok\":true,\"tool\":\"store_memory\",\"status\":\"stored\",\"memory\":{{\"id\":{},\"content\":{},\"memory_type\":{},\"visibility\":{},\"project\":{}}},\"host_calls\":[\"db.write(memory_record)\",\"ai.embed(content)\"],\"host_results\":{{\"embedding_dimensions\":{},\"write\":{}}}}}",
        json_quote(&memory_id(content)),
        json_quote(content),
        json_quote(memory_type),
        json_quote(json_string_field(request_json, "visibility").unwrap_or("team")),
        json_quote(json_string_field(request_json, "project").unwrap_or("default")),
        embedding.as_ref().map(|values| values.len()).unwrap_or(0),
        write_result.unwrap_or_else(|err| format!("{{\"ok\":false,\"error\":{}}}", json_quote(&err)))
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
    let embedding = host_embed("default", query);
    let db_result = host_db_query(
        "memory_record",
        &format!(
            "{{\"query\":{},\"limit\":{},\"embedding_dimensions\":{}}}",
            json_quote(query),
            json_number_field(request_json, "limit").unwrap_or("10"),
            embedding.as_ref().map(|values| values.len()).unwrap_or(0)
        ),
    );
    format!(
        "{{\"ok\":true,\"tool\":\"search_memories\",\"status\":\"planned\",\"query\":{},\"limit\":{},\"host_calls\":[\"ai.embed(query)\",\"db.query(memory_record)\"],\"host_results\":{{\"embedding_dimensions\":{},\"rows\":{}}}}}",
        json_quote(query),
        json_number_field(request_json, "limit").unwrap_or("10"),
        embedding.as_ref().map(|values| values.len()).unwrap_or(0),
        db_result.unwrap_or_else(|_| "[]".to_string())
    )
}

fn list_memories(request_json: &str) -> String {
    let db_result = host_db_query(
        "memory_record",
        &format!(
            "{{\"project\":{},\"limit\":{},\"offset\":{}}}",
            json_quote(json_string_field(request_json, "project").unwrap_or("")),
            json_number_field(request_json, "limit").unwrap_or("20"),
            json_number_field(request_json, "offset").unwrap_or("0")
        ),
    );
    format!(
        "{{\"ok\":true,\"tool\":\"list_memories\",\"status\":\"planned\",\"project\":{},\"limit\":{},\"offset\":{},\"host_calls\":[\"db.query(memory_record)\"],\"host_results\":{{\"rows\":{}}}}}",
        json_quote(json_string_field(request_json, "project").unwrap_or("")),
        json_number_field(request_json, "limit").unwrap_or("20"),
        json_number_field(request_json, "offset").unwrap_or("0"),
        db_result.unwrap_or_else(|_| "[]".to_string())
    )
}

#[cfg(target_arch = "wasm32")]
fn host_db_query(table: &str, query_json: &str) -> Result<String, String> {
    antfly::extension::db::query(table, query_json)
}

#[cfg(not(target_arch = "wasm32"))]
fn host_db_query(_table: &str, _query_json: &str) -> Result<String, String> {
    Ok("[]".to_string())
}

#[cfg(target_arch = "wasm32")]
fn host_db_write(table: &str, writes_json: &str) -> Result<String, String> {
    antfly::extension::db::write(table, writes_json)
}

#[cfg(not(target_arch = "wasm32"))]
fn host_db_write(_table: &str, _writes_json: &str) -> Result<String, String> {
    Ok("{\"ok\":true}".to_string())
}

#[cfg(target_arch = "wasm32")]
fn host_embed(model: &str, text: &str) -> Result<Vec<f32>, String> {
    antfly::extension::ai::embed(model, text)
}

#[cfg(not(target_arch = "wasm32"))]
fn host_embed(_model: &str, _text: &str) -> Result<Vec<f32>, String> {
    Ok(Vec::new())
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

fn memory_id(content: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in content.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("memory:{hash:016x}")
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
        assert!(response.contains("\"id\":\"memory:"));
    }

    #[test]
    fn search_requires_query() {
        let response = search_memories("{}");
        assert!(response.contains(r#""ok":false"#));
        assert!(response.contains("requires non-empty query"));
    }
}
