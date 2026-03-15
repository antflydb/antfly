use pgrx::prelude::*;
use pgrx::trigger_support::PgTriggerOperation;
use std::num::NonZero;

/// Install a trigger that syncs row changes to an Antfly table.
///
/// Usage:
///   CREATE TRIGGER sync_to_antfly
///     AFTER INSERT OR UPDATE OR DELETE ON my_table
///     FOR EACH ROW
///     EXECUTE FUNCTION antfly_sync_trigger('http://localhost:8080/api/v1/', 'my_table', 'id');
///
/// Arguments:
///   1. base_url  - Antfly server URL (including /api/v1/ prefix)
///   2. table     - Target Antfly table name
///   3. id_column - Column to use as the document ID (default: "id")
#[pg_trigger]
fn antfly_sync_trigger<'a>(
    trigger: &'a pgrx::PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, pgrx::spi::Error> {
    let args = trigger.extra_args().unwrap_or_else(|_| vec![]);

    let base_url = args.first().unwrap_or_else(|| {
        pgrx::error!("pgaf: antfly_sync_trigger requires base_url as first argument");
    });
    let collection = args.get(1).unwrap_or_else(|| {
        pgrx::error!("pgaf: antfly_sync_trigger requires collection as second argument");
    });
    let id_column = args.get(2).map(|s| s.as_str()).unwrap_or("id");

    let client = crate::client::AntflyClient::new(base_url).unwrap_or_else(|e| {
        pgrx::error!("pgaf: failed to create client: {}", e);
    });

    let op = trigger.op().unwrap_or_else(|_| {
        pgrx::error!("pgaf: could not determine trigger operation");
    });

    // Handle DELETE: use OLD row
    if matches!(op, PgTriggerOperation::Delete) {
        if let Some(old) = trigger.old() {
            let doc_id = get_id_from_tuple(&old, id_column);
            if let Err(e) = client.delete_document(collection, &doc_id) {
                pgrx::warning!("pgaf: failed to delete from antfly: {}", e);
            }
        }
        return Ok(None);
    }

    // Handle INSERT/UPDATE: use NEW row
    if let Some(new) = trigger.new() {
        let doc_id = get_id_from_tuple(&new, id_column);
        let doc = heap_tuple_to_json(&new);

        if let Err(e) = client.sync_document(collection, &doc_id, &doc) {
            pgrx::warning!("pgaf: failed to sync to antfly: {}", e);
        }

        return Ok(Some(new));
    }

    Ok(None)
}

/// Extract the ID value from a heap tuple by column name.
fn get_id_from_tuple(tuple: &PgHeapTuple<'_, impl WhoAllocated>, id_column: &str) -> String {
    tuple
        .get_by_name::<String>(id_column)
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            tuple
                .get_by_name::<i64>(id_column)
                .ok()
                .flatten()
                .map(|v| v.to_string())
                .unwrap_or_else(|| {
                    pgrx::error!(
                        "pgaf: could not read '{}' column as text or bigint",
                        id_column
                    );
                })
        })
}

/// Convert a heap tuple to a JSON object by iterating its attributes.
fn heap_tuple_to_json(tuple: &PgHeapTuple<'_, impl WhoAllocated>) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for i in 1..=tuple.len() {
        let Some(index) = NonZero::new(i) else {
            continue;
        };

        let attr_name = match tuple.get_attribute_by_index(index) {
            Some(attr) => attr.name().to_string(),
            None => continue,
        };

        if let Ok(Some(v)) = tuple.get_by_index::<String>(index) {
            map.insert(attr_name, serde_json::Value::String(v));
        } else if let Ok(Some(v)) = tuple.get_by_index::<i64>(index) {
            map.insert(attr_name, serde_json::json!(v));
        } else if let Ok(Some(v)) = tuple.get_by_index::<i32>(index) {
            map.insert(attr_name, serde_json::json!(v));
        } else if let Ok(Some(v)) = tuple.get_by_index::<f64>(index) {
            map.insert(attr_name, serde_json::json!(v));
        } else if let Ok(Some(v)) = tuple.get_by_index::<bool>(index) {
            map.insert(attr_name, serde_json::json!(v));
        } else if let Ok(Some(v)) = tuple.get_by_index::<pgrx::JsonB>(index) {
            map.insert(attr_name, v.0);
        }
    }

    serde_json::Value::Object(map)
}
