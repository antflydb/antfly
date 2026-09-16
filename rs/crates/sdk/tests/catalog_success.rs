use antfly_sdk::types::*;

#[test]
fn catalog_success_unions_preserve_pending_visibility() {
    let body = r#"{"status":"committed_visibility_pending"}"#;
    macro_rules! pending {
        ($ty:ident) => {
            assert!(matches!(
                serde_json::from_str::<$ty>(body).unwrap(),
                $ty::CatalogMutationVisibilityPending(_)
            ));
            assert!(serde_json::from_str::<$ty>(r#"{"status":"not_a_success"}"#).is_err());
        };
    }
    pending!(CreateDatabaseSuccess);
    pending!(CreateNamespaceSuccess);
    pending!(CreateTablespaceSuccess);
    pending!(SetDatabaseTablespaceSuccess);
    pending!(ClearDatabaseTablespaceSuccess);
    pending!(SetNamespaceTablespaceSuccess);
    pending!(ClearNamespaceTablespaceSuccess);
}

#[test]
fn created_resource_remains_a_typed_success() {
    let body = r#"{"database_id":7,"name":"analytics","settings_json":"{}"}"#;
    match serde_json::from_str::<CreateDatabaseSuccess>(body).unwrap() {
        CreateDatabaseSuccess::DatabaseCatalogRecord(record) => {
            assert_eq!(record.database_id, 7);
            assert_eq!(record.name, "analytics");
        }
        _ => panic!("created database must retain its resource payload"),
    }
}
