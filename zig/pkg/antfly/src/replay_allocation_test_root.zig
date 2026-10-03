test {
    _ = @import("storage/db/lookup_key_scratch.zig");
    _ = @import("storage/db/doc_identity.zig");
    _ = @import("storage/db/document_read_scratch.zig");
    _ = @import("storage/db/derived/derived_worker.zig");
    _ = @import("storage/db/derived/change_journal.zig");
}
