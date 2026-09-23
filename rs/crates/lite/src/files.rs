//! Free functions and `Database` methods that read or write Lite database
//! files directly (backup/export to disk, restore, path-level integrity
//! checks). Mirrors `go/pkg/lite/files.go` and the file-oriented helpers in
//! `go/pkg/lite/maintenance.go`.

use std::path::Path;

use antfly_lite_sys::{self as sys, antfly_buffer};

use crate::db::{Database, validate_abi};
use crate::error::{Error, Result};
use crate::ffi::{borrow_slice, check, path_has_suffix, path_to_cstring, take_buffer};

/// Runs Lite integrity checks for `path` without opening a database handle
/// and returns the JSON result.
pub fn check_file_json(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    validate_abi()?;
    let c_path = path_to_cstring(path.as_ref())?;
    let mut out = antfly_buffer::default();
    check(unsafe { sys::antfly_lite_check_file_json(c_path.as_ptr(), &mut out) })?;
    Ok(unsafe { take_buffer(out) })
}

/// Opens `src_path` read-only, copies a stable Lite snapshot to
/// `dest_path`, and returns the JSON result. Neither path is required to
/// end in `.aflite`; see [`crate::maintenance::copy_stable_snapshot_file`]
/// for the suffix-checked, typed convenience.
pub fn copy_stable_snapshot_file_json(
    src_path: impl AsRef<Path>,
    dest_path: impl AsRef<Path>,
    replace: bool,
) -> Result<Vec<u8>> {
    validate_abi()?;
    let c_src = path_to_cstring(src_path.as_ref())?;
    let c_dest = path_to_cstring(dest_path.as_ref())?;
    let mut out = antfly_buffer::default();
    check(unsafe {
        sys::antfly_lite_copy_stable_snapshot_file_json(
            c_src.as_ptr(),
            c_dest.as_ptr(),
            replace,
            &mut out,
        )
    })?;
    Ok(unsafe { take_buffer(out) })
}

fn restore_backup_to_file(path: &Path, backup: &[u8], replace: bool) -> Result<()> {
    let c_path = path_to_cstring(path)?;
    let mut out = antfly_buffer::default();
    check(unsafe {
        sys::antfly_lite_restore_backup_json(
            c_path.as_ptr(),
            borrow_slice(backup),
            replace,
            &mut out,
        )
    })?;
    unsafe { sys::antfly_buffer_free(&mut out) };
    Ok(())
}

fn restore_to_file(path: &Path, backup: &[u8], replace: bool) -> Result<()> {
    let c_path = path_to_cstring(path)?;
    let mut out = antfly_buffer::default();
    check(unsafe {
        sys::antfly_lite_restore_json(c_path.as_ptr(), borrow_slice(backup), replace, &mut out)
    })?;
    unsafe { sys::antfly_buffer_free(&mut out) };
    Ok(())
}

fn restore_backup_file_to_file(path: &Path, backup_path: &Path, replace: bool) -> Result<()> {
    let c_path = path_to_cstring(path)?;
    let c_backup_path = path_to_cstring(backup_path)?;
    let mut out = antfly_buffer::default();
    check(unsafe {
        sys::antfly_lite_restore_backup_file_json(
            c_path.as_ptr(),
            c_backup_path.as_ptr(),
            replace,
            &mut out,
        )
    })?;
    unsafe { sys::antfly_buffer_free(&mut out) };
    Ok(())
}

/// Creates or replaces a Lite database from a portable Antfly backup
/// archive. `path` must end in `.aflite` and `backup` must be non-empty.
/// [`Error::OutcomeUnknown`] means the destination was published but crash
/// durability could not be confirmed; inspect it and do not retry
/// automatically.
pub fn restore_backup(path: impl AsRef<Path>, backup: &[u8], replace: bool) -> Result<()> {
    let path = path.as_ref();
    if !path_has_suffix(path, ".aflite") || backup.is_empty() {
        return Err(Error::InvalidArgument);
    }
    restore_backup_to_file(path, backup, replace)
}

/// See [`restore_backup`]; kept for parity with the Go binding's
/// backup/export naming migration (both validate the same way and call
/// distinct but equivalent C ABI entry points).
pub fn restore(path: impl AsRef<Path>, backup: &[u8], replace: bool) -> Result<()> {
    let path = path.as_ref();
    if !path_has_suffix(path, ".aflite") || backup.is_empty() {
        return Err(Error::InvalidArgument);
    }
    restore_to_file(path, backup, replace)
}

/// Creates or replaces a Lite database by streaming a portable Antfly
/// backup archive file with bounded memory use. `path` must end in
/// `.aflite` and `backup_path` in `.afb`. [`Error::Busy`] means the source
/// changed during streaming or the source/destination is concurrently
/// locked; retry after the files are stable and no writer is active.
/// [`Error::Unsupported`] means the source filesystem lacks required
/// advisory locking; copy the archive to a supported local filesystem.
/// [`Error::OutcomeUnknown`] means the destination was published but crash
/// durability could not be confirmed; inspect it and do not retry
/// automatically.
pub fn restore_backup_file(
    path: impl AsRef<Path>,
    backup_path: impl AsRef<Path>,
    replace: bool,
) -> Result<()> {
    let path = path.as_ref();
    let backup_path = backup_path.as_ref();
    if !path_has_suffix(path, ".aflite") || !path_has_suffix(backup_path, ".afb") {
        return Err(Error::InvalidArgument);
    }
    restore_backup_file_to_file(path, backup_path, replace)
}

/// See [`restore_backup_file`]; both validate identically and call the same
/// underlying C ABI entry point (`antfly_lite_restore_backup_file_json`),
/// mirroring the Go binding's `RestoreFile`/`RestoreBackupFile` pair.
pub fn restore_file(
    path: impl AsRef<Path>,
    backup_path: impl AsRef<Path>,
    replace: bool,
) -> Result<()> {
    restore_backup_file(path, backup_path, replace)
}

/// Writes `data` to `path` atomically: a temp file in the same directory is
/// written, `fsync`'d, and renamed into place. I/O failures are reported as
/// [`Error::Internal`] (they are not `antfly_error_code` values).
fn write_file_atomically(path: &Path, data: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or(Error::InvalidArgument)?;
    let tmp_path = dir.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));

    let write_result = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(data)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.sync_all()
    })();

    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(Error::Internal);
    }

    std::fs::rename(&tmp_path, path).map_err(|_| {
        let _ = std::fs::remove_file(&tmp_path);
        Error::Internal
    })
}

impl Database {
    /// Writes a portable Antfly backup archive for this Lite database to
    /// `path`, which must end in `.afb`.
    pub fn backup_to_file(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if !path_has_suffix(path, ".afb") {
            return Err(Error::InvalidArgument);
        }
        let backup = self.backup()?;
        write_file_atomically(path, &backup)
    }

    /// Writes a portable Antfly backup archive for this Lite database to
    /// `path`, which must end in `.afb`. See [`Database::backup_to_file`].
    pub fn export_to_file(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if !path_has_suffix(path, ".afb") {
            return Err(Error::InvalidArgument);
        }
        let backup = self.export()?;
        write_file_atomically(path, &backup)
    }
}
