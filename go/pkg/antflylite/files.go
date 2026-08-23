// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//go:build cgo

package antflylite

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// BackupToFile writes a portable Antfly backup archive for this Lite database.
func (db *DB) BackupToFile(path string) error {
	if !strings.HasSuffix(path, ".afb") {
		return InvalidArgument
	}
	backup, err := db.Backup()
	if err != nil {
		return err
	}
	return writeFileAtomically(path, backup, 0o600)
}

// ExportToFile writes a portable Antfly backup archive for this Lite database.
func (db *DB) ExportToFile(path string) error {
	if !strings.HasSuffix(path, ".afb") {
		return InvalidArgument
	}
	backup, err := db.Export()
	if err != nil {
		return err
	}
	return writeFileAtomically(path, backup, 0o600)
}

// CopyStableSnapshotFile opens srcPath read-only, copies a stable physical
// .aflite snapshot to destPath, and returns the typed snapshot report.
func CopyStableSnapshotFile(srcPath, destPath string, replace bool) (*StableSnapshotReport, error) {
	if !strings.HasSuffix(srcPath, ".aflite") || !strings.HasSuffix(destPath, ".aflite") {
		return nil, InvalidArgument
	}
	body, err := CopyStableSnapshotFileJSON(srcPath, destPath, replace)
	if err != nil {
		return nil, err
	}
	var report StableSnapshotReport
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// RestoreBackupFile creates or replaces a Lite database by streaming a
// portable Antfly backup archive with bounded memory use. Busy means the source
// changed during streaming or the source/destination is concurrently locked;
// retry after the files are stable and no writer is active. Unsupported means
// the source filesystem lacks required advisory locking; copy the archive to a
// supported local filesystem. OutcomeUnknown means the destination was
// published but crash durability could not be confirmed; inspect it and do not
// retry automatically.
func RestoreBackupFile(path, backupPath string, replace bool) error {
	if !strings.HasSuffix(path, ".aflite") || !strings.HasSuffix(backupPath, ".afb") {
		return InvalidArgument
	}
	return restoreBackupFileToFile(path, backupPath, replace)
}

// RestoreBackup creates or replaces a Lite database from a portable Antfly
// backup archive. OutcomeUnknown means the destination was published but crash
// durability could not be confirmed; inspect it and do not retry automatically.
func RestoreBackup(path string, backup []byte, replace bool) error {
	if !strings.HasSuffix(path, ".aflite") || len(backup) == 0 {
		return InvalidArgument
	}
	return restoreBackupToFile(path, backup, replace)
}

// RestoreFile creates or replaces a Lite database by streaming a portable
// Antfly backup archive with bounded memory use. Busy means the source changed
// during streaming or the source/destination is concurrently locked; retry
// after the files are stable and no writer is active. Unsupported means the
// source filesystem lacks required advisory locking; copy the archive to a
// supported local filesystem. OutcomeUnknown means the destination was
// published but crash durability could not be confirmed; inspect it and do not
// retry automatically.
func RestoreFile(path, backupPath string, replace bool) error {
	if !strings.HasSuffix(path, ".aflite") || !strings.HasSuffix(backupPath, ".afb") {
		return InvalidArgument
	}
	return restoreBackupFileToFile(path, backupPath, replace)
}

// Restore creates or replaces a Lite database from a portable Antfly backup
// archive. OutcomeUnknown means the destination was published but crash
// durability could not be confirmed; inspect it and do not retry automatically.
func Restore(path string, backup []byte, replace bool) error {
	if !strings.HasSuffix(path, ".aflite") || len(backup) == 0 {
		return InvalidArgument
	}
	return restoreToFile(path, backup, replace)
}

func writeFileAtomically(path string, data []byte, perm fs.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	file, err := os.CreateTemp(dir, "."+base+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := file.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(perm); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanupTmp = false
	return nil
}
