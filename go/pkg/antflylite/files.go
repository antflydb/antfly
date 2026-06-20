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

// RestoreBackupFile creates or replaces a Lite database from a portable Antfly
// backup archive.
func RestoreBackupFile(path, backupPath string, replace bool) error {
	if !strings.HasSuffix(backupPath, ".afb") {
		return InvalidArgument
	}
	backup, err := os.ReadFile(backupPath)
	if err != nil {
		return err
	}
	return RestoreBackup(path, backup, replace)
}

// RestoreBackup creates or replaces a Lite database from a portable Antfly
// backup archive.
func RestoreBackup(path string, backup []byte, replace bool) error {
	if !strings.HasSuffix(path, ".aflite") || len(backup) == 0 {
		return InvalidArgument
	}
	if err := preflightRestoreTarget(path, replace); err != nil {
		return err
	}

	tmpPath, err := reserveTempAflitePath(path)
	if err != nil {
		return err
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	db, err := Open(tmpPath)
	if err != nil {
		return err
	}
	if err := db.ImportBackup(backup); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}

	if err := preflightRestoreTarget(path, replace); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanupTmp = false
	return nil
}

func preflightRestoreTarget(path string, replace bool) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !replace {
		return fs.ErrExist
	}
	db, err := Open(path)
	if err != nil {
		return err
	}
	return db.Close()
}

func reserveTempAflitePath(path string) (string, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	file, err := os.CreateTemp(dir, "."+base+".*.restore-tmp.aflite")
	if err != nil {
		return "", err
	}
	tmpPath := file.Name()
	closeErr := file.Close()
	removeErr := os.Remove(tmpPath)
	if closeErr != nil {
		return "", closeErr
	}
	if removeErr != nil {
		return "", removeErr
	}
	return tmpPath, nil
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
