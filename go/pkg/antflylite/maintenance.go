// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//go:build cgo

package antflylite

import "encoding/json"

// CheckReport is the typed form of CheckJSON.
type CheckReport struct {
	Valid            bool    `json:"valid"`
	FileSize         uint64  `json:"file_size"`
	ValidPrefixSize  uint64  `json:"valid_prefix_size"`
	TailBytes        uint64  `json:"tail_bytes"`
	RecordCount      uint64  `json:"record_count"`
	LiveFileCount    uint64  `json:"live_file_count"`
	LiveBytes        uint64  `json:"live_bytes"`
	CompactSize      uint64  `json:"compact_size"`
	ReclaimableBytes uint64  `json:"reclaimable_bytes"`
	Issue            *string `json:"issue,omitempty"`
}

// VacuumReport is the typed form of VacuumJSON.
type VacuumReport struct {
	BeforeSize     uint64 `json:"before_size"`
	AfterSize      uint64 `json:"after_size"`
	ReclaimedBytes uint64 `json:"reclaimed_bytes"`
	LiveFileCount  uint64 `json:"live_file_count"`
	LiveBytes      uint64 `json:"live_bytes"`
}

// CompactReport is the typed form of CompactJSON.
type CompactReport struct {
	Compacted bool         `json:"compacted"`
	Vacuum    VacuumReport `json:"vacuum"`
}

// StableSnapshotReport is the typed form of CopyStableSnapshotJSON.
type StableSnapshotReport struct {
	SourceSize         uint64 `json:"source_size"`
	SnapshotSize       uint64 `json:"snapshot_size"`
	CheckpointSequence uint64 `json:"checkpoint_sequence"`
	PageCount          uint64 `json:"page_count"`
	TailBytes          uint64 `json:"tail_bytes"`
}

// Check runs Lite integrity checks and returns the typed result.
func (db *DB) Check() (*CheckReport, error) {
	body, err := db.CheckJSON()
	if err != nil {
		return nil, err
	}
	var report CheckReport
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// CheckFile runs Lite integrity checks for path without opening a database
// handle and returns the typed result.
func CheckFile(path string) (*CheckReport, error) {
	body, err := CheckFileJSON(path)
	if err != nil {
		return nil, err
	}
	var report CheckReport
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// Vacuum compacts free space and returns the typed result.
func (db *DB) Vacuum() (*VacuumReport, error) {
	body, err := db.VacuumJSON()
	if err != nil {
		return nil, err
	}
	var report VacuumReport
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// Compact drains maintenance, compacts indexes, vacuums free space, and returns
// the typed result.
func (db *DB) Compact() (*CompactReport, error) {
	body, err := db.CompactJSON()
	if err != nil {
		return nil, err
	}
	var report CompactReport
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, err
	}
	return &report, nil
}

// CopyStableSnapshot copies a stable Lite snapshot to destPath and returns the
// typed result.
func (db *DB) CopyStableSnapshot(destPath string, replace bool) (*StableSnapshotReport, error) {
	body, err := db.CopyStableSnapshotJSON(destPath, replace)
	if err != nil {
		return nil, err
	}
	var report StableSnapshotReport
	if err := json.Unmarshal(body, &report); err != nil {
		return nil, err
	}
	return &report, nil
}
