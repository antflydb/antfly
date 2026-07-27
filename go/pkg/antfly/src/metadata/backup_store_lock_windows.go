// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//go:build windows

package metadata

import (
	"context"
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

type repositoryFileLock struct {
	file *os.File
}

func lockRepositoryFile(
	ctx context.Context,
	root *os.Root,
	name string,
) (*repositoryFileLock, error) {
	if err := validateRepositoryRelativePath(name); err != nil {
		return nil, err
	}
	file, err := root.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	retry := time.NewTicker(10 * time.Millisecond)
	defer retry.Stop()
	for {
		err = windows.LockFileEx(
			windows.Handle(file.Fd()),
			windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
			0,
			1,
			0,
			&windows.Overlapped{},
		)
		if err == nil || errors.Is(err, windows.Errno(0)) {
			if err := ctx.Err(); err != nil {
				_ = windows.UnlockFileEx(
					windows.Handle(file.Fd()),
					0,
					1,
					0,
					&windows.Overlapped{},
				)
				_ = file.Close()
				return nil, err
			}
			return &repositoryFileLock{file: file}, nil
		}
		if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) &&
			!errors.Is(err, windows.ERROR_IO_PENDING) {
			_ = file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-retry.C:
		}
	}
}

func (l *repositoryFileLock) Close() error {
	unlockErr := windows.UnlockFileEx(
		windows.Handle(l.file.Fd()),
		0,
		1,
		0,
		&windows.Overlapped{},
	)
	closeErr := l.file.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}
