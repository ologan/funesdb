// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners_test

import (
	"path/filepath"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

const thpDir string = "/sys/kernel/mm/transparent_hugepage"

func TestTHPTunerSupported(t *testing.T) {
	tests := []struct {
		name           string
		thpDir         string
		expected       bool
		expectedReason string
	}{
		{
			name:     "should return true if the default dir exists",
			thpDir:   thpDir,
			expected: true,
		},
		{
			name:     "should return true if the RHEL-specific dir exists",
			thpDir:   "/sys/kernel/mm/redhat_transparent_hugepage",
			expected: true,
		},
		{
			name:           "should return false if no dir exists",
			expected:       false,
			expectedReason: "None of /sys/kernel/mm/transparent_hugepage, /sys/kernel/mm/redhat_transparent_hugepage was found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()

			if tt.thpDir != "" {
				err := fs.MkdirAll(tt.thpDir, 0o755)
				require.NoError(st, err)
			}
			exec := executors.NewDirectExecutor()
			tuner := tuners.NewEnableTHPTuner(fs, exec)
			supported, reason := tuner.CheckIfSupported()
			require.Equal(st, tt.expected, supported)
			require.Equal(st, tt.expectedReason, reason)
		})
	}
}

func TestTHPTunerScriptExecutor(t *testing.T) {
	expected := `#!/bin/bash

# Funes Tuning Script
# ----------------------------------
# This file was autogenerated by RPK

echo 'always' > /sys/kernel/mm/transparent_hugepage/enabled
`
	fs := afero.NewMemMapFs()
	scriptFileName := "script.sh"
	exec := executors.NewScriptRenderingExecutor(fs, scriptFileName)
	dir := thpDir
	err := fs.MkdirAll(dir, 0o755)
	require.NoError(t, err)

	_, err = fs.Create(filepath.Join(dir, "enabled"))
	require.NoError(t, err)

	tuner := tuners.NewEnableTHPTuner(fs, exec)

	res := tuner.Tune()
	require.False(t, res.IsFailed())

	bs, err := afero.ReadFile(fs, scriptFileName)
	require.NoError(t, err)

	require.Equal(t, expected, string(bs))
}

func TestTHPTunerDirectExecutor(t *testing.T) {
	// The file is on sysfs, and when printed, its contents are printed
	// differently, showing the valid options and the chosen option wrapped
	// in brackets, so in reality it would look like this:
	//
	// [always] madvise never
	//
	// This expected value is meant only for this test, which uses an
	// fs.MemMapFs
	expected := "always"
	fs := afero.NewMemMapFs()
	exec := executors.NewDirectExecutor()
	dir := thpDir
	filePath := filepath.Join(dir, "enabled")
	err := fs.MkdirAll(dir, 0o755)
	require.NoError(t, err)

	_, err = fs.Create(filePath)
	require.NoError(t, err)

	tuner := tuners.NewEnableTHPTuner(fs, exec)

	res := tuner.Tune()
	require.False(t, res.IsFailed())
	require.False(t, res.IsRebootRequired())

	bs, err := afero.ReadFile(fs, filePath)
	require.NoError(t, err)

	require.Equal(t, expected, string(bs))
}

func TestTHPCheckID(t *testing.T) {
	c := tuners.NewTransparentHugePagesChecker(afero.NewMemMapFs())
	require.Equal(t, tuners.CheckerID(tuners.TransparentHugePagesChecker), c.ID())
}

func TestTHPCheck(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		expected bool
	}{
		{
			name:     "should return true if the active value is 'always'",
			contents: "[always] madvise never",
			expected: true,
		},
		{
			name:     "should return true if the active value is 'madvise'",
			contents: "always [madvise] never",
			expected: true,
		},
		{
			name:     "should return false if the active value is 'never'",
			contents: "always madvise [never]",
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()
			dir := thpDir
			err := fs.MkdirAll(dir, 0o755)
			require.NoError(t, err)

			f, err := fs.Create(filepath.Join(dir, "enabled"))
			require.NoError(t, err)
			defer f.Close()
			_, err = f.Write([]byte(tt.contents))
			require.NoError(t, err)
			c := tuners.NewTransparentHugePagesChecker(fs)
			res := c.Check()
			require.Equal(t, tt.expected, res.IsOk)
		})
	}
}
