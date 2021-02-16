// Copyright 2021 Andrew Archibald. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package walk

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

func (w *gitignoreWalker) loadGlobalGitignore() error {
	fsys := osfs.New("/")
	system, err := gitignore.LoadSystemPatterns(fsys)
	if err != nil {
		return err
	}
	global, err := gitignore.LoadGlobalPatterns(fsys)
	if err != nil {
		return err
	}
	ps := global
	if len(system) != 0 {
		ps = append(system, global...)
	}
	w.ps = ps
	w.m = gitignore.NewMatcher(ps)
	return nil
}

// readGitignore reads a specific git ignore file.
func (w *gitignoreWalker) readGitignore(path string, pathSplit []string) error {
	f, err := os.Open(filepath.Join(path, ".gitignore"))
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if !strings.HasPrefix(line, "#") && len(strings.TrimSpace(line)) > 0 {
			w.ps = append(w.ps, gitignore.ParsePattern(line, pathSplit))
		}
	}
	w.m = gitignore.NewMatcher(w.ps)
	return s.Err()
}
