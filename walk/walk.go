// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package walk

import (
	"bufio"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// Modified from Go's filepath.WalkDir in path/filepath/path.go.
// filepath.WalkDir does not visit directories after its entries have
// been traversed, so a custom implementation is needed.

// SkipDir is used as a return value from a walk.Func to indicate that
// the directory named in the call is to be skipped. It is not returned
// as an error by any function.
var SkipDir = fs.SkipDir

// Func is the type of the function called by Walk to visit each file
// or directory.
type Func = fs.WalkDirFunc

type Walker interface {
	Walk(root string, fn Func) error
}

type walker struct{}

func NewWalker() Walker { return walker{} }

func (w walker) Walk(root string, fn Func) error {
	return filepath.WalkDir(root, fn)
}

type gitignoreWalker struct {
	ps []gitignore.Pattern
	m  gitignore.Matcher
}

func NewGitignoreWalker() (Walker, error) {
	var w gitignoreWalker
	if err := w.loadGlobalGitignore(); err != nil {
		return nil, err
	}
	return &w, nil
}

// walk recursively descends path, calling walkFn.
func (w *gitignoreWalker) walk(path string, pathSplit []string, d fs.DirEntry, walkFn Func) error {
	if err := walkFn(path, d, nil); err != nil || !d.IsDir() {
		if err == SkipDir && d.IsDir() {
			// Successfully skipped directory.
			err = nil
		}
		return err
	}

	dirs, err := os.ReadDir(path)
	if err != nil {
		// Second call, to report ReadDir error.
		if err := walkFn(path, d, err); err != nil {
			return err
		}
	}

	l := len(w.ps)
	err = w.readGitignore(path, pathSplit)
	if err != nil {
		// Third call, to report readGitignore error.
		if err := walkFn(path, d, err); err != nil {
			return err
		}
	}

	for _, d1 := range dirs {
		name := d1.Name()
		path1 := filepath.Join(path, name)
		pathSplit1 := append(pathSplit, name)
		if w.m.Match(pathSplit1, d1.IsDir()) {
			// TODO log only on -logskip
			log.Printf("skipped %s: excluded by gitignore\n", path1)
			continue
		}
		if err := w.walk(path1, pathSplit1, d1, walkFn); err != nil {
			if err == SkipDir {
				break
			}
			return err
		}
	}

	// Pop the gitignore patterns when backing out of this dir. go-git
	// already checks whether a file is within scope of a gitignore, but
	// this saves extra checks when many gitignores have been read.
	w.ps = w.ps[:l]
	return nil
}

// Walk walks the file tree rooted at root, calling fn for each file or
// directory in the tree, including root.
//
// All errors that arise visiting files and directories are filtered by fn:
// see the walk.Func documentation for details.
//
// The files are walked in lexical order, which makes the output deterministic
// but requires Walk to read an entire directory into memory before proceeding
// to walk that directory.
//
// Walk does not follow symbolic links found in directories,
// but if root itself is a symbolic link, its target will be walked.
func (w *gitignoreWalker) Walk(root string, fn Func) error {
	info, err := os.Lstat(root)
	if err != nil {
		err = fn(root, nil, err)
	} else {
		err = w.walk(root, split(root), &statDirEntry{info}, fn)
	}
	if err == SkipDir {
		return nil
	}
	return err
}

type statDirEntry struct {
	info fs.FileInfo
}

func (d *statDirEntry) Name() string               { return d.info.Name() }
func (d *statDirEntry) IsDir() bool                { return d.info.IsDir() }
func (d *statDirEntry) Type() fs.FileMode          { return d.info.Mode().Type() }
func (d *statDirEntry) Info() (fs.FileInfo, error) { return d.info, nil }

// split splits a path into names separated by os.PathSeparator.
func split(path string) []string {
	sep := string(os.PathSeparator)
	if path == sep {
		return []string{}
	}
	return strings.Split(strings.TrimPrefix(path, sep), sep)
}

// loadGlobalGitignore reads the gitignore files specified in
// /etc/gitconfig and ~/.gitconfig, if they exist.
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

// readGitignore reads the gitignore file in the given directory, if it
// exists.
func (w *gitignoreWalker) readGitignore(path string, pathSplit []string) error {
	f, err := os.Open(filepath.Join(path, ".gitignore"))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
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
