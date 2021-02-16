// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package walk

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

// Copied from Go's io/fs:walk.go.

// SkipDir is used as a return value from a walk.Func to indicate that
// the directory named in the call is to be skipped. It is not returned
// as an error by any function.
var SkipDir = fs.SkipDir

// Func is the type of the function called by Walk to visit each file
// or directory.
//
// The path argument contains the argument to Walk as a prefix.
// That is, if Walk is called with root argument "dir" and finds a file
// named "a" in that directory, the walk function will be called with
// argument "dir/a".
//
// The d argument is the fs.DirEntry for the named path.
//
// The error result returned by the function controls how Walk
// continues. If the function returns the special value SkipDir, Walk
// skips the current directory (path if d.IsDir() is true, otherwise
// path's parent directory). Otherwise, if the function returns a
// non-nil error, Walk stops entirely and returns that error.
//
// The err argument reports an error related to path, signaling that
// Walk will not walk into that directory. The function can decide how
// to handle that error; as described earlier, returning the error will
// cause Walk to stop walking the entire tree.
//
// Walk calls the function with a non-nil err argument in two cases.
//
// First, if the initial fs.Stat on the root directory fails, Walk
// calls the function with path set to root, d set to nil, and err set
// to the error from fs.Stat.
//
// Second, if a directory's ReadDir method fails, Walk calls the
// function with path set to the directory's path, d set to an
// fs.DirEntry describing the directory, and err set to the error from
// ReadDir. In this second case, the function is called twice with the
// path of the directory: the first call is before the directory read is
// attempted and has err set to nil, giving the function a chance to
// return SkipDir and avoid the ReadDir entirely. The second call is
// after a failed ReadDir and reports the error from ReadDir.
// (If ReadDir succeeds, there is no second call.)
//
// The differences between Func compared to filepath.Func are:
//
//   - The second argument has type fs.DirEntry instead of fs.FileInfo.
//   - The function is called before reading a directory, to allow
//     SkipDir to bypass the directory read entirely.
//   - If a directory read fails, the function is called a second time
//     for that directory to report the error.
//
type Func func(path string, d fs.DirEntry, err error) error

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
		// Third call, to report push error.
		if err := walkFn(path, d, err); err != nil {
			return err
		}
	}

	for _, d1 := range dirs {
		name := d1.Name()
		pathSplit1 := append(pathSplit, name)
		if w.m.Match(pathSplit1, d1.IsDir()) {
			fmt.Println("skipped", name)
			continue
		}
		path1 := filepath.Join(path, name)
		if err := w.walk(path1, pathSplit1, d1, walkFn); err != nil {
			if err == SkipDir {
				break
			}
			return err
		}
	}

	// Pop the gitignore from this dir.
	w.ps = w.ps[:l]
	return nil
}

type gitignoreWalker struct {
	ps []gitignore.Pattern
	m  gitignore.Matcher
}

type Walker interface {
	Walk(root string, fn Func) error
}

func NewGitignoreWalker() (Walker, error) {
	var w gitignoreWalker
	if err := w.loadGlobalGitignore(); err != nil {
		return nil, err
	}
	return &w, nil
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

func split(path string) []string {
	sep := string(os.PathSeparator)
	if path == sep {
		return []string{}
	}
	return strings.Split(strings.TrimPrefix(path, sep), sep)
}
