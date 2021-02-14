// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"io/ioutil"
	"os"
	"testing"
)

var postFiles = map[string]string{
	"file0": "",
	"file1": "Google Code Search",
	"file2": "Google Code Project Hosting",
	"file3": "Google Web Search",
}

func tri(x, y, z byte) uint32 {
	return uint32(x)<<16 | uint32(y)<<8 | uint32(z)
}

func TestTrivialPosting(t *testing.T) {
	f, _ := ioutil.TempFile("", "index-test")
	defer os.Remove(f.Name())
	out := f.Name()
	buildIndex(t, out, nil, postFiles)
	ix, err := Open(out)
	if err != nil {
		t.Fatal(err)
	}

	checkPosting := func(label string, want []uint32) func([]uint32, error) {
		return func(got []uint32, err error) {
			if err != nil {
				t.Errorf("PostingList(%s): %v", label, err)
			} else if !equalList(got, want) {
				t.Errorf("PostingList(%s) = %v, want %v", label, got, want)
			}
		}
	}

	checkPosting("Sea", []uint32{1, 3})(ix.PostingList(tri('S', 'e', 'a')))
	checkPosting("Goo", []uint32{1, 2, 3})(ix.PostingList(tri('G', 'o', 'o')))
	checkPosting("Sea&Goo", []uint32{1, 3})(ix.PostingAnd([]uint32{1, 3}, tri('G', 'o', 'o')))
	checkPosting("Goo&Sea", []uint32{1, 3})(ix.PostingAnd([]uint32{1, 2, 3}, tri('S', 'e', 'a')))
	checkPosting("Sea|Goo", []uint32{1, 2, 3})(ix.PostingOr([]uint32{1, 3}, tri('G', 'o', 'o')))
	checkPosting("Goo|Sea", []uint32{1, 2, 3})(ix.PostingOr([]uint32{1, 2, 3}, tri('S', 'e', 'a')))
}

func equalList(x, y []uint32) bool {
	if len(x) != len(y) {
		return false
	}
	for i, xi := range x {
		if xi != y[i] {
			return false
		}
	}
	return true
}
