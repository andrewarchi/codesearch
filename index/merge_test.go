// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"io/ioutil"
	"os"
	"testing"
)

var mergePaths1 = []string{
	"/a",
	"/b",
	"/c",
}

var mergePaths2 = []string{
	"/b",
	"/cc",
}

var mergeFiles1 = map[string]string{
	"/a/x":  "hello world",
	"/a/y":  "goodbye world",
	"/b/xx": "now is the time",
	"/b/xy": "for all good men",
	"/c/ab": "give me all the potatoes",
	"/c/de": "or give me death now",
}

var mergeFiles2 = map[string]string{
	"/b/www": "world wide indeed",
	"/b/xx":  "no, not now",
	"/b/yy":  "first potatoes, now liberty?",
	"/cc":    "come to the aid of his potatoes",
}

func TestMerge(t *testing.T) {
	tempFile := func() string {
		f, err := ioutil.TempFile("", "index-test")
		if err != nil {
			t.Fatal(err)
		}
		return f.Name()
	}

	out1 := tempFile()
	out2 := tempFile()
	out3 := tempFile()
	defer os.Remove(out1)
	defer os.Remove(out2)
	defer os.Remove(out3)

	buildIndex(t, out1, mergePaths1, mergeFiles1)
	buildIndex(t, out2, mergePaths2, mergeFiles2)

	if err := Merge(out3, out1, out2); err != nil {
		t.Fatal(err)
	}

	ix1, err := Open(out1)
	if err != nil {
		t.Fatal(err)
	}
	ix2, err := Open(out2)
	if err != nil {
		t.Fatal(err)
	}
	ix3, err := Open(out3)
	if err != nil {
		t.Fatal(err)
	}

	nameof := func(ix *Index) string {
		switch {
		case ix == ix1:
			return "ix1"
		case ix == ix2:
			return "ix2"
		case ix == ix3:
			return "ix3"
		}
		return "???"
	}

	checkFiles := func(ix *Index, l ...string) {
		for i, s := range l {
			n, err := ix.Name(uint32(i))
			if err != nil {
				t.Errorf("%s: Name(%d): %v", nameof(ix), i, err)
			} else if n != s {
				t.Errorf("%s: Name(%d) = %s, want %s", nameof(ix), i, n, s)
			}
		}
	}

	checkFiles(ix1, "/a/x", "/a/y", "/b/xx", "/b/xy", "/c/ab", "/c/de")
	checkFiles(ix2, "/b/www", "/b/xx", "/b/yy", "/cc")
	checkFiles(ix3, "/a/x", "/a/y", "/b/www", "/b/xx", "/b/yy", "/c/ab", "/c/de", "/cc")

	check := func(ix *Index, trig string, l ...uint32) {
		l1, err := ix.PostingList(tri(trig[0], trig[1], trig[2]))
		if err != nil {
			t.Error(err)
		} else if !equalList(l1, l) {
			t.Errorf("PostingList(%s, %s) = %v, want %v", nameof(ix), trig, l1, l)
		}
	}

	check(ix1, "wor", 0, 1)
	check(ix1, "now", 2, 5)
	check(ix1, "all", 3, 4)

	check(ix2, "now", 1, 2)

	check(ix3, "all", 5)
	check(ix3, "wor", 0, 1, 2)
	check(ix3, "now", 3, 4, 6)
	check(ix3, "pot", 4, 5, 7)
}
