// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

// Index format.
//
// An index stored on disk has the format:
//
//	"csearch index 1\n"
//	list of paths
//	list of names
//	list of posting lists
//	name index
//	posting list index
//	trailer
//
// The list of paths is a sorted sequence of NUL-terminated file or directory names.
// The index covers the file trees rooted at those paths.
// The list ends with an empty name ("\x00").
//
// The list of names is a sorted sequence of NUL-terminated file names.
// The initial entry in the list corresponds to file #0,
// the next to file #1, and so on. The list ends with an
// empty name ("\x00").
//
// The list of posting lists are a sequence of posting lists.
// Each posting list has the form:
//
//	trigram [3]
//	deltas [v]...
//
// The trigram gives the 3 byte trigram that this list describes. The
// delta list is a sequence of varint-encoded deltas between file
// IDs, ending with a zero delta. For example, the delta list [2,5,1,1,0]
// encodes the file ID list 1, 6, 7, 8. The delta list [0] would
// encode the empty file ID list, but empty posting lists are usually
// not recorded at all. The list of posting lists ends with an entry
// with trigram "\xff\xff\xff" and a delta list consisting a single zero.
//
// The indexes enable efficient random access to the lists. The name
// index is a sequence of 4-byte big-endian values listing the byte
// offset in the name list where each name begins. The posting list
// index is a sequence of index entries describing each successive
// posting list. Each index entry has the form:
//
//	trigram [3]
//	file count [4]
//	offset [4]
//
// Index entries are only written for the non-empty posting lists,
// so finding the posting list for a specific trigram requires a
// binary search over the posting list index. In practice, the majority
// of the possible trigrams are never seen, so omitting the missing
// ones represents a significant storage savings.
//
// The trailer has the form:
//
//	offset of path list [4]
//	offset of name list [4]
//	offset of posting lists [4]
//	offset of name index [4]
//	offset of posting list index [4]
//	"\ncsearch trailr\n"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

const (
	magic        = "csearch index 1\n"
	trailerMagic = "\ncsearch trailr\n"
)

// An Index implements read-only access to a trigram index.
type Index struct {
	Verbose   bool
	data      mmapData
	pathData  uint32
	nameData  uint32
	postData  uint32
	nameIndex uint32
	postIndex uint32
	numName   int
	numPost   int
}

const postEntrySize = 3 + 4 + 4

func Open(file string) (*Index, error) {
	mm, err := mmap(file)
	if err != nil {
		return nil, err
	}
	if len(mm.d) < 4*4+len(trailerMagic) || string(mm.d[len(mm.d)-len(trailerMagic):]) != trailerMagic {
		return nil, corrupt()
	}
	n := uint32(len(mm.d) - len(trailerMagic) - 5*4)
	ix := &Index{data: *mm}
	if ix.pathData, err = ix.uint32(n); err != nil {
		return nil, err
	}
	if ix.nameData, err = ix.uint32(n + 4); err != nil {
		return nil, err
	}
	if ix.postData, err = ix.uint32(n + 8); err != nil {
		return nil, err
	}
	if ix.nameIndex, err = ix.uint32(n + 12); err != nil {
		return nil, err
	}
	if ix.postIndex, err = ix.uint32(n + 16); err != nil {
		return nil, err
	}
	ix.numName = int((ix.postIndex-ix.nameIndex)/4) - 1
	ix.numPost = int((n - ix.postIndex) / postEntrySize)
	return ix, nil
}

// slice returns the slice of index data starting at the given byte offset.
// If n >= 0, the slice must have length at least n and is truncated to length n.
func (ix *Index) slice(off uint32, n int) ([]byte, error) {
	o := int(off)
	if uint32(o) != off || n >= 0 && o+n > len(ix.data.d) {
		return nil, corrupt()
	}
	if n < 0 {
		return ix.data.d[o:], nil
	}
	return ix.data.d[o : o+n], nil
}

// uint32 returns the uint32 value at the given offset in the index data.
func (ix *Index) uint32(off uint32) (uint32, error) {
	d, err := ix.slice(off, 4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(d), nil
}

// uvarint returns the varint value at the given offset in the index data.
func (ix *Index) uvarint(off uint32) (uint32, error) {
	d, err := ix.slice(off, -1)
	if err != nil {
		return 0, err
	}
	v, n := binary.Uvarint(d)
	if n <= 0 {
		return 0, corrupt()
	}
	return uint32(v), nil
}

// Paths returns the list of indexed paths.
func (ix *Index) Paths() ([]string, error) {
	off := ix.pathData
	var x []string
	for {
		s, err := ix.str(off)
		if err != nil {
			return nil, err
		}
		if len(s) == 0 {
			break
		}
		x = append(x, string(s))
		off += uint32(len(s) + 1)
	}
	return x, nil
}

// NameBytes returns the name corresponding to the given file ID.
func (ix *Index) NameBytes(fileID uint32) ([]byte, error) {
	if fileID > uint32(ix.numName) {
		return nil, fmt.Errorf("file ID %d out of range", fileID)
	}
	off, err := ix.uint32(ix.nameIndex + 4*fileID)
	if err != nil {
		return nil, err
	}
	return ix.str(ix.nameData + off)
}

func (ix *Index) str(off uint32) ([]byte, error) {
	str, err := ix.slice(off, -1)
	if err != nil {
		return nil, err
	}
	i := bytes.IndexByte(str, '\x00')
	if i < 0 {
		return nil, corrupt()
	}
	return str[:i], nil
}

// Name returns the name corresponding to the given file ID.
func (ix *Index) Name(fileID uint32) (string, error) {
	name, err := ix.NameBytes(fileID)
	if err != nil {
		return "", err
	}
	return string(name), nil
}

// Names returns all file names in the index.
func (ix *Index) Names() ([]string, error) {
	names := make([]string, 0, ix.numName)
	for i := 0; i < ix.numName; i++ {
		name, err := ix.Name(uint32(i))
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

// NumNames returns the number of file names in the index.
func (ix *Index) NumNames() int {
	return ix.numName
}

// listAt returns the index list entry at the given offset.
func (ix *Index) listAt(off uint32) (trigram, count, offset uint32, err error) {
	d, err := ix.slice(ix.postIndex+off, postEntrySize)
	if err != nil {
		return 0, 0, 0, err
	}
	trigram = uint32(d[0])<<16 | uint32(d[1])<<8 | uint32(d[2])
	count = binary.BigEndian.Uint32(d[3:])
	offset = binary.BigEndian.Uint32(d[3+4:])
	return
}

func (ix *Index) dumpPosting() error {
	d, err := ix.slice(ix.postIndex, postEntrySize*ix.numPost)
	if err != nil {
		return err
	}
	for i := 0; i < ix.numPost; i++ {
		j := i * postEntrySize
		t := uint32(d[j])<<16 | uint32(d[j+1])<<8 | uint32(d[j+2])
		count := int(binary.BigEndian.Uint32(d[j+3:]))
		offset := binary.BigEndian.Uint32(d[j+3+4:])
		log.Printf("%#x: %d at %d", t, count, offset)
	}
	return nil
}

func (ix *Index) findList(trigram uint32) (count int, offset uint32, err error) {
	// binary search
	d, err := ix.slice(ix.postIndex, postEntrySize*ix.numPost)
	if err != nil {
		return 0, 0, err
	}
	i := sort.Search(ix.numPost, func(i int) bool {
		i *= postEntrySize
		t := uint32(d[i])<<16 | uint32(d[i+1])<<8 | uint32(d[i+2])
		return t >= trigram
	})
	if i >= ix.numPost {
		return 0, 0, nil
	}
	i *= postEntrySize
	t := uint32(d[i])<<16 | uint32(d[i+1])<<8 | uint32(d[i+2])
	if t != trigram {
		return 0, 0, nil
	}
	count = int(binary.BigEndian.Uint32(d[i+3:]))
	offset = binary.BigEndian.Uint32(d[i+3+4:])
	return
}

type postReader struct {
	ix       *Index
	count    int
	offset   uint32
	fileID   uint32
	d        []byte
	restrict []uint32
}

func (r *postReader) init(ix *Index, trigram uint32, restrict []uint32) error {
	count, offset, err := ix.findList(trigram)
	if count == 0 || err != nil {
		return err
	}
	d, err := ix.slice(ix.postData+offset+3, -1)
	if err != nil {
		return err
	}
	r.ix = ix
	r.count = count
	r.offset = offset
	r.fileID = ^uint32(0)
	r.d = d
	r.restrict = restrict
	return nil
}

func (r *postReader) max() int {
	return int(r.count)
}

func (r *postReader) next() (bool, error) {
	for r.count > 0 {
		r.count--
		delta64, n := binary.Uvarint(r.d)
		delta := uint32(delta64)
		if n <= 0 || delta == 0 {
			return false, corrupt()
		}
		r.d = r.d[n:]
		r.fileID += delta
		if r.restrict != nil {
			i := 0
			for i < len(r.restrict) && r.restrict[i] < r.fileID {
				i++
			}
			r.restrict = r.restrict[i:]
			if len(r.restrict) == 0 || r.restrict[0] != r.fileID {
				continue
			}
		}
		return true, nil
	}
	// list should end with terminating 0 delta
	if r.d != nil && (len(r.d) == 0 || r.d[0] != 0) {
		return false, corrupt()
	}
	r.fileID = ^uint32(0)
	return false, nil
}

func (ix *Index) PostingList(trigram uint32) ([]uint32, error) {
	return ix.postingList(trigram, nil)
}

func (ix *Index) postingList(trigram uint32, restrict []uint32) ([]uint32, error) {
	var r postReader
	if err := r.init(ix, trigram, restrict); err != nil {
		return nil, err
	}
	x := make([]uint32, 0, r.max())
	for {
		ok, err := r.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		x = append(x, r.fileID)
	}
	return x, nil
}

func (ix *Index) PostingAnd(list []uint32, trigram uint32) ([]uint32, error) {
	return ix.postingAnd(list, trigram, nil)
}

func (ix *Index) postingAnd(list []uint32, trigram uint32, restrict []uint32) ([]uint32, error) {
	var r postReader
	r.init(ix, trigram, restrict)
	x := list[:0]
	i := 0
	for {
		ok, err := r.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		fileID := r.fileID
		for i < len(list) && list[i] < fileID {
			i++
		}
		if i < len(list) && list[i] == fileID {
			x = append(x, fileID)
			i++
		}
	}
	return x, nil
}

func (ix *Index) PostingOr(list []uint32, trigram uint32) ([]uint32, error) {
	return ix.postingOr(list, trigram, nil)
}

func (ix *Index) postingOr(list []uint32, trigram uint32, restrict []uint32) ([]uint32, error) {
	var r postReader
	r.init(ix, trigram, restrict)
	x := make([]uint32, 0, len(list)+r.max())
	i := 0
	for {
		ok, err := r.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		fileID := r.fileID
		for i < len(list) && list[i] < fileID {
			x = append(x, list[i])
			i++
		}
		x = append(x, fileID)
		if i < len(list) && list[i] == fileID {
			i++
		}
	}
	x = append(x, list[i:]...)
	return x, nil
}

func (ix *Index) PostingQuery(q *Query) ([]uint32, error) {
	return ix.postingQuery(q, nil)
}

func (ix *Index) postingQuery(q *Query, restrict []uint32) ([]uint32, error) {
	var list []uint32
	var err error
	switch q.Op {
	case QNone:
		// nothing
	case QAll:
		if restrict != nil {
			return restrict, nil
		}
		list = make([]uint32, ix.numName)
		for i := range list {
			list[i] = uint32(i)
		}
		return list, nil
	case QAnd:
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])
			if list == nil {
				list, err = ix.postingList(tri, restrict)
			} else {
				list, err = ix.postingAnd(list, tri, restrict)
			}
			if len(list) == 0 || err != nil {
				return nil, err
			}
		}
		for _, sub := range q.Sub {
			if list == nil {
				list = restrict
			}
			list, err = ix.postingQuery(sub, list)
			if len(list) == 0 || err != nil {
				return nil, err
			}
		}
	case QOr:
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])
			if list == nil {
				list, err = ix.postingList(tri, restrict)
			} else {
				list, err = ix.postingOr(list, tri, restrict)
			}
			if err != nil {
				return nil, err
			}
		}
		for _, sub := range q.Sub {
			list1, err := ix.postingQuery(sub, restrict)
			if err != nil {
				return nil, err
			}
			list = mergeOr(list, list1)
		}
	}
	return list, nil
}

func mergeOr(l1, l2 []uint32) []uint32 {
	var l []uint32
	i := 0
	j := 0
	for i < len(l1) || j < len(l2) {
		switch {
		case j == len(l2) || (i < len(l1) && l1[i] < l2[j]):
			l = append(l, l1[i])
			i++
		case i == len(l1) || (j < len(l2) && l1[i] > l2[j]):
			l = append(l, l2[j])
			j++
		case l1[i] == l2[j]:
			l = append(l, l1[i])
			i++
			j++
		}
	}
	return l
}

func corrupt() error {
	return fmt.Errorf("corrupt index: remove %s", File())
}

// An mmapData is mmap'ed read-only data from a file.
type mmapData struct {
	f *os.File
	d []byte
}

// mmap maps the given file into memory.
func mmap(file string) (*mmapData, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return mmapFile(f)
}

// File returns the name of the index file to use.
// It is at $CSEARCHINDEX, the current working directory or a parent
// directory, or $HOME/.csearchindex.
func File() string {
	if f := os.Getenv("CSEARCHINDEX"); f != "" {
		return f
	}

	cwd, err := os.Getwd()
	if err == nil {
		for {
			f := filepath.Join(cwd, ".csearchindex")
			if _, err := os.Lstat(f); err == nil {
				return f
			}
			parent := filepath.Dir(cwd)
			if parent == cwd {
				break
			}
			cwd = parent
		}
	}

	var home string
	home = os.Getenv("HOME")
	if runtime.GOOS == "windows" && home == "" {
		home = os.Getenv("USERPROFILE")
	}
	return filepath.Clean(home + "/.csearchindex")
}
