// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

// Merging indexes.
//
// To merge two indexes A and B (newer) into a combined index C:
//
// Load the path list from B and determine for each path the docID ranges
// that it will replace in A.
//
// Read A's and B's name lists together, merging them into C's name list.
// Discard the identified ranges from A during the merge. Also during the merge,
// record the mapping from A's docids to C's docids, and also the mapping from
// B's docids to C's docids. Both mappings can be summarized in a table like
//
//	10-14 map to 20-24
//	15-24 is deleted
//	25-34 maps to 40-49
//
// The number of ranges will be at most the combined number of paths.
// Also during the merge, write the name index to a temporary file as usual.
//
// Now merge the posting lists (this is why they begin with the trigram).
// During the merge, translate the docID numbers to the new C docID space.
// Also during the merge, write the posting list index to a temporary file as usual.
//
// Copy the name index and posting list index into C's index and write the trailer.
// Rename C's index onto the new index.

import (
	"encoding/binary"
	"os"
	"strings"
)

// An idRange records that the half-open interval [lo, hi) maps to [new, new+hi-lo).
type idRange struct {
	lo, hi, new uint32
}

type postIndex struct {
	tri    uint32
	count  uint32
	offset uint32
}

// Merge creates a new index in the file dst that corresponds to merging
// the two indices src1 and src2. If both src1 and src2 claim responsibility
// for a path, src2 is assumed to be newer and is given preference.
func Merge(dst, src1, src2 string) error {
	ix1, err := Open(src1)
	if err != nil {
		return err
	}
	ix2, err := Open(src2)
	if err != nil {
		return err
	}
	paths1, err := ix1.Paths()
	if err != nil {
		return err
	}
	paths2, err := ix2.Paths()
	if err != nil {
		return err
	}

	// Build docID maps.
	var i1, i2, new uint32
	var map1, map2 []idRange
	for _, path := range paths2 {
		// Determine range shadowed by this path.
		old := i1
		for i1 < uint32(ix1.numName) {
			name, err := ix1.Name(i1)
			if err != nil {
				return err
			}
			if name >= path {
				break
			}
			i1++
		}
		lo := i1
		limit := path[:len(path)-1] + string(path[len(path)-1]+1)
		for i1 < uint32(ix1.numName) {
			name, err := ix1.Name(i1)
			if err != nil {
				return err
			}
			if name >= limit {
				break
			}
			i1++
		}
		hi := i1

		// Record range before the shadow.
		if old < lo {
			map1 = append(map1, idRange{old, lo, new})
			new += lo - old
		}

		// Determine range defined by this path.
		// Because we are iterating over the ix2 paths,
		// there can't be gaps, so it must start at i2.
		if i2 < uint32(ix2.numName) {
			name, err := ix2.Name(i2)
			if err != nil {
				return err
			}
			if name < path {
				panic("merge: inconsistent index")
			}
		}
		lo = i2
		for i2 < uint32(ix2.numName) {
			name, err := ix2.Name(i2)
			if err != nil {
				return err
			}
			if name >= limit {
				break
			}
			i2++
		}
		hi = i2
		if lo < hi {
			map2 = append(map2, idRange{lo, hi, new})
			new += hi - lo
		}
	}

	if i1 < uint32(ix1.numName) {
		map1 = append(map1, idRange{i1, uint32(ix1.numName), new})
		new += uint32(ix1.numName) - i1
	}
	if i2 < uint32(ix2.numName) {
		panic("merge: inconsistent index")
	}
	numName := new

	ix3, err := bufCreate(dst)
	if err != nil {
		return err
	}
	if err := ix3.writeString(magic); err != nil {
		return err
	}

	// Merged list of paths.
	pathData := ix3.offset()
	mi1 := 0
	mi2 := 0
	last := "\x00" // not a prefix of anything
	for mi1 < len(paths1) || mi2 < len(paths2) {
		var p string
		if mi2 >= len(paths2) || mi1 < len(paths1) && paths1[mi1] <= paths2[mi2] {
			p = paths1[mi1]
			mi1++
		} else {
			p = paths2[mi2]
			mi2++
		}
		if strings.HasPrefix(p, last) {
			continue
		}
		last = p
		if err := ix3.writeString(p); err != nil {
			return err
		}
		if err := ix3.writeByte('\x00'); err != nil {
			return err
		}
	}
	if err := ix3.writeByte('\x00'); err != nil {
		return err
	}

	// Merged list of names.
	nameData := ix3.offset()
	nameIndexFile, err := bufCreate("")
	if err != nil {
		return err
	}
	new = 0
	mi1 = 0
	mi2 = 0
	for new < numName {
		if mi1 < len(map1) && map1[mi1].new == new {
			for i := map1[mi1].lo; i < map1[mi1].hi; i++ {
				name, err := ix1.Name(i)
				if err != nil {
					return err
				}
				if err := nameIndexFile.writeUint32(ix3.offset() - nameData); err != nil {
					return err
				}
				if err := ix3.writeString(name); err != nil {
					return err
				}
				if err := ix3.writeByte('\x00'); err != nil {
					return err
				}
				new++
			}
			mi1++
		} else if mi2 < len(map2) && map2[mi2].new == new {
			for i := map2[mi2].lo; i < map2[mi2].hi; i++ {
				name, err := ix2.Name(i)
				if err != nil {
					return err
				}
				if err := nameIndexFile.writeUint32(ix3.offset() - nameData); err != nil {
					return err
				}
				if err := ix3.writeString(name); err != nil {
					return err
				}
				if err := ix3.writeByte('\x00'); err != nil {
					return err
				}
				new++
			}
			mi2++
		} else {
			panic("merge: inconsistent index")
		}
	}
	if new*4 != nameIndexFile.offset() {
		panic("merge: inconsistent index")
	}
	if err := nameIndexFile.writeUint32(ix3.offset()); err != nil {
		return err
	}

	// Merged list of posting lists.
	postData := ix3.offset()
	var r1 postMapReader
	var r2 postMapReader
	var w postDataWriter
	if err := r1.init(ix1, map1); err != nil {
		return err
	}
	if err := r2.init(ix2, map2); err != nil {
		return err
	}
	if err := w.init(ix3); err != nil {
		return err
	}
	for {
		if r1.trigram < r2.trigram {
			w.trigram(r1.trigram)
			for {
				ok, err := r1.nextID()
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				if err := w.fileID(r1.fileID); err != nil {
					return err
				}
			}
			r1.nextTrigram()
			w.endTrigram()
		} else if r2.trigram < r1.trigram {
			w.trigram(r2.trigram)
			for {
				ok, err := r2.nextID()
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				if err := w.fileID(r2.fileID); err != nil {
					return err
				}
			}
			r2.nextTrigram()
			w.endTrigram()
		} else {
			if r1.trigram == ^uint32(0) {
				break
			}
			w.trigram(r1.trigram)
			r1.nextID()
			r2.nextID()
			for r1.fileID < ^uint32(0) || r2.fileID < ^uint32(0) {
				if r1.fileID < r2.fileID {
					if err := w.fileID(r1.fileID); err != nil {
						return err
					}
					r1.nextID()
				} else if r2.fileID < r1.fileID {
					if err := w.fileID(r2.fileID); err != nil {
						return err
					}
					r2.nextID()
				} else {
					panic("merge: inconsistent index")
				}
			}
			if err := r1.nextTrigram(); err != nil {
				return err
			}
			if err := r2.nextTrigram(); err != nil {
				return err
			}
			if err := w.endTrigram(); err != nil {
				return err
			}
		}
	}

	// Name index
	nameIndex := ix3.offset()
	copyFile(ix3, nameIndexFile)

	// Posting list index
	postIndex := ix3.offset()
	copyFile(ix3, w.postIndexFile)

	if err := ix3.writeUint32(pathData); err != nil {
		return err
	}
	if err := ix3.writeUint32(nameData); err != nil {
		return err
	}
	if err := ix3.writeUint32(postData); err != nil {
		return err
	}
	if err := ix3.writeUint32(nameIndex); err != nil {
		return err
	}
	if err := ix3.writeUint32(postIndex); err != nil {
		return err
	}
	if err := ix3.writeString(trailerMagic); err != nil {
		return err
	}
	if err := ix3.flush(); err != nil {
		return err
	}

	os.Remove(nameIndexFile.name)
	os.Remove(w.postIndexFile.name)
	return nil
}

type postMapReader struct {
	ix      *Index
	idMap   []idRange
	triNum  uint32
	trigram uint32
	count   uint32
	offset  uint32
	d       []byte
	oldID   uint32
	fileID  uint32
	i       int
}

func (r *postMapReader) init(ix *Index, idMap []idRange) error {
	r.ix = ix
	r.idMap = idMap
	r.trigram = ^uint32(0)
	return r.load()
}

func (r *postMapReader) nextTrigram() error {
	r.triNum++
	return r.load()
}

func (r *postMapReader) load() error {
	if r.triNum >= uint32(r.ix.numPost) {
		r.trigram = ^uint32(0)
		r.count = 0
		r.fileID = ^uint32(0)
		return nil
	}
	var err error
	r.trigram, r.count, r.offset, err = r.ix.listAt(r.triNum * postEntrySize)
	if err != nil {
		return err
	}
	if r.count == 0 {
		r.fileID = ^uint32(0)
		return nil
	}
	r.d, err = r.ix.slice(r.ix.postData+r.offset+3, -1)
	r.oldID = ^uint32(0)
	r.i = 0
	return err
}

func (r *postMapReader) nextID() (bool, error) {
	for r.count > 0 {
		r.count--
		delta64, n := binary.Uvarint(r.d)
		delta := uint32(delta64)
		if n <= 0 || delta == 0 {
			return false, corrupt()
		}
		r.d = r.d[n:]
		r.oldID += delta
		for r.i < len(r.idMap) && r.idMap[r.i].hi <= r.oldID {
			r.i++
		}
		if r.i >= len(r.idMap) {
			r.count = 0
			break
		}
		if r.oldID < r.idMap[r.i].lo {
			continue
		}
		r.fileID = r.idMap[r.i].new + r.oldID - r.idMap[r.i].lo
		return true, nil
	}

	r.fileID = ^uint32(0)
	return false, nil
}

type postDataWriter struct {
	out           *bufWriter
	postIndexFile *bufWriter
	buf           [10]byte
	base          uint32
	count, offset uint32
	last          uint32
	t             uint32
}

func (w *postDataWriter) init(out *bufWriter) error {
	b, err := bufCreate("")
	if err != nil {
		return err
	}
	w.out = out
	w.postIndexFile = b
	w.base = out.offset()
	return nil
}

func (w *postDataWriter) trigram(t uint32) {
	w.offset = w.out.offset()
	w.count = 0
	w.t = t
	w.last = ^uint32(0)
}

func (w *postDataWriter) fileID(id uint32) error {
	if w.count == 0 {
		if err := w.out.writeTrigram(w.t); err != nil {
			return err
		}
	}
	if err := w.out.writeUvarint(id - w.last); err != nil {
		return err
	}
	w.last = id
	w.count++
	return nil
}

func (w *postDataWriter) endTrigram() error {
	if w.count == 0 {
		return nil
	}
	if err := w.out.writeUvarint(0); err != nil {
		return err
	}
	if err := w.postIndexFile.writeTrigram(w.t); err != nil {
		return err
	}
	if err := w.postIndexFile.writeUint32(w.count); err != nil {
		return err
	}
	return w.postIndexFile.writeUint32(w.offset - w.base)
}
