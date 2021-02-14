// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"unsafe"

	"github.com/andrewarchi/codesearch/sparse"
)

// Index writing. See read.go for details of on-disk format.
//
// It would suffice to make a single large list of (trigram, file#) pairs
// while processing the files one at a time, sort that list by trigram,
// and then create the posting lists from subsequences of the list.
// However, we do not assume that the entire index fits in memory.
// Instead, we sort and flush the list to a new temporary file each time
// it reaches its maximum in-memory size, and then at the end we
// create the final posting lists by merging the temporary files as we
// read them back in.
//
// It would also be useful to be able to create an index for a subset
// of the files and then merge that index into an existing one. This would
// allow incremental updating of an existing index when a directory changes.
// But we have not implemented that.

// A Writer creates an on-disk index corresponding to a set of files.
type Writer struct {
	LogSkip bool // log information about skipped files
	Verbose bool // log status using package log

	trigram *sparse.Set // trigrams for the current file
	buf     [8]byte     // scratch buffer

	paths []string

	nameData   *bufWriter // temp file holding list of names
	nameLen    uint32     // number of bytes written to nameData
	nameIndex  *bufWriter // temp file holding name index
	numName    int        // number of names written
	totalBytes int64

	post      []postEntry // list of (trigram, file#) pairs
	postFile  []*os.File  // flushed post entries
	postIndex *bufWriter  // temp file holding posting list index

	inbuf []byte     // input buffer
	main  *bufWriter // main index file
}

const npost = 64 << 20 / 8 // 64 MB worth of post entries

// Create returns a new Writer that will write the index to file.
func Create(file string) (*Writer, error) {
	w := &Writer{
		trigram: sparse.NewSet(1 << 24),
		post:    make([]postEntry, 0, npost),
		inbuf:   make([]byte, 16384),
	}
	var err error
	if w.nameData, err = bufCreate(""); err != nil {
		return nil, err
	}
	if w.nameIndex, err = bufCreate(""); err != nil {
		return nil, err
	}
	if w.postIndex, err = bufCreate(""); err != nil {
		return nil, err
	}
	if w.main, err = bufCreate(file); err != nil {
		return nil, err
	}
	return w, nil
}

// A postEntry is an in-memory (trigram, file#) pair.
type postEntry uint64

func (p postEntry) trigram() uint32 {
	return uint32(p >> 32)
}

func (p postEntry) fileID() uint32 {
	return uint32(p)
}

func makePostEntry(trigram, fileID uint32) postEntry {
	return postEntry(trigram)<<32 | postEntry(fileID)
}

// Tuning constants for detecting text files.
// A file is assumed not to be a text file (and thus not indexed)
// if it contains an invalid UTF-8 sequence, if it is longer than maxFileLength
// bytes, if it contains a line longer than maxLineLen bytes,
// or if it contains more than maxTextTrigrams distinct trigrams.
const (
	maxFileLen      = 1 << 30
	maxLineLen      = 2000
	maxTextTrigrams = 20000
)

// AddPaths adds the given paths to the index's list of paths.
func (ix *Writer) AddPaths(paths []string) {
	ix.paths = append(ix.paths, paths...)
}

// AddFile adds the file with the given name (opened using os.Open)
// to the index. It logs errors using package log.
func (ix *Writer) AddFile(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return ix.Add(name, f)
}

// Add adds the file f to the index under the given name.
// It logs errors using package log.
func (ix *Writer) Add(name string, f io.Reader) error {
	ix.trigram.Reset()
	var (
		c       = byte(0)
		i       = 0
		buf     = ix.inbuf[:0]
		tv      = uint32(0)
		n       = int64(0)
		lineLen = 0
		lineNum = 1
	)
	for {
		tv = (tv << 8) & (1<<24 - 1)
		if i >= len(buf) {
			n, err := f.Read(buf[:cap(buf)])
			if n == 0 {
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("%s: %w", name, err)
				}
				return fmt.Errorf("%s: 0-length read", name)
			}
			buf = buf[:n]
			i = 0
		}
		c = buf[i]
		i++
		tv |= uint32(c)
		if n++; n >= 3 {
			ix.trigram.Add(tv)
		}
		if !validUTF8((tv>>8)&0xFF, tv&0xFF) {
			if ix.LogSkip {
				log.Printf("%s:%d: invalid UTF-8, ignoring\n", name, lineNum)
			}
			return nil
		}
		if n > maxFileLen {
			if ix.LogSkip {
				log.Printf("%s: file too long (%d bytes), ignoring\n", name, n)
			}
			return nil
		}
		if lineLen++; lineLen > maxLineLen {
			if ix.LogSkip {
				log.Printf("%s:%d: line too long (%d bytes), ignoring\n", name, lineNum, lineLen)
			}
			return nil
		}
		if c == '\n' {
			lineLen = 0
			lineNum++
		}
	}
	if ix.trigram.Len() > maxTextTrigrams {
		if ix.LogSkip {
			log.Printf("%s: too many trigrams (%d), probably not text, ignoring\n", name, ix.trigram.Len())
		}
		return nil
	}
	ix.totalBytes += n

	if ix.Verbose {
		log.Printf("%d %d %s\n", n, ix.trigram.Len(), name)
	}

	fileID, err := ix.addName(name)
	if err != nil {
		return err
	}
	for _, trigram := range ix.trigram.Dense() {
		if len(ix.post) >= cap(ix.post) {
			if err := ix.flushPost(); err != nil {
				return err
			}
		}
		ix.post = append(ix.post, makePostEntry(trigram, fileID))
	}
	return nil
}

// Flush flushes the index entry to the target file.
func (ix *Writer) Flush() error {
	if _, err := ix.addName(""); err != nil {
		return err
	}

	var off [5]uint32
	if err := ix.main.writeString(magic); err != nil {
		return err
	}
	off[0] = ix.main.offset()
	for _, p := range ix.paths {
		if err := ix.main.writeString(p); err != nil {
			return err
		}
		if err := ix.main.writeByte('\x00'); err != nil {
			return err
		}
	}
	if err := ix.main.writeByte('\x00'); err != nil {
		return err
	}
	off[1] = ix.main.offset()
	if err := copyFile(ix.main, ix.nameData); err != nil {
		return nil
	}
	off[2] = ix.main.offset()
	if err := ix.mergePost(ix.main); err != nil {
		return nil
	}
	off[3] = ix.main.offset()
	if err := copyFile(ix.main, ix.nameIndex); err != nil {
		return nil
	}
	off[4] = ix.main.offset()
	if err := copyFile(ix.main, ix.postIndex); err != nil {
		return nil
	}
	for _, v := range off {
		if err := ix.main.writeUint32(v); err != nil {
			return err
		}
	}
	if err := ix.main.writeString(trailerMagic); err != nil {
		return err
	}

	os.Remove(ix.nameData.name)
	for _, f := range ix.postFile {
		os.Remove(f.Name())
	}
	os.Remove(ix.nameIndex.name)
	os.Remove(ix.postIndex.name)

	log.Printf("%d data bytes, %d index bytes", ix.totalBytes, ix.main.offset())

	return ix.main.flush()
}

func copyFile(dst, src *bufWriter) error {
	if err := dst.flush(); err != nil {
		return err
	}
	f, err := src.finish()
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst.file, f); err != nil {
		return fmt.Errorf("copying %s to %s: %w", src.name, dst.name, err)
	}
	return nil
}

// addName adds the file with the given name to the index.
// It returns the assigned file ID number.
func (ix *Writer) addName(name string) (uint32, error) {
	if strings.Contains(name, "\x00") {
		return 0, fmt.Errorf("%q: file has NUL byte in name", name)
	}

	if err := ix.nameIndex.writeUint32(ix.nameData.offset()); err != nil {
		return 0, err
	}
	if err := ix.nameData.writeString(name); err != nil {
		return 0, err
	}
	if err := ix.nameData.writeByte('\x00'); err != nil {
		return 0, err
	}
	id := ix.numName
	ix.numName++
	return uint32(id), nil
}

// flushPost writes ix.post to a new temporary file and
// clears the slice.
func (ix *Writer) flushPost() error {
	w, err := ioutil.TempFile("", "csearch-index")
	if err != nil {
		return err
	}
	if ix.Verbose {
		log.Printf("flush %d entries to %s", len(ix.post), w.Name())
	}
	sortPost(ix.post)

	// Write the raw ix.post array to disk as is.
	// This process is the one reading it back in, so byte order is not a concern.
	data := (*[npost * 8]byte)(unsafe.Pointer(&ix.post[0]))[:len(ix.post)*8]
	if n, err := w.Write(data); err != nil || n < len(data) {
		if err != nil {
			return err
		}
		return fmt.Errorf("short write writing %s", w.Name())
	}

	ix.post = ix.post[:0]
	_, err = w.Seek(0, 0)
	ix.postFile = append(ix.postFile, w)
	return err
}

// mergePost reads the flushed index entries and merges them
// into posting lists, writing the resulting lists to out.
func (ix *Writer) mergePost(out *bufWriter) error {
	var h postHeap

	log.Printf("merge %d files + mem", len(ix.postFile))
	for _, f := range ix.postFile {
		if err := h.addFile(f); err != nil {
			return err
		}
	}
	sortPost(ix.post)
	h.addMem(ix.post)

	npost := 0
	e := h.next()
	offset0 := out.offset()
	for {
		npost++
		offset := out.offset() - offset0
		trigram := e.trigram()
		ix.buf[0] = byte(trigram >> 16)
		ix.buf[1] = byte(trigram >> 8)
		ix.buf[2] = byte(trigram)

		// posting list
		fileID := ^uint32(0)
		nfile := uint32(0)
		if err := out.write(ix.buf[:3]); err != nil {
			return err
		}
		for ; e.trigram() == trigram && trigram != 1<<24-1; e = h.next() {
			if err := out.writeUvarint(e.fileID() - fileID); err != nil {
				return err
			}
			fileID = e.fileID()
			nfile++
		}
		if err := out.writeUvarint(0); err != nil {
			return err
		}

		// index entry
		if err := ix.postIndex.write(ix.buf[:3]); err != nil {
			return err
		}
		if err := ix.postIndex.writeUint32(nfile); err != nil {
			return err
		}
		if err := ix.postIndex.writeUint32(offset); err != nil {
			return err
		}

		if trigram == 1<<24-1 {
			break
		}
	}
	return nil
}

// A postChunk represents a chunk of post entries flushed to disk or
// still in memory.
type postChunk struct {
	e postEntry   // next entry
	m []postEntry // remaining entries after e
}

const postBuf = 4096

// A postHeap is a heap (priority queue) of postChunks.
type postHeap struct {
	ch []*postChunk
}

func (h *postHeap) addFile(f *os.File) error {
	data, err := mmapFile(f)
	if err != nil {
		return err
	}
	d := data.d
	m := (*[npost]postEntry)(unsafe.Pointer(&d[0]))[:len(d)/8]
	h.addMem(m)
	return nil
}

func (h *postHeap) addMem(x []postEntry) {
	h.add(&postChunk{m: x})
}

// step reads the next entry from ch and saves it in ch.e.
// It returns false if ch is over.
func (h *postHeap) step(ch *postChunk) bool {
	old := ch.e
	m := ch.m
	if len(m) == 0 {
		return false
	}
	ch.e = postEntry(m[0])
	m = m[1:]
	ch.m = m
	if old >= ch.e {
		panic("bad sort")
	}
	return true
}

// add adds the chunk to the postHeap.
// All adds must be called before the first call to next.
func (h *postHeap) add(ch *postChunk) {
	if len(ch.m) > 0 {
		ch.e = ch.m[0]
		ch.m = ch.m[1:]
		h.push(ch)
	}
}

// empty reports whether the postHeap is empty.
func (h *postHeap) empty() bool {
	return len(h.ch) == 0
}

// next returns the next entry from the postHeap.
// It returns a postEntry with trigram == 1<<24 - 1 if h is empty.
func (h *postHeap) next() postEntry {
	if len(h.ch) == 0 {
		return makePostEntry(1<<24-1, 0)
	}
	ch := h.ch[0]
	e := ch.e
	m := ch.m
	if len(m) == 0 {
		h.pop()
	} else {
		ch.e = m[0]
		ch.m = m[1:]
		h.siftDown(0)
	}
	return e
}

func (h *postHeap) pop() *postChunk {
	ch := h.ch[0]
	n := len(h.ch) - 1
	h.ch[0] = h.ch[n]
	h.ch = h.ch[:n]
	if n > 1 {
		h.siftDown(0)
	}
	return ch
}

func (h *postHeap) push(ch *postChunk) {
	n := len(h.ch)
	h.ch = append(h.ch, ch)
	if len(h.ch) >= 2 {
		h.siftUp(n)
	}
}

func (h *postHeap) siftDown(i int) {
	ch := h.ch
	for {
		j1 := 2*i + 1
		if j1 >= len(ch) {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < len(ch) && ch[j1].e >= ch[j2].e {
			j = j2
		}
		if ch[i].e < ch[j].e {
			break
		}
		ch[i], ch[j] = ch[j], ch[i]
		i = j
	}
}

func (h *postHeap) siftUp(j int) {
	ch := h.ch
	for {
		i := (j - 1) / 2
		if i == j || ch[i].e < ch[j].e {
			break
		}
		ch[i], ch[j] = ch[j], ch[i]
		j = i
	}
}

// A bufWriter is a convenience wrapper: a closeable bufio.Writer.
type bufWriter struct {
	name string
	file *os.File
	buf  []byte
	tmp  [8]byte
}

// bufCreate creates a new file with the given name and returns a
// corresponding bufWriter. If name is empty, bufCreate uses a
// temporary file.
func bufCreate(name string) (*bufWriter, error) {
	var (
		f   *os.File
		err error
	)
	if name != "" {
		f, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	} else {
		f, err = ioutil.TempFile("", "csearch")
	}
	if err != nil {
		return nil, err
	}
	return &bufWriter{
		name: f.Name(),
		buf:  make([]byte, 0, 256<<10),
		file: f,
	}, nil
}

func (b *bufWriter) write(x []byte) error {
	n := cap(b.buf) - len(b.buf)
	if len(x) > n {
		if err := b.flush(); err != nil {
			return err
		}
		if len(x) >= cap(b.buf) {
			if _, err := b.file.Write(x); err != nil {
				return fmt.Errorf("writing %s: %w", b.name, err)
			}
			return nil
		}
	}
	b.buf = append(b.buf, x...)
	return nil
}

func (b *bufWriter) writeByte(x byte) error {
	if len(b.buf) >= cap(b.buf) {
		if err := b.flush(); err != nil {
			return err
		}
	}
	b.buf = append(b.buf, x)
	return nil
}

func (b *bufWriter) writeString(s string) error {
	n := cap(b.buf) - len(b.buf)
	if len(s) > n {
		if err := b.flush(); err != nil {
			return err
		}
		if len(s) >= cap(b.buf) {
			if _, err := b.file.WriteString(s); err != nil {
				return fmt.Errorf("writing %s: %w", b.name, err)
			}
			return nil
		}
	}
	b.buf = append(b.buf, s...)
	return nil
}

// offset returns the current write offset.
func (b *bufWriter) offset() uint32 {
	off, _ := b.file.Seek(0, 1)
	off += int64(len(b.buf))
	if int64(uint32(off)) != off {
		log.Fatalf("index is larger than 4GB")
	}
	return uint32(off)
}

func (b *bufWriter) flush() error {
	if len(b.buf) == 0 {
		return nil
	}
	_, err := b.file.Write(b.buf)
	if err != nil {
		return fmt.Errorf("writing %s: %w", b.name, err)
	}
	b.buf = b.buf[:0]
	return nil
}

// finish flushes the file to disk and returns an open file ready for reading.
func (b *bufWriter) finish() (*os.File, error) {
	if err := b.flush(); err != nil {
		return nil, err
	}
	f := b.file
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}
	return f, nil
}

func (b *bufWriter) writeTrigram(t uint32) error {
	if cap(b.buf)-len(b.buf) < 3 {
		if err := b.flush(); err != nil {
			return err
		}
	}
	b.buf = append(b.buf, byte(t>>16), byte(t>>8), byte(t))
	return nil
}

func (b *bufWriter) writeUint32(x uint32) error {
	if cap(b.buf)-len(b.buf) < 4 {
		if err := b.flush(); err != nil {
			return err
		}
	}
	b.buf = append(b.buf, byte(x>>24), byte(x>>16), byte(x>>8), byte(x))
	return nil
}

func (b *bufWriter) writeUvarint(x uint32) error {
	if cap(b.buf)-len(b.buf) < 5 {
		if err := b.flush(); err != nil {
			return err
		}
	}
	switch {
	case x < 1<<7:
		b.buf = append(b.buf, byte(x))
	case x < 1<<14:
		b.buf = append(b.buf, byte(x|0x80), byte(x>>7))
	case x < 1<<21:
		b.buf = append(b.buf, byte(x|0x80), byte(x>>7|0x80), byte(x>>14))
	case x < 1<<28:
		b.buf = append(b.buf, byte(x|0x80), byte(x>>7|0x80), byte(x>>14|0x80), byte(x>>21))
	default:
		b.buf = append(b.buf, byte(x|0x80), byte(x>>7|0x80), byte(x>>14|0x80), byte(x>>21|0x80), byte(x>>28))
	}
	return nil
}

// validUTF8 reports whether the byte pair can appear in a
// valid sequence of UTF-8-encoded code points.
func validUTF8(c1, c2 uint32) bool {
	switch {
	case c1 < 0x80:
		// 1-byte, must be followed by 1-byte or first of multi-byte
		return c2 < 0x80 || 0xc0 <= c2 && c2 < 0xf8
	case c1 < 0xc0:
		// continuation byte, can be followed by nearly anything
		return c2 < 0xf8
	case c1 < 0xf8:
		// first of multi-byte, must be followed by continuation byte
		return 0x80 <= c2 && c2 < 0xc0
	}
	return false
}

// sortPost sorts the postentry list.
// The list is already sorted by file ID (bottom 32 bits)
// and the top 8 bits are always zero, so there are only
// 24 bits to sort. Run two rounds of 12-bit radix sort.
const sortK = 12

var sortTmp []postEntry
var sortN [1 << sortK]int

func sortPost(post []postEntry) {
	if len(post) > len(sortTmp) {
		sortTmp = make([]postEntry, len(post))
	}
	tmp := sortTmp[:len(post)]

	const k = sortK
	for i := range sortN {
		sortN[i] = 0
	}
	for _, p := range post {
		r := uintptr(p>>32) & (1<<k - 1)
		sortN[r]++
	}
	tot := 0
	for i, count := range sortN {
		sortN[i] = tot
		tot += count
	}
	for _, p := range post {
		r := uintptr(p>>32) & (1<<k - 1)
		o := sortN[r]
		sortN[r]++
		tmp[o] = p
	}
	tmp, post = post, tmp

	for i := range sortN {
		sortN[i] = 0
	}
	for _, p := range post {
		r := uintptr(p>>(32+k)) & (1<<k - 1)
		sortN[r]++
	}
	tot = 0
	for i, count := range sortN {
		sortN[i] = tot
		tot += count
	}
	for _, p := range post {
		r := uintptr(p>>(32+k)) & (1<<k - 1)
		o := sortN[r]
		sortN[r]++
		tmp[o] = p
	}
}
