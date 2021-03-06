// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp/syntax"
	"runtime/pprof"

	"github.com/andrewarchi/codesearch/index"
	"github.com/andrewarchi/codesearch/regexp"
)

var usageMessage = `usage: csearch [-c] [-f fileregexp] [-index path] [-h] [-i] [-l] [-n] regexp

csearch behaves like grep over all indexed files, searching for regexp,
an RE2 (nearly PCRE) regular expression.

The -c, -h, -i, -l, and -n flags are as in grep, although note that as
per Go's flag parsing convention, they cannot be combined: the option
pair -i -n cannot be abbreviated to -in.

The -f flag restricts the search to files whose names match the RE2
regular expression fileregexp.

csearch relies on the existence of an up-to-date index created ahead of
time. To build or rebuild the index that csearch uses, run:

	cindex path...

where path... is a list of directories or individual files to be
included in the index. If no index exists, this command creates one.
If an index already exists, cindex overwrites it. Run cindex -help for
more.

The path to the index is named by the -index flag or $CSEARCHINDEX
variable. If both are empty, the current working directory and parents
are recursively searched for a .csearchindex file. If none is found, an
index is created at ~/.csearchindex.
`

func usage() {
	fmt.Fprintf(os.Stderr, usageMessage)
	os.Exit(2)
}

var (
	fFlag       = flag.String("f", "", "search only files with names matching this regexp")
	iFlag       = flag.Bool("i", false, "case-insensitive search")
	indexFlag   = flag.String("index", "", "path to the index")
	verboseFlag = flag.Bool("verbose", false, "print extra information")
	bruteFlag   = flag.Bool("brute", false, "brute force - search all files in index")
	cpuProfile  = flag.String("cpuprofile", "", "write cpu profile to this file")
)

func main() {
	g := regexp.Grep{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	g.AddFlags()

	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	if len(args) != 1 {
		usage()
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	reFlags := syntax.Perl &^ syntax.OneLine
	if *iFlag {
		reFlags |= syntax.FoldCase
	}
	re, err := regexp.CompileFlags(args[0], reFlags)
	if err != nil {
		log.Fatal(err)
	}
	g.Regexp = re
	var fre *regexp.Regexp
	if *fFlag != "" {
		fre, err = regexp.Compile(*fFlag)
		if err != nil {
			log.Fatal(err)
		}
	}
	q := index.RegexpQuery(re.Syntax)
	if *verboseFlag {
		log.Printf("query: %s\n", q)
	}
	if *bruteFlag {
		q = &index.Query{Op: index.QAll}
	}

	indexPath := *indexFlag
	if indexPath == "" {
		indexPath = index.File()
	}
	ix, err := index.Open(indexPath)
	if err != nil {
		log.Fatal(err)
	}
	ix.Verbose = *verboseFlag
	post, err := ix.PostingQuery(q)
	if err != nil {
		log.Fatal(err)
	}
	if *verboseFlag {
		log.Printf("post query identified %d possible files\n", len(post))
	}

	if fre != nil {
		filenames := make([]uint32, 0, len(post))

		for _, fileID := range post {
			name, err := ix.Name(fileID)
			if err != nil {
				log.Fatal(err)
			}
			if fre.MatchString(name, true, true) < 0 {
				continue
			}
			filenames = append(filenames, fileID)
		}

		if *verboseFlag {
			log.Printf("filename regexp matched %d files\n", len(filenames))
		}
		post = filenames
	}

	for _, fileID := range post {
		name, err := ix.Name(fileID)
		if err != nil {
			log.Fatal(err)
		}
		g.File(name)
	}

	if !g.Match {
		os.Exit(1)
	}
}
