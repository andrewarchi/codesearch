// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"

	"github.com/andrewarchi/codesearch/index"
)

var usageMessage = `usage: cindex [-list] [-reset] [-index path] [path...]

cindex prepares the trigram index for use by csearch. The index is the
file named by the -index flag or $CSEARCHINDEX variable. If both are
empty, the index path defaults to ~/.csearchindex.

The simplest invocation is

	cindex path...

which adds the file or directory tree named by each path to the index.
For example:

	cindex ~/src /usr/include

or, equivalently:

	cindex ~/src
	cindex /usr/include

If cindex is invoked with no paths, it reindexes the paths that have
already been added, in case the files have changed. Thus, 'cindex' by
itself is a useful command to run in a nightly cron job.

The -list flag causes cindex to list the paths it has indexed and exit.

By default cindex adds the named paths to the index but preserves
information about other paths that might already be indexed
(the ones printed by cindex -list). The -reset flag causes cindex to
delete the existing index before indexing the new paths.
With no path arguments, cindex -reset removes the index.
`

func usage() {
	fmt.Fprintf(os.Stderr, usageMessage)
	os.Exit(2)
}

var (
	listFlag    = flag.Bool("list", false, "list indexed paths and exit")
	resetFlag   = flag.Bool("reset", false, "discard existing index")
	indexFlag   = flag.String("index", "", "path to the index")
	logSkipFlag = flag.Bool("logskip", false, "log skipped files")
	verboseFlag = flag.Bool("verbose", false, "print extra information")
	cpuProfile  = flag.String("cpuprofile", "", "write cpu profile to this file")
)

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()

	if *listFlag {
		ix, err := index.Open(index.File())
		if err != nil {
			log.Fatal(err)
		}
		paths, err := ix.Paths()
		if err != nil {
			log.Fatal(err)
		}
		for _, arg := range paths {
			fmt.Printf("%s\n", arg)
		}
		return
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

	if *resetFlag && len(args) == 0 {
		os.Remove(index.File())
		return
	}
	if len(args) == 0 {
		ix, err := index.Open(index.File())
		if err != nil {
			log.Fatal(err)
		}
		paths, err := ix.Paths()
		if err != nil {
			log.Fatal(err)
		}
		for _, arg := range paths {
			args = append(args, arg)
		}
	}

	// Translate paths to absolute paths so that we can
	// generate the file list in sorted order.
	for i, arg := range args {
		a, err := filepath.Abs(arg)
		if err != nil {
			log.Printf("%s: %s", arg, err)
			args[i] = ""
			continue
		}
		args[i] = a
	}
	sort.Strings(args)

	for len(args) > 0 && args[0] == "" {
		args = args[1:]
	}

	primary := *indexFlag
	if primary == "" {
		primary = index.File()
	}
	if _, err := os.Stat(primary); err != nil {
		// Does not exist.
		*resetFlag = true
	}
	file := primary
	if !*resetFlag {
		file += "~"
	}

	ix, err := index.Create(file)
	if err != nil {
		log.Fatal(err)
	}
	ix.LogSkip = *logSkipFlag || *verboseFlag
	ix.Verbose = *verboseFlag
	ix.AddPaths(args)
	for _, arg := range args {
		log.Printf("index %s", arg)
		err := filepath.Walk(arg, func(path string, info os.FileInfo, err error) error {
			if _, elem := filepath.Split(path); elem != "" {
				// Skip various temporary or "hidden" files or directories.
				if elem[0] == '.' || elem[0] == '#' || elem[0] == '~' || elem[len(elem)-1] == '~' {
					if info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}
			if err != nil {
				log.Printf("%s: %s", path, err)
				return nil
			}
			// Avoid symlinks.
			if info != nil && info.Mode()&os.ModeType == 0 {
				return ix.AddFile(path)
			}
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("flush index")
	if err := ix.Flush(); err != nil {
		log.Fatal(err)
	}

	if !*resetFlag {
		log.Printf("merge %s %s", primary, file)
		if err := index.Merge(file+"~", primary, file); err != nil {
			log.Fatal(err)
		}
		os.Remove(file)
		os.Rename(file+"~", primary)
	}
	log.Printf("done")
	return
}
