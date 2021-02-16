# Code Search

Code Search is a tool for indexing and then performing
regular expression searches over large bodies of source code.
It is a set of command-line programs written in Go.

For background and an overview of the commands,
see http://swtch.com/~rsc/regexp/regexp4.html.

To install:

```sh
go get github.com/andrewarchi/codesearch/cmd/...
```

Use `go get -u` to update an existing installation.

Russ Cox<br>
rsc@swtch.com<br>
June 2015

## About this fork

This fork introduces a number of features and bug fixes.

- Skips files excluded by local, global, and system .gitignore files
- Improves usage as library:
  - Returns error values, rather than exiting, to give control to caller
  - Adds `regexp.CompileFlags`
  - Adds `(*index.Index).NumNames` ([evanj]) and `(*index.Index).Names`
- Searches current working directory and parents for a .csearchindex
  file ([tomnomnom])
- Adds flags to `cindex`:
  - `-index` path to the index ([taliesinb])
  - `-nogitignore` do not skip files in .gitignore
  - `-logskip` log skipped files
- Adds flags to `cgrep`:
  - `-index` path to the index ([taliesinb])
- Adds flags to `csearch`:
  - `-0` null delimit file names ([taliesinb])
- Updates build scripts for current Go tools

[evanj]:     https://github.com/evanj/codesearch
[taliesinb]: https://github.com/taliesinb/codesearch
[tomnomnom]: https://github.com/tomnomnom/codesearch
