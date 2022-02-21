//go:build task
// +build task

// Generate side effect only import statements, usually used for
// registering plugins.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"text/template"

	"golang.org/x/tools/go/packages"
)

var (
	genOutput  = flag.String("o", "", "output path")
	genPackage = flag.String("package", os.Getenv("GOPACKAGE"), "Go package name")
)

var gen = template.Must(template.New("gen").Parse(`package {{.Package}}

import (
{{range .Imports}}{{"\t"}}_ "{{.PkgPath}}"
{{end}})
`))

var prog = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", prog)
	fmt.Fprintf(os.Stderr, "  %s -o PATH PACKAGE..\n", prog)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Options:\n")
	flag.PrintDefaults()
}

func expandPackages(spec []string) ([]*packages.Package, error) {
	// expand "..."
	pkgs, err := packages.Load(&packages.Config{Mode: packages.NeedName}, spec...)
	if err != nil {
		return nil, err
	}
	if packages.PrintErrors(pkgs) > 0 {
		return nil, errors.New("errors loading packages")
	}
	return pkgs, nil
}

func process(dst string, imports []string) error {
	dir := filepath.Dir(dst)
	tmp, err := ioutil.TempFile(dir, "temp-gen-import-all-")
	if err != nil {
		return err
	}
	closed := false
	removed := false
	defer func() {
		if !closed {
			// silence errcheck
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}
	}()

	pkgs, err := expandPackages(imports)
	if err != nil {
		return fmt.Errorf("listing packages: %v", err)
	}

	type state struct {
		Package string
		Imports []*packages.Package
	}
	s := state{
		Package: *genPackage,
		Imports: pkgs,
	}
	if err := gen.Execute(tmp, s); err != nil {
		return fmt.Errorf("template error: %v", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cannot write temp file: %v", err)
	}
	closed = true

	if err := os.Rename(tmp.Name(), *genOutput); err != nil {
		return fmt.Errorf("cannot finalize file: %v", err)
	}
	removed = true

	return nil
}

func main() {
	log.SetFlags(0)
	log.SetPrefix(prog + ": ")

	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if *genOutput == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *genPackage == "" {
		log.Fatal("$GOPACKAGE must be set or -package= passed")
	}

	if err := process(*genOutput, flag.Args()); err != nil {
		log.Fatal(err)
	}
}
