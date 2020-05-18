package main

import (
	"os"

	_ "bazil.org/plop/internal/blobdrivers"
	"bazil.org/plop/internal/cli"
)

//go:generate go run ../../task/gen-imports.go -o commands.gen.go bazil.org/plop/internal/cli/...

func main() {
	code := cli.Main()
	os.Exit(code)
}
