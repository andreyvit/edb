// Command evmbind generates a package of native wrappers for an EVM API.
package main

import (
	"flag"
	"log"
	"os"

	"github.com/andreyvit/edb/evm/evmbind"
)

func main() {
	dir := flag.String("dir", ".", "directory of guest API Go sources")
	path := flag.String("path", "", "guest import path")
	native := flag.String("native", "", "native Go import path")
	pkg := flag.String("package", "", "generated package")
	out := flag.String("o", "", "output file")
	flag.Parse()
	b, err := evmbind.Generate(*path, *native, *pkg, os.DirFS(*dir))
	if err != nil {
		log.Fatal(err)
	}
	if err = os.WriteFile(*out, b, 0644); err != nil {
		log.Fatal(err)
	}
}
