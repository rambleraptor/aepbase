package main

import (
	"flag"
	"log"

	"github.com/rambleraptor/aepbase/pkg/aepbase"
)

func main() {
	var opts aepbase.ServerOptions
	opts.RegisterFlags()
	flag.Parse()

	if err := aepbase.Run(opts); err != nil {
		log.Fatal(err)
	}
}
