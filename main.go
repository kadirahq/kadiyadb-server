package main

import (
	"flag"
	"log"

	"net/http"
	_ "net/http/pprof"
)

var (
	data = flag.String("data", "/tmp/data/", "Where the databases files are located")
	addr = flag.String("addr", ":8000", "Host and port of kadiyadb server <host>:<port>")
	prof = flag.String("prof", ":6060", "Host and port of pprof server <host>:<port>")
)

func main() {
	flag.Parse()

	log.Println("Listening:")
	log.Println("  data : " + *addr)
	if *prof != "" {
		log.Println("  pprof: " + *prof)
		go http.ListenAndServe(*prof, nil)
	}

	if err := Listen(*addr, *data); err != nil {
		panic(err)
	}
}
