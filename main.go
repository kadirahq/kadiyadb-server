package main

import (
	"flag"
	"fmt"

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

	if *prof != "" {
		go http.ListenAndServe(*prof, nil)
	}

	s, err := NewServer(*addr, *data)
	if err != nil {
		panic(err)
	}

	fmt.Println("Listening on " + *addr)
	if err := s.Start(); err != nil {
		fmt.Println(err)
	}
}
