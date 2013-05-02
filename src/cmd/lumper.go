package main

import (
	"flag"
	"srv"
)

var Version = "0.0.1"

//the server main program
func main() {
	//the only parameter is the config file path
	configFile := flag.String("cfg", "/Users/XJ/Projects/npg/npg.conf", "config file path")
	flag.Parse()

	srv.StartPartita(*configFile)
}
