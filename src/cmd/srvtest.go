package main

import (
	"srv"
    "log"
)

func main() {
	backends := []string{"localhost","localhost"}
    log.Println("start partita serving 8888")
	srv.StartPartita("8888", backends)
}
