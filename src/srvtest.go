package main

import (
	"srv"
)

func main() {
	backends := []string{"localhost"}
	StartPartitaServer("1234", backends)
}
