package srv

import (
	"flag"
	"testing"
)

func TestStartPartita(t *testing.T) {
	//the only parameter is the config file path
	configFile := flag.String("cfg", "/Users/XJ/Projects/npg/npg.conf", "config file path") //"/Users/XJ/Projects/npg/npg.conf",
	flag.Parse()

	StartPartita(*configFile)
}
