package srv

import (
	"os"
	//"bufio"
	//"fmt"
	"log"
)

//the config file used by partita is like -
//	port: port
//	backend: xxx
//	username: xxx
//	passwd:	xxx
//	dbname:	xxx

type Config struct {
	port     string
	backend  string
	username string
	passwd   string
	dbname   string
}

func LoadConfigFile(file string) *Config {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	cfg := new(Config)
	//parse the file
	return cfg
}

func (cfg *Config) GetString(key string) string {
	return "hello"
}
