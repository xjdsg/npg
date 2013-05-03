package srv

import (
	"bufio"
	//"fmt"
	"io"
	"log"
	"os"
	"strings"
)

//the config file used by partita is like - 
//hostname:port:database:username:password

type Config struct {
	backends []string
	port     string
}

func LoadConfigFile(file string) *Config {

	cfg := &Config{backends: make([]string, 0, 32)}

	fp, err := os.Open(file)
	defer fp.Close()

	if err != nil {
		log.Fatal(err)
	}

	br := bufio.NewReader(fp)
	for {
		line, ok := br.ReadString('\n')
		line = strings.Replace(line, "\n", "", 1)
		if ok == io.EOF {
			return cfg
		}
		pieces := strings.Split(line, " = ") //FIXME
		if len(pieces) < 2 {
			log.Fatal("The format of config file is wrong.")
		}
		switch pieces[0] {
		case "port":
			cfg.port = pieces[1]
		case "backend":
			cfg.backends = append(cfg.backends, pieces[1])
		default:
			log.Fatal(pieces[0], " is not supported")
		}
	}
	return cfg
}


