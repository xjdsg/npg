package srv

import (
	"bufio"
	//"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

//the config file used by partita is like - 
//hostname:port:database:username:password

var MaxBackends = 32

type Config struct {
	backends []string
	count    int // the num of backends
	port     string
}

func LoadConfigFile(file string) *Config {

	cfg := &Config{backends: make([]string, 0, MaxBackends)}

	fp, err := os.Open(file)
	defer fp.Close()

	if err != nil {
		log.Fatal(err)
	}

	br := bufio.NewReader(fp)
	for {
		line, ok := br.ReadString('\n')
		if ok == io.EOF {
			return cfg
		}
		line = strings.Replace(line, "\n", "", 1)
		if line == "" { //no empty line
			break
		}

		if strings.HasPrefix(line, "//") {
			continue
		}
		pieces := strings.Split(line, " = ") //FIXME
		if len(pieces) < 2 {
			log.Fatal("The format of config file is wrong!")
		}
		switch pieces[0] {
		case "port":
			cfg.port = pieces[1]
		case "count":
			count, err := strconv.Atoi(pieces[1])
			if err != nil {
				log.Fatal("count is not a number!")
			}
			if count > MaxBackends {
				log.Fatal("too many backends!")
			}
			cfg.count = count
		case "backend":
			cfg.backends = append(cfg.backends, pieces[1])
		default:
			log.Fatal(pieces[0], " is not supported!")
		}
	}
	if len(cfg.backends) != cfg.count {
		log.Fatal("backends num is not equal to count!")
	}
	return cfg
}
