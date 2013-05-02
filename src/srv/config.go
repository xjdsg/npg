package srv

import (
	"bufio"
	"fmt"
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

// string format is hostname:port:dbname:username:password
// hostname is required
func GetString(str string) (s string) {
	pieces := strings.Split(str, ":")
	if pieces[0] != "" {
		s = fmt.Sprintf("host=%s", pieces[0]) //fix if host is null,
	}
	if pieces[1] != "" {
		s = fmt.Sprintf("%s port=%s", s, pieces[1])
	}
	if pieces[2] != "" {
		s = fmt.Sprintf("%s dbname=%s", s, pieces[2])
	}
	if pieces[3] != "" {
		s = fmt.Sprintf("%s user=%s", s, pieces[3])
	}
	if pieces[4] != "" {
		s = fmt.Sprintf("%s password=%s", s, pieces[4])
	}
	//log.Println("conn params: ", s)
	return

}
