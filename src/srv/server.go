package srv

import (
	_ "driver/mypq"
	"log"
	"net/http"
	//"database/sql"
	"fmt"
)

type PartitaServer struct {
	pools    []*Pool //for each backend pg
	port     string  //self port
	backend  string  //backend postgres addr
	dbname   string
	username string
	passwd   string
}

var partita *PartitaServer

func NewPartitaServer(num int) *PartitaServer {
	return &PartitaServer{pools: make([]*Pool, num)}
}

func StartPartita(port string, backends []string) {
	n := len(backends)
	if n <= 0 {
		log.Fatal("no backends")
	}

	partita = NewPartitaServer(n)

	//make a connection pool for each backend pg
	for i := 0; i < n; i++ {
		partita.pools[i] = NewPool(backends[i])
	}

	//parse the config file to set port, backend, dbname, ...
	//cfg := LoadConfigFile(configFile)

	//start http server
    log.Println("wait for query ...  ",fmt.Sprintf(":%s", port),)
	http.HandleFunc("/query", DMLHandler)
	err := http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}

//POST sql string to /query?mode=xx&op=xx&flag=xx ...
//op: select/update/insert, it is optional
//mode: random, parallel, it is required
//flag: slave, it is optional
func DMLHandler(w http.ResponseWriter, r *http.Request) {
	//parse URL to get mode etc.

	//read the sql string from req body

	//do remote executions, and merge the results

	sql := r.FormValue("sql")
    opt := r.FormValue("opt")
	//mode :=r.FormValue("mode")

	//for simple, just use one backend
	pool := partita.pools[0]
	cn, err := pool.GetConn()
	if err != nil {
		log.Fatal("GetConn: ", err)
	}

	switch opt {
	case "select":
		_, err := cn.Query(sql)  //cann't use Exec, why?
		if err != nil {
            log.Fatal("Query failed : ", err)
		} else {
			log.Println("Query: %s success", sql)
		}
	case "insert", "update", "delete":
		_, err := cn.Exec(sql)
		if err != nil {
            log.Fatal("Exec failed : ", err)
		} else {
			log.Println("Exec: %s success", sql)

		}
	default:
        log.Fatal("opt not support : ", opt)
	}
}

func handleRandomInsert(sql string, w http.ResponseWriter) error {
	//todo
	return nil
}

func handleParallelUpdate(sql string, w http.ResponseWriter) error {
	return nil
}

func handleParallelSelect(sql string, w http.ResponseWriter) error {
	return nil
}
