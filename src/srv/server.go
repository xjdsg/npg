package srv

import (
	"driver/pgsql"
	"log"
	"net/http"
	"fmt"
    "time"
)

type PartitaServer struct {
	pools    []*pgsql.Pool //for each backend pg
	port     string        //self port
	backend  string        //backend postgres addr
	dbname   string
	username string
	passwd   string
}

var partita *PartitaServer

func StartPartita(port string, backends []string) {
	n := 2 //len(backends)
	if n <= 0 {
		log.Fatal("no backends")
	}

	partita = &PartitaServer{pools: make([]*pgsql.Pool,n)}
	var err error
	//make a connection pool for each backend pg
	//for i := 0; i < n; i++ {
	partita.pools[0], err = pgsql.NewPool("dbname=pqtest user=pqtest port=5432", 3, 5, pgsql.DEFAULT_IDLE_TIMEOUT)
	if err != nil {
		log.Fatal("Error opening connection pool: %s\n", err)
	}

	//partita.pools[0].Debug = true
	//}

	partita.pools[1], err = pgsql.NewPool("dbname=pqtest2 user=pqtest2 port=5433", 3, 5, pgsql.DEFAULT_IDLE_TIMEOUT)
	if err != nil {
	       log.Fatalf("Error opening connection pool: %s\n", err)
	 }
	//partita.pools[1].Debug = true

	//parse the config file to set port, backend, dbname, ...
	//cfg := LoadConfigFile(configFile)

	//start http server
	log.Println("wait for query ...  ", fmt.Sprintf(":%s", port))
	http.HandleFunc("/query", DMLHandler)
	err = http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
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
    pool := partita.pools[0]  //the global partita is invalid in handler!!!
	cn, err := pool.Acquire()
	if err != nil {
		log.Fatal("GetConn: ", err)
	}

	switch opt {
	case "select":
		rs, err := cn.Query(sql)
        if err != nil {
			log.Fatal("Query failed : ", err)
		} else {
			log.Println("Query: %s success", sql)
			fieldCount := rs.FieldCount()
			log.Println("fieldCount:", fieldCount)
			for {
				hasRow, _ := rs.FetchNext()
				if !hasRow {
					log.Println("has no Row")
					break
				}
                r0,_,_ :=rs.Any(0)
                r1,_,_ :=rs.Any(1)
				log.Println(r0,r1)
                t := fmt.Sprintf("%d %s\n",r0,r1)
                w.Write([]byte(t))
			}
		}
	case "insert", "update", "delete":
		_, err := cn.Execute(sql)
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

