package srv

import (
	_ "driver/mypq"
	"log"
	"net/http"
    //"database/sql"
    "fmt"
)

type PartitaServer struct {
	pools []*Pool //for each backend pg
}

var partita *PartitaServer

func NewPartitaServer(num int) *PartitaServer{
	return &PartitaServer{pools: make([]*Pool, num)}
}

func StartPartita(port string, backends []string) {
	n := len(backends)
	if n <= 0 {
		log.Println("no backends")
	}

	partita = NewPartitaServer(n)

	//make a connection pool for each backend pg
	for i := 0; i < n; i++ {
		partita.pools[i] = NewPool(backends[i])
	}

	//start http server

	http.HandleFunc("/query", DMLHandler)
	err := http.ListenAndServe(fmt.Sprintf(":%s", port),nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}

//POST to /query?sql=select * from ...&opt=select/insert/update,delete&mode=random
//read the sql string, do remote executions, and merge the results

func DMLHandler(w http.ResponseWriter, r *http.Request) {
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
		rows, err := cn.Query(sql)
		if err != nil {
			log.Fatal("Query: %s failed", sql)
		} else {
			log.Println("Query: %s success", sql)
		}
	case "insert", "update", "delete":
		res, err := cn.Exec(sql)
		if err != nil {
			log.Fatal("Exec: %s failed", sql)
		} else {
			log.Println("Exec: %s success", sql)

		}
	default:
		log.Fatal("opt %s not support")
	}

}
