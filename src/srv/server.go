package srv

import (
	"net/http"
)

type PartitaServer struct {
	pools []*Pool //for each backend pg
}

var partita *PartitaServer

func StartPartita(port string, backends []string) {
	partita = new(PartitaServer)

	//make a connection pool for each backend pg

	//start http server
}

//POST sql string to /query?mode=xx&op=xx&flag=xx ...
//op: select/update/insert, it is optional
//mode: random, parallel, it is required
//flag: slave, it is optional
func DMLHandler(w http.ResponseWriter, r *http.Request) error {
	//parse URL to get mode etc.

	//read the sql string from req body

	//do remote executions, and merge the results

	return nil
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
