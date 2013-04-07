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

//POST to /query?op=select/insert/update&mode=random/parallel
func DMLHandler(w http.ResponseWriter, r *http.Request) error {
	//read the sql string

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
