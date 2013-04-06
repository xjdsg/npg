package srv

import (
	"net/http"
)

type PartitaServer {
	pools		[]*Pool	//for each backend pg
}

var partita *PartitaServer

func StartPartita(port string, backends []string) {
	partita = new(PartitaServer)
	
	//make a connection pool for each backend pg
	
	//start http server
}

//POST to /query?type=select/insert/update
func DMLHandler(w http.ResponseWriter, r *http.Request) {
	//read the sql string, do remote executions, and merge the results
}


