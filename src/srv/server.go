package srv

import (
	"core"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

type PartitaServer struct {
	drv  *core.BDriver
	port string
	num  int //backends num
}

var partita *PartitaServer

func StartPartita(configFile string) {
	//parse the config file to set port, backend, dbname, ...
	cfg := LoadConfigFile(configFile)

	drv := core.NewBDriver(cfg.backends)

	partita = &PartitaServer{drv: drv, port: cfg.port, num: len(cfg.backends)}

	//start http server
	log.Println("Info: Start partita serving: ", cfg.port)

	http.HandleFunc("/query", DMLHandler)

	log.Println("Info: Waiting for query ...  ")

	err := http.ListenAndServe(fmt.Sprintf(":%s", cfg.port), nil)
	if err != nil {
		log.Fatalf("ListenAndServe: ", err)
	}

}

//POST sql string to /query?mode=xx&op=xx&flag=xx ...
//op: select/update/insert/delete, optional
//mode: random, parallel, required
//flag: slave, optional
func DMLHandler(w http.ResponseWriter, r *http.Request) {
	//parse URL to get sql,mode, etc.
	sql := r.FormValue("sql")
	//opt := r.FormValue("opt")
	mode := r.FormValue("mode")
	//flag := r.FormValue("flag")
	switch mode {
	case "parallel":
		rs, err := partita.drv.ExecParallelQuery(sql)
		if err != nil {
			fmt.Fprint(w, err)
		}
		err = writeResponse(rs, w)
		if err != nil {
			fmt.Fprint(w, err)
		}
	case "random":
		rand.Seed(time.Now().UTC().UnixNano())
		idx := rand.Intn(partita.num-0) + 0
		log.Println("Info: random processed at backend ", partita.drv.GetBackends()[idx])
		rs, err := partita.drv.ExecSingleQuery(sql, idx)
		if err != nil {
			fmt.Fprint(w, err)
		}
		err = writeResponse(rs, w)
		if err != nil {
			fmt.Fprint(w, err)
		}
	default:
		fmt.Fprint(w, "Error: mode %s not support\n", mode)
	}
}

/*
func getShardsPoolIdx(mode string) []int {

}
*/

func writeResponse(rs *core.Result, w http.ResponseWriter) (err error) {
	if rs == nil {
		return nil
	}
	if rs.RowsRetrieved() == 0 {
		rowsAffected, _ := rs.RowsAffected()
		if rowsAffected > 0 {
			fmt.Fprintf(w, "Rows Affected is %d\n", rowsAffected)
		} else {
			fmt.Fprintf(w, "0 row returned or row affected is 0\n")
		}
		return nil
	}
	for _, f := range rs.Fields() {
		fmt.Fprintf(w, "%s ", f.Name)
	}
	fmt.Fprintf(w, "\n----------------\n")
	for _, r := range rs.Rows() {
		for _, v := range r {
			fmt.Fprintf(w, "%s ", v.Raw())
		}
		fmt.Fprintf(w, "\n")
	}
	fmt.Fprintf(w, "-----------------\n%d rows\n", rs.RowsRetrieved())
	return
}
