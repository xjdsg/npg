package srv

import (
	"core"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

var MaxBackends = 32

type PartitaServer struct {
	pools    []*core.Pool // for each backend pg 
	backends []string     //backend   hostname:port:dbname:username:password
	port     string
}

var partita *PartitaServer

func StartPartita(configFile string) {
	//parse the config file to set port, backend, dbname, ...
	cfg := LoadConfigFile(configFile)
	n := len(cfg.backends)
	if n <= 0 {
		log.Fatal("Error: no backends")
	}

	partita = &PartitaServer{pools: make([]*core.Pool, 0, MaxBackends),
		backends: cfg.backends,
		port:     cfg.port}

	//make a connection pool for each backend pg
	for _, backend := range cfg.backends {
		log.Println("Info: Connecting backend ", backend)
		partita.pools = append(partita.pools, core.NewPool(GetString(backend)))
	}

	//start http server
	log.Println("Info: Start partita serving: ", cfg.port)

	http.HandleFunc("/query", DMLHandler)

	log.Println("Info: Waiting for query ...  ")

	err := http.ListenAndServe(fmt.Sprintf(":%s", cfg.port), nil)
	if err != nil {
		log.Fatalf("ListenAndServe: ", err)
	}

}

//backend   hostname:port:dbname:username:password
func AddBackend(backend string) error {
	if len(partita.backends) == MaxBackends {
		return fmt.Errorf("Info: We have reached the max backends")
	}
	partita.pools = append(partita.pools, core.NewPool(GetString(backend)))

	return nil
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
	var err error
	switch mode {
	case "parallel":
		err = handleParallelQuery(sql, w)
		if err != nil {
			fmt.Fprint(w, err)
		}
	case "random":
		rand.Seed(time.Now().UTC().UnixNano())
		idx := rand.Intn(len(partita.backends)-0) + 0
		log.Println("Info: random processed at backend ", partita.backends[idx])
		err = handleSingleQuery(sql, idx, w)
		if err != nil {
			fmt.Fprint(w, err)
		}
	default:
		fmt.Fprint(w, "Error: mode %s not support\n", mode)
	}
}

func handleSingleQuery(sql string, idx int, w http.ResponseWriter) (err error) {
	cn, err := partita.pools[idx].GetConn()
	defer partita.pools[idx].ReleaseConn(cn)
	if err != nil {
		return
	}
	rs, err := cn.Exec(sql)
	if err != nil {
		return
	}
	err = writeResponse(rs, w)
	return err
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

//do remote executions, and merge the results
func handleParallelQuery(sql string, w http.ResponseWriter) error {
	rchan := make(chan *core.Result, len(partita.backends))
	for _, pool := range partita.pools {
		go func(pool *core.Pool) {
			cn, err := pool.GetConn()
			defer pool.ReleaseConn(cn)
			if err != nil {
				rchan <- &core.Result{error: err}
				return
			}
			rs, err := cn.Exec(sql)
			if err != nil {
				rchan <- &core.Result{error: err}
				return
			}
			rchan <- rs
		}(pool)
	}

	//sum all results 
	results := make([]*core.Result, len(partita.backends))
	rowCount := int64(0)
	rowsAffected := int64(0)
	var hasError error
	for i := range results {
		results[i] = <-rchan
		if results[i].error != nil {
			hasError = results[i].error
			continue
		}
		affected, _ := results[i].RowsAffected()
		rowsAffected += affected
		rowCount += results[i].RowsRetrieved()
	}

	if hasError != nil {
		return fmt.Errorf("Partial result set has error (%v)", hasError)
	}

	//no rows return like update/delete/insert, or select result is empty 
	if rowCount == 0 {
		if rowsAffected > 0 {
			fmt.Fprintf(w, "Rows Affected is %d\n", rowsAffected)
		} else {
			fmt.Fprintf(w, "0 row returned or row affected is 0\n")
		}
		return nil
	}
	/*
		var fields []core.Field
		if len(results) > 0 {
			fields = results[0].Fields()
		}*/

	// check the schemas all match (both names and types)
	if len(results) > 1 {
		firstFields := results[0].Fields()
		for _, r := range results[1:] {
			fields := r.Fields()
			if len(fields) != len(firstFields) {
				return fmt.Errorf("server: column count mismatch: %v != %v", len(firstFields), len(fields))
			}
			for i, field := range fields {
				if field.Name != firstFields[i].Name {
					return fmt.Errorf("server: column[%v] name mismatch: %v != %v", i, field.Name, firstFields[i].Name)
				}
			}
		}
	}

	//combine results
	rs := core.NewResult(rowCount, rowsAffected, int64(0), results[0].Fields())
	idx := 0
	rows := rs.Rows()
	for _, tr := range results {
		for _, row := range tr.Rows() {
			rows[idx] = row
			idx++
		}
	}

	writeResponse(rs, w)
	return nil
}
