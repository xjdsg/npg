package core

import (
	"fmt"
	"log"
    "strings"
)

var MaxBackends = 32

//backends driver
type BDriver struct {
	pools    []*Pool // for each backend pg 
	backends []string     //backend   hostname:port:dbname:username:password
}

func NewBDriver(backends []string) *BDriver {
	n := len(backends)
	if n <= 0 {
		log.Fatal("Error: no backends specified")
	}
	drv := &BDriver{pools: make([]*Pool, 0, MaxBackends),
		backends: backends}

	//make a connection pool for each backend pg
	for _, backend := range backends {
		log.Println("Info: Connecting backend ", backend)
		drv.pools = append(drv.pools, NewPool(drv.GetString(backend))) //FIXME
	}
	return drv
}

//backend   hostname:port:dbname:username:password
func (drv *BDriver) AddBackend(backend string) error {
	if len(drv.backends) == MaxBackends {
		return fmt.Errorf("Info: We have reached the max backends")
	}
	drv.pools = append(drv.pools, NewPool(drv.GetString(backend)))

	return nil
}

func (drv *BDriver) GetBackends() []string {
	return drv.backends
}

//Exec a query on a single backend.
//idx: index of backend
func (drv *BDriver) ExecSingleQuery(sql string, idx int) (*Result, error) {
	if idx < 0 || idx > len(drv.pools) {
		return nil, fmt.Errorf("idx cann't be larger than %d or smaller than %d", len(drv.pools), 0)
	}
	cn, err := drv.pools[idx].GetConn()
	defer drv.pools[idx].ReleaseConn(cn)
	if err != nil {
		return nil, err
	}
	rs, err := cn.Exec(sql)
	return rs, err
}

//Exec query on all backends parallelly
func (drv *BDriver) ExecParallelQuery(sql string) (*Result, error) {
	rchan := make(chan *Result, len(drv.backends))
	for _, pool := range drv.pools {
		go func(pool *Pool) {
			cn, err := pool.GetConn()
			defer pool.ReleaseConn(cn)
			if err != nil {
				rchan <- &Result{error: err}
				return
			}
			rs, err := cn.Exec(sql)
			if err != nil {
				rchan <- &Result{error: err}
				return
			}
			rchan <- rs
		}(pool)
	}

	//sum all results 
	results := make([]*Result, len(drv.backends))
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
		return nil, fmt.Errorf("Partial result set has error (%v)", hasError)
	}

	//no rows return like update/delete/insert, or select result is empty 
	if rowCount <= 0 {
        rs := NewResult(rowCount, rowsAffected, int64(0), results[0].Fields())
		return rs, nil
	}

	// check the schemas all match (both names and types)
	if len(results) > 1 {
		firstFields := results[0].Fields()
		for _, r := range results[1:] {
			fields := r.Fields()
			if len(fields) != len(firstFields) {
				return nil, fmt.Errorf("server: column count mismatch: %v != %v", len(firstFields), len(fields))
			}
			for i, field := range fields {
				if field.Name != firstFields[i].Name {
					return nil, fmt.Errorf("server: column[%v] name mismatch: %v != %v", i, field.Name, firstFields[i].Name)
				}
			}
		}
	}

	//combine results
	rs := NewResult(rowCount, rowsAffected, int64(0), results[0].Fields())
	idx := 0
	rows := rs.Rows()
	for _, tr := range results {
		for _, row := range tr.Rows() {
			rows[idx] = row
			idx++
		}
	}
	return rs, nil
}

// Input: "hostname:port:dbname:username:password".
// Output: "hostname= port= dbname= username= password= " for connecting
func (drv *BDriver) GetString(str string) string {
	pieces := strings.Split(str, ":")
    s := make([]string, 0, 5)
	if pieces[0] != "" {
		s = append(s, fmt.Sprintf("host=%s", pieces[0]))
	}
	if pieces[1] != "" {
		s = append(s, fmt.Sprintf("port=%s", pieces[1]))
	}
	if pieces[2] != "" {
		s = append(s, fmt.Sprintf("dbname=%s", pieces[2]))
	}
	if pieces[3] != "" {
		s = append(s, fmt.Sprintf("user=%s", pieces[3]))
	}
	if pieces[4] != "" {
		s = append(s, fmt.Sprintf("password=%s", pieces[4]))
	}
	//log.Println("conn params: ", s)
	return strings.Join(s, " ")

}
