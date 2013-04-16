package srv

import (
	_ "driver/mypq"
	"database/sql"
)

//connection pooling of the backend postgres instances

var MaxFreeConns = 32 //todo - configurable

type Pool struct {
	Addr  string
	conns chan *sql.DB
}

func NewPool(addr string) *Pool {
	host := &Pool{Addr: addr}
	host.conns = make(chan *sql.DB, MaxFreeConns)

	return host
}

//empty the pool 
func (h *Pool) Clear() error {
	for {
		if _, ok := <-h.conns; !ok {
			break
		}
	}
	return nil
}

func (h *Pool) CreateConn() (c *sql.DB, e error) {
	c, e = sql.Open("postgres", "user=pqtest dbname=pqtest sslmode=disable")

	if e != nil {
		println("open postgres failed")
	}

	return c, e
}

func (h *Pool) GetConn() (c *sql.DB, e error) {
	select {
	case c = <-h.conns:
		return c, nil
	default:
		c, e = h.CreateConn()
		return c, e
	}
    return c, e
}

//if chan is full , close conn else put conn to chan
func (h *Pool) ReleaseConn(conn *sql.DB) {
	/*    select {
	case h.conns <- conn:
	default:
		conn.Close()
	}
	*/
	if len(h.conns) < cap(h.conns) {
		h.conns <- conn
	} else {
		conn.Close()
	}

}
