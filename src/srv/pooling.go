package srv

//connection pooling of the backend postgres instances

//note: to refine further; but basic idea as below

import (
	"database/sql"
	"driver/mypq"
)

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

func (h *Pool) Clear() error {
	//todo
	return nil
}

func (h *Pool) createConn() (c *sql.DB, e error) {
	//todo
	return
}

func (h *Pool) getConn() (c *sql.DB, e error) {
	select {
	case c = <-h.conns:
	default:
		c, e = h.createConn()
	}

	return
}

func (h *Pool) releaseConn(conn *sql.DB) {
	select {
	case h.conns <- conn:
	default:
		conn.Close()
	}
}
