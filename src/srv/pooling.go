package srv

import (
	_ "driver/mypq"
	"net"
    "database/sql"
    "database/sql/driver"
)

//connection pooling of the backend postgres instances

//note: to refine or even useless at all; but basic idea as below

var MaxFreeConns = 32 //todo - configurable

type Pool struct {
	Addr  string
	conns chan net.Conn
}

func NewPool(addr string) *Pool {
	host := &Pool{Addr: addr}
	host.conns = make(chan net.Conn, MaxFreeConns)

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

func (h *Pool) CreateConn() (c net.Conn, e error) {
    
    //error sql.DB can not convert to net.Conn
	c, e = sql.Open("postgres", "user=pqtest dbname=pqtest")

	if e != nil {
		println("open postgres failed")
	}

	return c, e
}

func (h *Pool) GetConn() (c net.Conn, e error) {
	select {
	case c = <-h.conns:
		return c, nil
	default:
		c, e = h.CreateConn()
		return c, e
	}
}

//if chan is full , close conn else put conn to chan
func (h *Pool) ReleaseConn(conn net.Conn) {
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

