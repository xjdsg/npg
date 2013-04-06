package srv

//connection pooling of the backend postgres instances

//note: to refine or even useless at all; but basic idea as below

var MaxFreeConns = 32	//todo - configurable

type Pool struct {
	Addr     string
	conns    chan net.Conn
}

func NewPool(addr string) *Pool {
	host := &Pool{Addr: addr}
	host.conns = make(chan net.Conn, MaxFreeConns)

	return host
}

func (h *Pool) Clear() error {
	//todo
	return nil
}

func (h *Pool) createConn() (c net.Conn, e error) {
	//todo
	return
}

func (h *Pool) getConn() (c net.Conn, e error) {
	select {
	case c = <-h.conns:
	default:
		c, e = h.createConn()
	}

	return
}

func (h *Pool) releaseConn(conn net.Conn) {
	select {
	case h.conns <- conn:
	default:
		conn.Close()
	}
}
