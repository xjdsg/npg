package core

import (
	"fmt"
	"log"
)

//connection pooling of the backend postgres instances

var MaxFreeConns = 32

type Pool struct {
	cnInfo string //connect info string
	conns  chan *Conn
}

func NewPool(cninfo string) *Pool {
	//log.Println("Info: NewPool... ", cninfo)
	pool := &Pool{cnInfo: cninfo}
	pool.conns = make(chan *Conn, MaxFreeConns)

	return pool
}

//empty the pool 
func (p *Pool) Clear() error {
	for {
		if _, ok := <-p.conns; !ok {
			break
		}
	}
	return nil
}

func (p *Pool) CreateConn() (cn *Conn, err error) {
	cn, err = Connect(p.cnInfo)

	if err != nil {
		return nil, fmt.Errorf("Create conn failed: [%s] %v", p.cnInfo, err)
	}

	log.Println("Info: Create a new conn success ", p.cnInfo)
	return
}

func (p *Pool) GetConn() (cn *Conn, err error) {
	select {
	case cn = <-p.conns:
		return cn, nil
	default:
		cn, err = p.CreateConn()
		return cn, err
	}
	return
}

//if chan is full , close conn else put conn to chan
func (p *Pool) ReleaseConn(cn *Conn) {
	if len(p.conns) < cap(p.conns) {
		p.conns <- cn
	} else {
		cn.Close()
	}

}
