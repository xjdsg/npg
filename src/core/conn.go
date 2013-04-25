//the core stuff here
//drive multiple postgres instances and behave as one logic entity.
package core

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/user"
	"strings"
	//    "database/sql/driver"
	"log"
)

type connParams struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

type Conn struct {
	cn     net.Conn
	params *connParams
	reader *bufio.Reader
	writer *bufio.Writer
}

type Values map[string]string

func (vs Values) Set(k, v string) {
	vs[k] = v
}

func (vs Values) Get(k string) (v string) {
	v, _ = vs[k]
	return
}

func (conn *Conn) parseParams(s string) *connParams {
	if len(s) == 0 {
		return nil
	}

	o := make(Values)
	o.Set("host", "localhost")
	o.Set("port", "5432")

	u, err := user.Current()
	if err == nil {
		o.Set("user", u.Username)
	}

	for k, v := range parseEnviron(os.Environ()) {
		o.Set(k, v)
	}

	ps := strings.Split(s, " ")
	for _, p := range ps {
		kv := strings.Split(p, "=")
		if len(kv) < 2 {
			fmt.Println("invalid option: %q", p) //fix
		}
		o.Set(kv[0], kv[1])
	}

	params := &connParams{}
	params.Host = o.Get("host")
	params.Port = o.Get("port")
	params.User = o.Get("user")
	params.Database = o.Get("dbname") //fix
	params.Password = o.Get("password")

	return params

}

func Connect(s string) (conn *Conn, err error) {
	conn = &Conn{}
	params := conn.parseParams(s)
	conn.params = params

	cn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", params.Host, params.Port))
	if err != nil {
		fmt.Println("dial error")
	}
	conn.cn = cn
	conn.reader = bufio.NewReader(cn)
	conn.writer = bufio.NewWriter(cn)

	conn.writeStartup()

	conn.readAuth()

	return

}

func (conn *Conn) Close() (err error) {
	conn.writeTerminate()
	panicIfErr(conn.cn.Close())
	return
}

func (conn *Conn) Query(sql string, args ...interface{}) (rs *Result, err error) {
	if len(args) == 0 {
		conn.writeQuery(sql)
		rs = conn.getResult()
	} else {
		st, _ := conn.Prepare(sql)
		rs, err = st.Exec(args)

	}
	return
}

func (conn *Conn) Prepare(sql string) (st *Stmt, err error) {
	st = &Stmt{cn: conn, query: sql, name: "stmt"} //fix name
	conn.writeParse(st)
	conn.writeDescribe(st)
	conn.getPreparedStmt(st)
	return
}

/*

//Execute is the same as Query except the returns, Query is Rows, Execute is rowAffected
func (conn *Conn) Execute(sql string, args ...interface{}) (rs *Result, err error) {
    st, err := conn.Prepare(sql)
    rs, err  = st.Exec(args)
    return
}

*/
type stParams struct {
	name  string
	ptype int32
	value interface{}
}

type Stmt struct {
	cn     *Conn
	query  string
	name   string
	params []*stParams
}

func (st *Stmt) Exec(args ...interface{}) (rs *Result, err error) {
	if len(args) == 0 {
		log.Println("simple query")
		return st.cn.Query(st.query) //fix
	}
	if len(args) != len(st.params) {
		fmt.Println("args and params different")
		return nil, nil
	}
	for i := 0; i < len(args); i++ {
		st.params[i].value = args[i]
	}
	st.cn.writeBind(st)
	st.cn.writeExecute(st)
	st.cn.writeSync()
	rs = st.cn.getResult()
	return
}

/*
// driver.Stmt interface
func (stmt *Stmt) Close() error {
	stmt.query = ""
	return nil
}

func (*Stmt) NumInput() int {
	return -1
}


func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	rs, err := stmt.cn.Execute(stmt.query, args)
	if err != nil {
		return nil, err
	}
	return rs.(driver.Rows), nil
}
*/
func parseEnviron(env []string) (out map[string]string) {
	out = make(map[string]string)

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		accrue := func(keyname string) {
			out[keyname] = parts[1]
		}

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual, with omissions briefly
		// noted.
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			accrue("hostaddr")
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
		// skip PGPASSFILE, PGSERVICE, PGSERVICEFILE,
		// PGREALM
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGREQUIRESSL":
			accrue("requiressl")
		case "PGSSLCERT":
			accrue("sslcert")
		case "PGSSLKEY":
			accrue("sslkey")
		case "PGSSLROOTCERT":
			accrue("sslrootcert")
		case "PGSSLCRL":
			accrue("sslcrl")
		case "PGREQUIREPEER":
			accrue("requirepeer")
		case "PGKRBSRVNAME":
			accrue("krbsrvname")
		case "PGGSSLIB":
			accrue("gsslib")
		case "PGCONNECT_TIMEOUT":
			accrue("connect_timeout")
		case "PGCLIENTENCODING":
			accrue("client_encoding")
			// skip PGDATESTYLE, PGTZ, PGGEQO, PGSYSCONFDIR,
			// PGLOCALEDIR
		}
	}

	return out
}
