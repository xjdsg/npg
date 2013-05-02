//the core stuff here
//drive multiple postgres instances and behave as one logic entity.
package core

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/user"
	"strings"
)

type cnParams struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

type Conn struct {
	cn     net.Conn
	params *cnParams
	reader *bufio.Reader
	writer *bufio.Writer
}

func (conn *Conn) parseParams(s string) *cnParams {
	if len(s) == 0 {
		return nil
	}
	vs := make(map[string]string)
	vs["host"] = "localhost"
	vs["port"] = "5432"

	u, err := user.Current()
	if err == nil {
		vs["user"] = u.Username
	}
	for k, v := range parseEnviron(os.Environ()) {
		vs[k] = v
	}
	ps := strings.Split(s, " ")
	for _, p := range ps {
		kv := strings.Split(p, "=")
		if len(kv) < 2 {
			log.Fatal("Error: invalid option ", p)
		}
		vs[kv[0]] = kv[1]
	}
	params := &cnParams{}
	params.Host, _ = vs["host"]
	params.Port, _ = vs["port"]
	params.User, _ = vs["user"]
	params.Database, _ = vs["dbname"]
	params.Password, _ = vs["password"]
	return params
}

func Connect(s string) (conn *Conn, err error) {
	conn = &Conn{}
	params := conn.parseParams(s)
	cn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", params.Host, params.Port))
	if err != nil {
		log.Println("dial error")
		return nil, err
	}
	conn.cn = cn
	conn.params = params
	conn.reader = bufio.NewReader(cn)
	conn.writer = bufio.NewWriter(cn)
	conn.writeStartup()
	err = conn.readAuth()
	return
}

func (conn *Conn) Exec(sql string, args ...interface{}) (rs *Result, err error) {
	if len(args) == 0 {
		conn.writeQuery(sql)
		rs, err = conn.getResult()
	} else {
		st, err := conn.Prepare(sql)
		if err != nil {
			return nil, err
		}
		rs, err = st.Exec(args)
	}
	return
}

func (conn *Conn) Prepare(sql string) (st *Stmt, err error) {
	st = &Stmt{cn: conn, query: sql, name: "stmt"} //FIXME: stmt name 
	conn.writeParse(st)
	conn.writeDescribe(st)
	err = conn.getPreparedStmt(st)
	return
}

func (conn *Conn) Close() (err error) {
	conn.writeTerminate()
	panicIfErr(conn.cn.Close())
	return
}

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
		return st.cn.Exec(st.query, nil)
	}
	if len(args) != len(st.params) {
		return nil, fmt.Errorf("Error: args and params different")
	}
	for i := 0; i < len(args); i++ {
		st.params[i].value = args[i]
	}
	st.cn.writeBind(st)
	st.cn.writeExecute(st)
	st.cn.writeSync()
	rs, err = st.cn.getResult()
	return
}

func (stmt *Stmt) Close() error {
	stmt.query = ""
	return nil
}

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
		// skip PGPASSFILE, PGSERVICE, PGSERVICEFILE,PGREALM
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
			// skip PGDATESTYLE, PGTZ, PGGEQO, PGSYSCONFDIR,PGLOCALEDIR
		}
	}
	return out
}
