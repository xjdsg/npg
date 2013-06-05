package core

import (
	"testing"
)

func TestConnect(t *testing.T) {

	conn, err := Connect("user=pqtest dbname=pqtest password=pqtest")

	if err != nil {
		t.Log(err)
	} else {
		t.Log("connect success!")
	}

	rs, err := conn.Exec("select * from test")
	n, _ := rs.RowsAffected()
	t.Log("rowsaffected: ", n)
}

func TestHerokuConnect(t *testing.T) {
	conn, err = Connect("host=ec2-107-22-163-119.compute-1.amazonaws.com port=5432 dbname=d8tvi9d1am0q2v user=xfrxpvfdflhznr password=TTEhF-VoC5VYKpl9xnMxAmYqjN sslmode=require")

	if err != nil {
		t.Log(err)
	} else {
		t.Log("connect success!")
	}

	rs, err = conn.Exec("select * from test")
	n, _ = rs.RowsAffected()
	t.Log("rowsaffected: ", n)

}


func TestQueryRewrite(t *testing.T) {

	// test order by target in the query rewrite
	sql := "select a, b from test order by c"
	query := QueryRewrite(sql)
	t.Log(sql)
	t.Log(query)

	// test *
	sql = "select * from test order by a+b"
	query = QueryRewrite(sql)
	t.Log(sql)
	t.Log(query)

}
