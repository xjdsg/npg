package main

import (
	"core"
	"fmt"
)

func main() {

	conn, err := core.Connect("user=pqtest dbname=pqtest")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("connect success!")
	}

	//st, err := conn.Prepare("select * from test where a=$1")
	//st.Exec(11)

	rs, err := conn.Exec("create TEMP table tmp(i int)")
	n, _ := rs.RowsAffected()
	fmt.Println("rowsaffected: ", n)
	rs, err = conn.Exec("insert into tmp values(5)")
	n, _ = rs.RowsAffected()
	fmt.Println("rowsaffected: ", n)

	rs, err = conn.Exec("select * from tmp")
	n, _ = rs.RowsAffected()

}
