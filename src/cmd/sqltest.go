package main

import (
	"log"
	"parser"
    "fmt"
)

func main() {
	sql := "select test.b, test.c, a+b, count(1,a+b) from test where t1.a = 10 and  test.b = 'C' order by a+b, b"
	tree, err := parser.Parse(sql)
	if err != nil {
		log.Fatal("Parser Error")
	}
	fmt.Println(tree.TreeString())
    fmt.Println(tree.String())
    parser.ExecParse(sql)


}
