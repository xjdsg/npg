package parser

import (
	"testing"
	//   "fmt"
)

func TestExecParse(t *testing.T) {
	sqlt := "select a,b from test1 join test2"
	tree, err := Parse(sqlt)
	if err != nil {
		t.Fatal("Parser Error")
	}
	t.Log(tree.TreeString())
	t.Log(tree.String())

	//testing select targets and order targets, func() as name
	sql := "select b, a+b, count(a,b) from test where t1.a = 10 and  test.b = 'C' order by a+b, b"
	aquery, _ := ExecParse(sql)
	t.Log(aquery.GetString())
	for _, target := range aquery.TargetList {
		t.Log(target.GetString())
	}
	t.Log(sql)

	//testing name in order targets
	sql = "select b, a+b as c, count(*) from test where t1.a = 10 and  test.b = 'C' order by b, c"
	aquery, _ = ExecParse(sql)
	t.Log(aquery.GetString())
	for _, target := range aquery.TargetList {
		t.Log(target.GetString())
	}
	t.Log(sql)

	t.Error("test")

}
