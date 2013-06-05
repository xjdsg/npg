package core

//process sql rewrite and result merge

import (
	"bytes"
	"fmt"
	"log"
	"parser"
	"strings"
)

//rewrite sql if having not order, group, aggs, distinct , just return the origin sql 
func QueryRewrite(sql string) string {
	aquery, _ := parser.ExecParse(sql)

	if !aquery.HasOrder && !aquery.HasGroup && !aquery.HasAggs && !aquery.HasDistinct {
		return sql
	}

	buf := bytes.NewBuffer(make([]byte, 0, 128))

	// select targets include order target
	fmt.Fprintf(buf, "select ")

	hasStar := false
	if len(aquery.TargetList) > 1 {
		s := make([]string, len(aquery.TargetList))
		for i, target := range aquery.TargetList {
			if target.Expr == "*" {
				hasStar = true
				break
			}
			s[i] = target.Expr
		}

		if hasStar {
			fmt.Fprintf(buf, "* ")
		} else {
			fmt.Fprintf(buf, "%s ", strings.Join(s, ","))
		}
	} else if len(aquery.TargetList) == 1 {
		fmt.Fprintf(buf, "%s, ", aquery.TargetList[0].Expr)
	} else {
		log.Fatal("no target ...")
	}

	// from clause
	fmt.Fprintf(buf, "from %s", aquery.TableName)

	// order by clause
	if aquery.HasOrder {
		fmt.Fprintf(buf, "order by ")
		for _, i := range aquery.OrderIdx {
			if i == len(aquery.OrderIdx)-1 { //the last one
				fmt.Fprintf(buf, "%s %s", aquery.TargetList[i].Expr, aquery.TargetList[i].OrderInfo)
				break
			}
			fmt.Fprintf(buf, "%s %s, ", aquery.TargetList[i].Expr, aquery.TargetList[i].OrderInfo)
		}
	}

	return buf.String()
}

/*
func MergeResult(){
}

func ExecSort(){
}

func ExecFunction() {
    switch {
    case "count":
    case "sum":
    case "avg":
    case "max":
    case "min":
    case "distinct":
    }
}

*/
