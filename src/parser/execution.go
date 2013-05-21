//analyze node tree
package parser

import (
	"bytes"
	"fmt"
	"log"
)

type ResTarget struct {
	Name        string   // the name after AS, if not, ""
	Expr        string   // the var like user, user+2
	FuncName    string   // if the target has function
	FuncVarList []string // the vars of function, if has function
	// Isjunk     bool     // false: not junk, the target is the result column, 
	// true:  target is not result, like order/group target
	OrderInfo string //asc, desc
}

type AnalyzedQuery struct {
	OriQuery    string
	TableName   string
	TargetList  []*ResTarget // the target column name
	ResMaxIdx   int          // the max index of targetlist of result,the target before the index is
	OrderIdx    []int        //the indexs orders from targetlist
	HasOrder    bool
	HasGroup    bool
	HasDistinct bool
	HasAggs     bool // target or havingClause has aggregate function like sum, avg, count ...
}

func ExecParse(sql string) (aquery *AnalyzedQuery, err error) {
	defer handleError(&err)

	tree, err := Parse(sql)
	if err != nil {
		return nil, err
	}

	aquery = tree.execAnalyzeSql()
	return
}

func (node *Node) execAnalyzeSql() *AnalyzedQuery {
	switch node.Type {
	case SELECT:
		return node.ExecSelectQuery()
	}
	return nil
}

func (node *Node) ExecSelectQuery() (aquery *AnalyzedQuery) {
	aquery = &AnalyzedQuery{}

	//from --> tablename
	tableName, err := node.At(SELECT_FROM_OFFSET).execAnalyzeFrom()
	if err != nil {
		log.Fatal(err)
	}

	aquery.TableName = tableName

	// targets
	targets := node.At(SELECT_EXPR_OFFSET).execAnalyzeSelectTargets()
	aquery.ResMaxIdx = len(targets) - 1

	for _, target := range targets {
		log.Println(target.Expr)
	}
	// orders
	orders := node.At(SELECT_ORDER_OFFSET).execAnalyzeOrder()
	if len(orders) > 0 {
		aquery.HasOrder = true
	}

	for _, order := range orders {
		log.Println(order.Expr)
	}

	// merge targets and orders
	targetList, orderIdx := mergeTargetsOrders(targets, orders)
	aquery.TargetList = targetList
	aquery.OrderIdx = orderIdx

	//HasAggs......
	return

}

func mergeTargetsOrders(targets, orders []*ResTarget) ([]*ResTarget, []int) {
	targetList := make([]*ResTarget, len(targets), len(targets)+len(orders))
	for i, target := range targets {
		targetList[i] = target
	}

	orderIdx := make([]int, len(orders))
	for i, order := range orders {
		orderIdx[i] = -1
		for j, target := range targets {
			if order.Expr == target.Expr || order.Expr == target.Name {
				orderIdx[i] = j
				targetList[j].OrderInfo = order.OrderInfo
			}
		}
		if orderIdx[i] == -1 {
			orderIdx[i] = len(targetList)
			targetList = append(targetList, order)
		}
	}
	return targetList, orderIdx
}

//-----------------------------------------------
// From

func (node *Node) execAnalyzeFrom() (tabelname string, err error) {
	if node.Len() > 1 {
		return "", fmt.Errorf("tablename more than one")
	}
	if node.At(0).Type != TABLE_EXPR {
		return "", fmt.Errorf("tablename is wrong")
	}
	node = node.At(0).At(0)

	switch node.Type {
	case AS:
		return string(node.At(0).Value), nil
	case ID:
		return string(node.Value), nil
	case '.':
		return string(node.At(1).Value), nil //like public.table
	}
	// sub-select
	return "", fmt.Errorf("we can not support this format")
}

//-----------------------------------------------
// Target Expressions

//FIXME: we don't check the target in table, the target maybe not found
func (node *Node) execAnalyzeSelectTargets() (targets []*ResTarget) {
	targets = make([]*ResTarget, 0, node.Len())
	for i := 0; i < node.Len(); i++ {
		target := &ResTarget{}
		node.At(i).getSelectTarget(target)
		if target == nil { //nil???
			log.Fatal("select target having field can not be parsed")
		} else {
			targets = append(targets, target)
		}
	}
	return
}

func (node *Node) getSelectTarget(target *ResTarget) {
	switch node.Type {
	case FUNCTION:
		target.FuncName = string(node.Value)
		target.Expr = node.getOpExpr()
		varList := make([]string, 0, node.Len())

		for i := 0; i < node.At(0).Len(); i++ {
			varList = append(varList, node.At(0).At(i).getOpExpr())
		}

		target.FuncVarList = varList
		target.Name = "--"

	case ID, SELECT_STAR:
		target.Expr = string(node.Value)

	case '.':
		target.Expr = string(node.At(1).Value) //table.column

	case AS:
		node.At(0).getSelectTarget(target)
		target.Name = string(node.At(1).Value)

	case '+', '-', '*', '/', '%', '&', '|', '^':
		target.Expr = node.getOpExpr()
		target.Name = "--"
	}
	return
}

func (node *Node) getOpExpr() (expr string) {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("%v", node)
	return buf.String()
}

//-----------------------------------------------
// Order

func (node *Node) execAnalyzeOrder() (orders []*ResTarget) {
	orders = make([]*ResTarget, 0, 8)
	if node.Len() == 0 {
		return orders
	}
	orderList := node.At(0)
	for i := 0; i < orderList.Len(); i++ {
		order := &ResTarget{}
		orderList.At(i).getOrderTarget(order)
		if order != nil {
			orders = append(orders, order)
		} else {
			log.Fatal("order target having field can not be parsed")
		}
	}
	return orders
}

func (node *Node) getOrderTarget(order *ResTarget) {
	switch node.Type {
	case ID:
		order.Expr = string(node.Value)
	case '.':
		order.Expr = string(node.At(1).Value)
	case '+', '-', '*', '/', '%', '&', '|', '^':
		order.Expr = node.getOpExpr()
	case ASC:
		order.OrderInfo = "asc"
		node.At(0).getOrderTarget(order)
	case DESC:
		order.OrderInfo = "desc"
		node.At(0).getOrderTarget(order)
		/*case '(':
		return node.At(0).getOrderTarget(order)
		*/ //FIXME
	}
	return
}

// for test
func (rt *ResTarget) GetString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "name: %s, expr: %s, funcname: %s, funcvarlist: ",
		rt.Name, rt.Expr, rt.FuncName)

	for _, varl := range rt.FuncVarList {
		fmt.Fprintf(buf, "%s, ", varl)
	}

	fmt.Fprintf(buf, "orderinfo: %s", rt.OrderInfo)
	return buf.String()
}

//for test without targetlist
func (aq *AnalyzedQuery) GetString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	fmt.Fprintf(buf, "oriquery: %s\ntablename: %s\nresmaxidx: %d\noderidx: ",
		aq.OriQuery, aq.TableName, aq.ResMaxIdx)
	for _, idx := range aq.OrderIdx {
		fmt.Fprintf(buf, "%d, ", idx)
	}
	fmt.Fprintf(buf, "\nhasorder: %v\nhasgroup: %v\nhasdistinct: %v\nhasAggs: %v\n",
		aq.HasOrder, aq.HasGroup, aq.HasDistinct, aq.HasAggs)
	return buf.String()
}
