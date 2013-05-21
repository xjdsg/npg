1.First Stage
    parse simple sql:
     with order clause
     no subquery
     no function
     no as

select a from test group by b;
ERROR:  column "test.a" must appear in the GROUP BY clause or be used in an aggregate function
select sum(a),b from test;
ERROR:  column "test.b" must appear in the GROUP BY clause or be used in an aggregate function

select a+2 from test;
 ?column? 

