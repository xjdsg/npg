package main

import (
	pq "driver/mypq"
	//"database/sql"
	//"core"
	"log"
)

func main() {
	_, err := pq.Open("user=pqtest dbname=pqtest password=pqtest sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	//_, err = conn.Query("select * from test")

	if err != nil {
		log.Fatal(err)
	}
	log.Println("connect success")
/*
	_, err = core.Connect("host=ec2-107-22-163-119.compute-1.amazonaws.com port=5432 dbname=d8tvi9d1am0q2v user=xfrxpvfdflhznr password=TTEhF-VoC5VYKpl9xnMxAmYqjN sslmode=require")

	if err != nil {
		log.Println(err)
	} else {
		log.Println("connect success!")
	}
*/
}
