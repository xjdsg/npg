package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
)

func main() {
	// Create 10 worker goroutines each of which acquires and uses a
	// connection from the pool.
	var wg sync.WaitGroup
	nthreads := 1
	wg.Add(nthreads)
	for i := 0; i < nthreads; i++ {
		go worker(i+1, &wg)
	}
	wg.Wait() // Wait for all the workers to finish.
}

func worker(i int, wg *sync.WaitGroup) {
	log.Println("worker: ", i)
	resp, _ := http.PostForm("http://localhost:8888/query",
		//url.Values{"sql": {"insert into test values(25, 'D')"}, "mode": {"random"}})
		url.Values{"sql": {"select * from test"}, "mode": {"parallel"}})
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
	wg.Done()
}
