package clt

import (
	"net/http"
)

type Client struct {
	ServerAddr string
}

func NewClient(server string) *Client {
	c:= new(Client)
	c.ServerAddr = server
}

func (c *Client) Fire(sql, mode, op, flag string) ([]byte, error) {
	//POST sql to the server with mode/op/flag as URL parameters if they are not empty
	return nil, nil
}
