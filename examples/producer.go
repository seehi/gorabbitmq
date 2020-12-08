package main

import (
	"time"

	"github.com/seehi/gorabbitmq/producer"
)

func main() {
	var (
		user     = "guest"
		password = "guest"
		host     = "127.0.0.1"
		port     = "5672"
		queue    = "fortest"
		msg      = map[string]interface{}{"test": true}
		p        = producer.New(user, password, host, port)
	)
	for {
		err := p.Send("", queue, msg)
		if err != nil {
			println(err)
		}
		time.Sleep(2 * time.Second)
	}
}
