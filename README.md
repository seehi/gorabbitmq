# Gorabbitmq
robust rabbitmq producer, auto reconnect
# Installation
```
go get -u github.com/seehi/gorabbitmq
```
# Quick start
```
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
		queue    = "test"
		msg      = map[string]interface{}{"test": true}
		p        = producer.New(user, password, host, port)
	)
	for {
		_ = p.Send("", queue, msg)
		time.Sleep(2 * time.Second)
	}
}
```
