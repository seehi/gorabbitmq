package producer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	retryConnectInterval = 2 // seconds
)

type Producer struct {
	url          string
	conn         *amqp.Connection
	channel      *amqp.Channel
	connClose    chan *amqp.Error       // chan for connection close
	channelClose chan *amqp.Error       // chan for channel close
	confirmChan  chan amqp.Confirmation // chan for message confirm
}

// New create a Producer with successful connection, otherwise it will retry until success
func New(user, password, host, port string) *Producer {
	p := &Producer{
		url: fmt.Sprintf("amqp://%s:%s@%s:%s/", user, password, host, port),
	}
	p.connect()
	return p
}

// connect ensure connection and channel
func (p *Producer) connect() {
	var err error
	for {
		if p.conn, err = amqp.Dial(p.url); err != nil {
			logrus.WithError(err).Errorf("rabbitmq connect %s failed, retrying in %d sec", p.url, retryConnectInterval)
			time.Sleep(retryConnectInterval * time.Second)
			continue
		}
		if err = p.initChannel(); err != nil {
			p.Close()
			logrus.WithError(err).Errorf("rabbitmq init channel %s failed, retrying in %d sec", p.url, retryConnectInterval)
			time.Sleep(retryConnectInterval * time.Second)
			continue
		}
		logrus.Infof("rabbitmq connect %s successful.", p.url)
		return
	}
}

// initChannel ensure channel, then monitor the close of connection&channel and the confirm of message
func (p *Producer) initChannel() (err error) {
	if p.channel, err = p.conn.Channel(); err != nil {
		return
	}
	if err = p.channel.Confirm(false); err != nil {
		return fmt.Errorf("could not be put into confirm mode: %s", err)
	}
	go p.listenClose()
	go p.listenConfirm()
	return
}

// listenClose will reconnect if connnection or channel close
func (p *Producer) listenClose() {
	var (
		err error
		tip string
	)
	p.connClose = p.conn.NotifyClose(make(chan *amqp.Error))
	p.channelClose = p.channel.NotifyClose(make(chan *amqp.Error))
	// if conneClose then drain channelClose, and vice versa
	select {
	case err = <-p.connClose:
		tip = "connection close"
		go p.drainChan(p.channelClose)
	case err = <-p.channelClose:
		tip = "channel close"
		go p.drainChan(p.connClose)
	}
	if err == nil {
		logrus.Infof("rabbitmq normal exit")
		return
	}
	logrus.Errorf("rabbitmq %s, %s", tip, err)
	p.Close()
	p.connect()
}

// drainChan avoid goroutine blocking
func (p *Producer) drainChan(ch <-chan *amqp.Error) {
	for range ch {
	}
}

func (p *Producer) Close() {
	if !p.conn.IsClosed() {
		p.conn.Close()
	}
}

// listenConfirm receive ack or nack of every message
func (p *Producer) listenConfirm() {
	p.confirmChan = p.channel.NotifyPublish(make(chan amqp.Confirmation, 100))
	for c := range p.confirmChan {
		if c.Ack {
			logrus.Infof("rabbitmq confirm ack %d", c.DeliveryTag)
		} else {
			logrus.Errorf("rabbitmq confirm nack %d", c.DeliveryTag)
		}
	}
}

// InitQueue decalre queue
func (p *Producer) InitQueue(queueNames []string) {
	for _, name := range queueNames {
		_, err := p.channel.QueueDeclare(
			name,  // name
			true,  // durable
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			logrus.Fatal("rabbitmq declare queue failed: ", name)
		}
	}
}

// Send publish msg to exchange with the routingKey of the queue name
func (p *Producer) Send(exchange, key string, msg map[string]interface{}) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("rabbitmq send fail: json marshal %v", err)
	}
	err = p.channel.Publish(
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Timestamp:    time.Now(),
		})
	p.tracePublish(exchange, key, msg, err)
	return err
}

// tracePublish log detail and status
func (p *Producer) tracePublish(exchange, key string, msg map[string]interface{}, err error) {
	if msg == nil {
		msg = make(map[string]interface{})
	}
	msg["exchange"] = exchange
	msg["key"] = key
	msg["time"] = time.Now()
	logrus.Infof("rabbitmq send %t: %v", err == nil, p.view(msg))
}

func (p *Producer) view(i interface{}) string {
	s, _ := json.Marshal(i)
	return string(s)
}
