package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	pulsar "github.com/bruth/go-pulsar-ws"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	var (
		url string
	)

	flag.StringVar(&url, "url", "ws://localhost:8080/ws", "Websocket endpoint")

	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		return errors.New("topic required")
	}

	topic := args[0]

	client := pulsar.New(url)

	p, err := client.Producer(topic, nil)
	if err != nil {
		return err
	}
	defer p.Close()

	res, err := p.Send(context.Background(), &pulsar.PublishMsg{
		Payload: []byte(`{"foo": 1, "bar": true}`),
	})
	if err != nil {
		return err
	}

	b, _ := json.Marshal(res)
	fmt.Println(string(b))

	return nil
}
