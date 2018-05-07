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

var encodings = map[string]struct{}{
	"json":     {},
	"protobuf": {},
	"string":   {},
	"bytes":    {},
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	var (
		url      string
		encoding string
		subName  string
	)

	flag.StringVar(&url, "url", "ws://localhost:8860/ws", "Websocket endpoint")
	flag.StringVar(&encoding, "encoding", "json", "Encoding. Choices are: json, protobuf, string, bytes.")
	flag.StringVar(&subName, "sub", "my-sub", "Subscription name.")

	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		return errors.New("topic required")
	}

	topic := args[0]

	if _, ok := encodings[encoding]; !ok {
		return errors.New("invalid encoding")
	}

	client := pulsar.New(url)

	c, err := client.Consumer(topic, subName, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx := context.Background()

	tmp := make(map[string]interface{})

	for {
		msg, err := c.Receive(ctx)
		if err != nil {
			return err
		}

		tmp["messageId"] = msg.MsgId
		tmp["key"] = msg.Key
		tmp["publishTime"] = msg.PublishTime

		switch encoding {
		case "json":
			tmp["payload"] = json.RawMessage(msg.Payload)
		case "string":
			tmp["payload"] = string(msg.Payload)
		default:
			tmp["payload"] = msg.Payload
		}

		b, _ := json.Marshal(tmp)
		fmt.Println(string(b))

		if err := c.Ack(ctx, msg); err != nil {
			return fmt.Errorf("ack failed: %s", err)
		}
	}

	return nil
}
