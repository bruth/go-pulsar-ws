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
		earliest bool
		encoding string
		msgId    string
	)

	flag.StringVar(&url, "url", "ws://localhost:8860/ws", "Websocket endpoint")
	flag.StringVar(&msgId, "msgid", "", "Message ID to start at.")
	flag.StringVar(&encoding, "encoding", "json", "Encoding. Choices are: json, protobuf, string, bytes.")
	flag.BoolVar(&earliest, "earliest", false, "Start with the earliest message.")

	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		return errors.New("topic required")
	}

	topic := args[0]

	if earliest && msgId != "" {
		return errors.New("cannot set earliest and msgid")
	}

	if _, ok := encodings[encoding]; !ok {
		return errors.New("invalid encoding")
	}

	p := pulsar.Params{}

	if earliest {
		p["messageId"] = "earliest"
	}
	if msgId != "" {
		p["messageId"] = msgId
	}

	client := pulsar.New(url)

	r, err := client.Reader(topic, p)
	if err != nil {
		return err
	}
	defer r.Close()

	ctx := context.Background()

	tmp := make(map[string]interface{})

	for {
		msg, err := r.Receive(ctx)
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

		if err := r.Ack(ctx, msg); err != nil {
			return fmt.Errorf("ack failed: %s", err)
		}
	}

	return nil
}
