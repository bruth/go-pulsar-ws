# go-pulsar-ws

[![GoDoc](https://godoc.org/github.com/bruth/go-pulsar-ws?status.svg)](http://godoc.org/github.com/bruth/go-pulsar-ws)

A Go client for [Apache Pulsar](https://pulsar.incubator.apache.org) using the [Websocket protocol](https://pulsar.incubator.apache.org/docs/latest/clients/WebSocket/).

## Status

Currently, there is no official Go client using the [binary protocol], although the Pulsar team is working on a cgo-based implementation. This library was created as a temporary solution. Once an official solution comes out, I will likely not support this anymore.

## Usage

### Producer

```go
// Initialize a client passing the Pulsar Websocket endpoint.
client := pulsar.New("ws://localhost:8080/ws")

// Initialize a producer given a topic with an optional set of parameters.
// Establishes a websocket connection.
producer, err := client.Producer("persistent/standalone/us-east/test", nil)
if err != nil {
  log.Fatal(err)
}
defer producer.Close()

ctx := context.Background()
res, err := producer.Send(ctx, *pulsar.PublishMsg{
  Payload: []byte("hello world!"),
})
if err != nil {
  log.Fatal(err)
}

// Print the resulting message id.
log.Print(res.MsgId)
```

### Consumer

```go
client := pulsar.New("ws://localhost:8080/ws")

consumer, err := client.Consumer("persistent/standalone/us-east/test", "my-sub", nil)
if err != nil {
  log.Fatal(err)
}
defer consumer.Close()

ctx := context.Background()

for {
  msg, err := consumer.Receive(ctx)
  if err != nil {
    log.Fatal(err)
  }

  // Print message.
  log.Print(string(msg.Payload))

  // Ack once processed.
  if err := consumer.Ack(msg); err != nil {
    log.Fatal(err)
  }
}
```

### Reader

```go
client := pulsar.New("ws://localhost:8080/ws")

reader, err := client.Reader("persistent/standalone/us-east/test", pulsar.Params{
  "messageId": "earliest",
})
if err != nil {
  log.Fatal(err)
}
defer reader.Close()

ctx := context.Background()

for {
  msg, err := reader.Receive(ctx)
  if err != nil {
    log.Fatal(err)
  }

  // Print message.
  log.Print(string(msg.Payload))

  // Ack once processed.
  if err := reader.Ack(msg); err != nil {
    log.Fatal(err)
  }
}
```

## License

MIT
