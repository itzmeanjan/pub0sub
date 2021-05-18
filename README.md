# pub0sub
Fast, Lightweight Pub/Sub over TCP, QUIC

## Motivation

Few days back I worked on an _Embeddable, Fast, Light-weight Pub/Sub System for Go Projects_, called `pubsub`, which is built only using native Go functionalities & as the title suggests, you can embed that system in your application for doing in-app message passing using any of following patterns

- Single Publisher Single Subscriber
- Single Publisher Multiple Subscriber
- Multiple Publisher Single Subscriber
- Multiple Publisher Multiple Subscriber

That enables making multiple go routines talk to each other over **topics**. Also there's no involvement of network I/O, so all operations are quite low-latency.

> If you're interested in taking a look at [`pubsub`](https://github.com/itzmeanjan/pubsub)

Now I'm interested in extending aforementioned `pubsub` architecture to a more generic form so that clients i.e. _{publishers, subscribers}_ can talk to **Pub/Sub Hub** over network i.e. TCP, QUIC.

What it gives us is, ability to publish messages to topics over network, where **Pub/Sub Hub** might sit somewhere else; subscribe to topics of interest & keep receiving messages as soon as they're published, over network.

> QUIC to be preferred choice of network I/O, due to benefits it brings on table.

> ⭐️ Primary implementation is on top of TCP.

## Architecture

![architecture](./sc/architecture.jpg)

## Install

Add `pub0sub` into your project _( **GOMOD** enabled )_

```bash
go get -u github.com/itzmeanjan/pub0sub
```

## Usage

`pub0sub` has three components

- [Hub](#hub)
- [Publisher](#publisher)
- Subscriber

### Hub

You probably would like to use `0hub` for this purpose.

Build using 

```bash
make build_hub
```

Run using

```bash
./0hub -help
./0hub # run
```

> You can build & run with `make hub`

> If interested, you can check `0hub` implementation [here](./cli/hub/0hub.go)

### Publisher

You can interact with Hub, using minimalistic publisher CLI client `0pub`. Implementation can be found [here](./cli/publisher/0pub.go)

Build using

```bash
make build_pub
```

Run using

```bash
./0pub -help
./0pub # run
```

> Single step build-and-run with `make pub`, using defaults

You're probably interested in publishing messages programmatically. 

- Let's first create a publisher, which will establish TCP connection with Hub

```go
ctx, cancel := context.WithCancel(context.Background())

pub, err := publisher.New(ctx, "tcp", "127.0.0.1:13000")
if err != nil {
	return
}
```

- Construct message you want to publish

```go
data := []byte("hello")
topics := []string{"topic_1", "topic_2"}

msg := ops.Msg{Topics: topics, Data: data}
```

- Publish message

```go
n, err := pub.Publish(&msg)
if err != nil {
	return
}

log.Printf("Approximate %d receiver(s)\n", n)
```

- When done using publisher instance, cancel context, which will tear down network connection gracefully

```go
cancel()
<-time.After(time.Second) // just wait a second
```

- You can always check whether network connection with Hub in unaffected or not

```go
if pub.Connected() {
    log.Println("Yes, still connected")
}
```

**More coming soon**
