# pub0sub
Fast, Lightweight Pub/Sub over TCP, QUIC

## Motivation

Few days back I worked on an _Embeddable, Fast, Light-weight Pub/Sub System for Go Projects_, called `pubsub`, which is built only using native Go functionalities i.e. **go channels**, **go routines** & as the title suggests, you can embed that system in your application for doing in-app message passing using following patterns

- Single Publisher Single Subscriber
- Single Publisher Multiple Subscriber
- Multiple Publisher Single Subscriber
- Multiple Publisher Multiple Subscriber

That enables making multiple go routines talk to each other over **topics**. Also there's no involvement of network I/O, so all operations are quite low-latency.

> If you're interested in taking a look at [`pubsub`](https://github.com/itzmeanjan/pubsub)

Now I'm interested in extending aforementioned `pubsub` architecture to a more generic form so that clients i.e. _{publishers, subscribers}_ can talk to **Pub/Sub Hub** over network i.e. TCP, QUIC.

What it gives us is, ability to publish messages to topics over network, where **Pub/Sub Hub** might sit somewhere else; subscribe to topics of interest & keep receiving messages as soon as they're published, over network.

> QUIC to be preferred choice of network I/O, due to benefits it brings on table.

## Architecture

![architecture](./sc/architecture.jpg)

**More coming soon**
