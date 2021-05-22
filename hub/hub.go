package hub

import (
	"context"
	"net"
	"sync"

	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/xtaci/gaio"
)

// Hub - Abstraction between message publishers & subscribers,
// works as a multiplexer ( or router )
type Hub struct {
	watcher                    *gaio.Watcher
	pendingPublishers          map[net.Conn]bool
	pendingNewSubscribers      map[net.Conn]bool
	pendingExistingSubscribers map[net.Conn]bool
	pendingUnsubscribers       map[net.Conn]bool
	enqueuedRead               map[net.Conn]*enqueuedRead
	connectedSubscribers       map[net.Conn]uint64
	index                      uint64
	subLock                    *sync.RWMutex
	subscribers                map[string]map[uint64]net.Conn
	revLock                    *sync.RWMutex
	revSubscribers             map[uint64]map[string]bool
	queueLock                  *sync.RWMutex
	pendingQueue               []*ops.Msg
	ping                       chan struct{}
	evict                      chan uint64
}

type enqueuedRead struct {
	yes bool
	buf []byte
}

// New - Creates a new instance of hub, ready to be used
func New(ctx context.Context, addr string, cap uint64) (*Hub, error) {
	watcher, err := gaio.NewWatcher()
	if err != nil {
		return nil, err
	}

	hub := Hub{
		watcher:                    watcher,
		pendingPublishers:          make(map[net.Conn]bool),
		pendingNewSubscribers:      make(map[net.Conn]bool),
		pendingExistingSubscribers: make(map[net.Conn]bool),
		pendingUnsubscribers:       make(map[net.Conn]bool),
		enqueuedRead:               make(map[net.Conn]*enqueuedRead),
		connectedSubscribers:       make(map[net.Conn]uint64),
		index:                      0,
		subLock:                    &sync.RWMutex{},
		subscribers:                make(map[string]map[uint64]net.Conn),
		revLock:                    &sync.RWMutex{},
		revSubscribers:             make(map[uint64]map[string]bool),
		queueLock:                  &sync.RWMutex{},
		pendingQueue:               make([]*ops.Msg, 0, cap),
		ping:                       make(chan struct{}, cap),
		evict:                      make(chan uint64, cap),
	}

	var (
		runListener = make(chan bool)
		runWatcher  = make(chan struct{})
		runProc     = make(chan struct{})
		runEvict    = make(chan struct{})
	)

	go hub.listen(ctx, addr, runListener)
	if !<-runListener {
		return nil, ops.ErrListenerNotStarted
	}

	go hub.watch(ctx, runWatcher)
	go hub.process(ctx, runProc)
	go hub.evictSubscribers(ctx, runEvict)
	<-runWatcher
	<-runProc
	<-runEvict

	return &hub, nil
}
