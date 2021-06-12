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
	addr                     string
	watchersLock             *sync.RWMutex
	watchers                 map[uint]*watcher
	watcherCount             uint
	connectedSubscribers     map[net.Conn]*subInfo
	connectedSubscribersLock *sync.RWMutex
	index                    uint64
	subLock                  *sync.RWMutex
	subscribers              map[string]map[uint64]net.Conn
	revLock                  *sync.RWMutex
	revSubscribers           map[uint64]map[string]bool
	queueLock                *sync.RWMutex
	pendingQueue             []*ops.Msg
	ping                     chan struct{}
	evict                    chan uint64
	Connected                chan string
	Disconnected             chan string
}

func (h *Hub) Addr() string {
	return h.addr
}

type watcher struct {
	eventLoop   *gaio.Watcher
	ongoingRead map[net.Conn]*readState
	lock        *sync.RWMutex
}

type readState struct {
	opcode       ops.OP
	envelopeRead bool
	buf          []byte
}

type subInfo struct {
	id        uint64
	watcherId uint
}

// New - Creates a new instance of hub, ready to be used
func New(ctx context.Context, addr string, cap uint64) (*Hub, error) {
	hub := Hub{
		watcherCount:             2,
		watchersLock:             &sync.RWMutex{},
		watchers:                 make(map[uint]*watcher),
		connectedSubscribers:     make(map[net.Conn]*subInfo),
		connectedSubscribersLock: &sync.RWMutex{},
		index:                    0,
		subLock:                  &sync.RWMutex{},
		subscribers:              make(map[string]map[uint64]net.Conn),
		revLock:                  &sync.RWMutex{},
		revSubscribers:           make(map[uint64]map[string]bool),
		queueLock:                &sync.RWMutex{},
		pendingQueue:             make([]*ops.Msg, 0, cap),
		ping:                     make(chan struct{}, cap),
		evict:                    make(chan uint64, cap),
		Connected:                make(chan string, 1),
		Disconnected:             make(chan string, 1),
	}

	var runWatcher = make(chan struct{}, hub.watcherCount)
	var i uint
	hub.watchersLock.Lock()
	for ; i < hub.watcherCount; i++ {
		w, err := gaio.NewWatcher()
		if err != nil {
			return nil, err
		}
		hub.watchers[i] = &watcher{
			eventLoop:   w,
			ongoingRead: make(map[net.Conn]*readState),
			lock:        &sync.RWMutex{},
		}
		func(id uint) {
			go hub.watch(ctx, id, runWatcher)
		}(i)
	}
	hub.watchersLock.Unlock()

	startedOff := 0
	for range runWatcher {
		startedOff++
		if startedOff >= int(hub.watcherCount) {
			break
		}
	}

	var (
		runListener  = make(chan bool)
		runEvictor   = make(chan struct{})
		runProcessor = make(chan struct{})
	)

	go hub.listen(ctx, addr, runListener)
	if !<-runListener {
		return nil, ops.ErrListenerNotStarted
	}
	go hub.process(ctx, runProcessor)
	go hub.evictSubscribers(ctx, runEvictor)
	<-runProcessor
	<-runEvictor

	return &hub, nil
}
