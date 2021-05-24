package hub

import "context"

func (h *Hub) evictSubscribers(ctx context.Context, running chan struct{}) {
	close(running)

	for {
		select {
		case <-ctx.Done():
			return

		case id := <-h.evict:
			h.revLock.Lock()
			revSubs, ok := h.revSubscribers[id]
			if ok {
				for topic := range revSubs {
					h.subLock.Lock()
					subs, ok := h.subscribers[topic]
					if ok {
						delete(subs, id)
						if len(subs) == 0 {
							delete(h.subscribers, topic)
						}
					}
					h.subLock.Unlock()
				}

				delete(h.revSubscribers, id)
			}
			h.revLock.Unlock()
		}
	}
}
