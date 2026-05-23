package runtimechannel

import (
	"sync"
	"time"
)

type replayEntry struct {
	key       string
	expiresAt time.Time
}

// ReplayCache is a small in-memory nonce cache. It is intentionally scoped to
// one control-panel-service process; multi-instance sharing can be introduced
// when runtime streams are sharded/shared across instances.
type ReplayCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	maxSize int
	seen    map[string]time.Time
	order   []replayEntry
}

func NewReplayCache(ttl time.Duration, maxSize int) *ReplayCache {
	if ttl <= 0 {
		ttl = replayTTL
	}
	if maxSize <= 0 {
		maxSize = 4096
	}
	return &ReplayCache{
		ttl:     ttl,
		maxSize: maxSize,
		seen:    map[string]time.Time{},
	}
}

// CheckAndStore returns false when (keyID, nonce) is still in the cache.
func (c *ReplayCache) CheckAndStore(keyID, nonce string, now time.Time) bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pruneLocked(now)
	k := keyID + "\x00" + nonce
	if exp, ok := c.seen[k]; ok && now.Before(exp) {
		return false
	}
	exp := now.Add(c.ttl)
	c.seen[k] = exp
	c.order = append(c.order, replayEntry{key: k, expiresAt: exp})
	for len(c.seen) > c.maxSize && len(c.order) > 0 {
		old := c.order[0]
		c.order = c.order[1:]
		if c.seen[old.key] == old.expiresAt {
			delete(c.seen, old.key)
		}
	}
	return true
}

func (c *ReplayCache) pruneLocked(now time.Time) {
	n := 0
	for _, e := range c.order {
		if now.Before(e.expiresAt) {
			c.order[n] = e
			n++
			continue
		}
		if c.seen[e.key] == e.expiresAt {
			delete(c.seen, e.key)
		}
	}
	c.order = c.order[:n]
}
