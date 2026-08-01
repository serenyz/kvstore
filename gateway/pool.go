package gateway

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"
)

type backendConfig struct {
	id      uint64
	address string
}

type bucket struct {
	second uint64
	count  uint64
}

const windowSeconds uint64 = 60

type window struct {
	buckets [windowSeconds]bucket
}

func (w *window) addScoreAt(now, score uint64) {
	index := now % windowSeconds
	current := &w.buckets[index]

	if current.second != now {
		current.second = now
		current.count = score
		return
	}

	current.count += score
}

func (w *window) totalAt(now uint64) uint64 {
	var total uint64
	var cutoff uint64

	if now > windowSeconds {
		cutoff = now - windowSeconds
	}

	for i := range w.buckets {
		b := &w.buckets[i]

		if b.second > cutoff && b.second <= now {
			total += b.count
		}
	}

	return total
}

type backendPool struct {
	backends  []*backend
	healthy   []bool
	revisions []uint64
	buckets   []*window
	mu        sync.Mutex
	closeSig  chan struct{}
	wg        *sync.WaitGroup
}

func newBackendPool(config []backendConfig) (*backendPool, error) {
	n := len(config)
	pool := &backendPool{
		backends:  make([]*backend, n),
		healthy:   make([]bool, n),
		revisions: make([]uint64, n),
		buckets:   make([]*window, n),
		closeSig:  make(chan struct{}),
		wg:        &sync.WaitGroup{},
	}

	for i, c := range config {
		b, err := newBackend(c.id, c.address)
		if err != nil {
			if err2 := pool.close(); err2 != nil {
				return nil, errors.Join(err, err2)
			}
			return nil, err
		}
		pool.backends[i] = b
		pool.buckets[i] = &window{}
	}

	go pool.heartBeat(time.Second)
	return pool, nil
}

func (p *backendPool) close() error {
	close(p.closeSig)

	var firstErr error
	for _, b := range p.backends {
		if b == nil {
			continue
		}
		if err := b.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.wg.Wait()
	return firstErr
}

func (p *backendPool) pickRevision(revision uint64) []int {
	c1, c2, c3 := make([]int, 0), make([]int, 0), make([]int, 0)

	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.revisions {
		healthy := p.healthy[i]
		r := p.revisions[i]
		if !healthy {
			c3 = append(c3, i)
		} else if r < revision {
			c2 = append(c2, i)
		} else {
			c1 = append(c1, i)
		}
	}

	now := uint64(time.Now().Unix())
	cmp := func(i, j int) bool { return p.buckets[i].totalAt(now) < p.buckets[j].totalAt(now) }
	sort.Slice(c1, cmp)
	sort.Slice(c2, cmp)
	sort.Slice(c3, cmp)

	c1 = append(append(c1, c2...))
	for i := range c1 {
		p.buckets[c1[i]].addScoreAt(now, uint64(len(c1)-i)*uint64(len(c1)-i))
	}
	return append(c1, c3...)
}

func (p *backendPool) pick() []int {
	return p.pickRevision(math.MaxUint64)
}

func (p *backendPool) updateState(i int, healthy bool, revision uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.healthy[i] = healthy

	if i < 0 || i >= len(p.backends) {
		return
	}

	if healthy {
		p.revisions[i] = max(p.revisions[i], revision)
	}
}

func (p *backendPool) heartBeat(interval time.Duration) {
	for i, b := range p.backends {
		p.wg.Add(1)
		go func(i int, b *backend) {
			defer p.wg.Done()
			h := b.checkHealth(context.Background())
			p.mu.Lock()
			p.healthy[i] = h
			p.mu.Unlock()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					h = b.checkHealth(context.TODO())
					p.mu.Lock()
					p.healthy[i] = h
					p.mu.Unlock()
				case <-p.closeSig:
					_ = b.close()
					return
				}
			}
		}(i, b)
	}
}
