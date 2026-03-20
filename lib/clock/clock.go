package clock

import (
	"sync"
	"time"
)

// Clock abstracts wall-clock access and timer creation so core control-plane
// loops can be driven deterministically in tests and simulations.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
	Sleep(d time.Duration)
}

// Timer abstracts time.Timer.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Ticker abstracts time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock delegates to the standard library time package.
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (RealClock) NewTimer(d time.Duration) Timer {
	return &realTimer{timer: time.NewTimer(d)}
}

func (RealClock) NewTicker(d time.Duration) Ticker {
	return &realTicker{ticker: time.NewTicker(d)}
}

func (RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

type realTimer struct {
	timer *time.Timer
}

func (t *realTimer) C() <-chan time.Time {
	return t.timer.C
}

func (t *realTimer) Stop() bool {
	return t.timer.Stop()
}

func (t *realTimer) Reset(d time.Duration) bool {
	return t.timer.Reset(d)
}

type realTicker struct {
	ticker *time.Ticker
}

func (t *realTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *realTicker) Stop() {
	t.ticker.Stop()
}

// MockClock advances only when instructed by tests.
type MockClock struct {
	mu      sync.Mutex
	current time.Time
	timers  map[*mockTimer]struct{}
	tickers map[*mockTicker]struct{}
}

func NewMockClock(start time.Time) *MockClock {
	return &MockClock{
		current: start,
		timers:  make(map[*mockTimer]struct{}),
		tickers: make(map[*mockTicker]struct{}),
	}
}

func (m *MockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

func (m *MockClock) After(d time.Duration) <-chan time.Time {
	return m.NewTimer(d).C()
}

func (m *MockClock) NewTimer(d time.Duration) Timer {
	m.mu.Lock()
	defer m.mu.Unlock()

	timer := &mockTimer{
		clock:    m,
		c:        make(chan time.Time, 1),
		deadline: m.current.Add(d),
		active:   true,
	}
	m.timers[timer] = struct{}{}
	return timer
}

func (m *MockClock) NewTicker(d time.Duration) Ticker {
	m.mu.Lock()
	defer m.mu.Unlock()

	ticker := &mockTicker{
		clock:    m,
		c:        make(chan time.Time, 1),
		interval: d,
		next:     m.current.Add(d),
		active:   true,
	}
	m.tickers[ticker] = struct{}{}
	return ticker
}

func (m *MockClock) Sleep(d time.Duration) {
	m.Advance(d)
}

func (m *MockClock) Advance(d time.Duration) {
	m.mu.Lock()
	m.current = m.current.Add(d)
	m.fireDueLocked()
	m.mu.Unlock()
}

func (m *MockClock) Set(t time.Time) {
	m.mu.Lock()
	m.current = t
	m.fireDueLocked()
	m.mu.Unlock()
}

func (m *MockClock) fireDueLocked() {
	for timer := range m.timers {
		if !timer.active || timer.deadline.After(m.current) {
			continue
		}
		timer.active = false
		select {
		case timer.c <- timer.deadline:
		default:
		}
	}

	for ticker := range m.tickers {
		if !ticker.active {
			continue
		}
		for !ticker.next.After(m.current) {
			next := ticker.next
			ticker.next = ticker.next.Add(ticker.interval)
			select {
			case ticker.c <- next:
			default:
			}
		}
	}
}

type mockTimer struct {
	clock    *MockClock
	c        chan time.Time
	deadline time.Time
	active   bool
}

func (t *mockTimer) C() <-chan time.Time {
	return t.c
}

func (t *mockTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = false
	return wasActive
}

func (t *mockTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.deadline = t.clock.current.Add(d)
	t.active = true
	t.clock.fireDueLocked()
	return wasActive
}

type mockTicker struct {
	clock    *MockClock
	c        chan time.Time
	interval time.Duration
	next     time.Time
	active   bool
}

func (t *mockTicker) C() <-chan time.Time {
	return t.c
}

func (t *mockTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.active = false
}
