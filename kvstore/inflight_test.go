package kvstore

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
)

func TestProposalInflightRegister(t *testing.T) {
	t.Run("registers a buffered result channel", func(t *testing.T) {
		inflight := newProposalInflight(1)

		resultC, err := inflight.register("request-1")
		if err != nil {
			t.Fatalf("register() error = %v", err)
		}
		if resultC == nil {
			t.Fatal("register() returned a nil channel")
		}
		if cap(resultC) != 1 {
			t.Fatalf("result channel capacity = %d, want 1", cap(resultC))
		}
		if len(inflight.chs) != 1 {
			t.Fatalf("registered request count = %d, want 1", len(inflight.chs))
		}
	})

	t.Run("rejects a duplicate id without replacing its channel", func(t *testing.T) {
		inflight := newProposalInflight(2)
		original, err := inflight.register("request-1")
		if err != nil {
			t.Fatalf("first register() error = %v", err)
		}

		duplicate, err := inflight.register("request-1")
		if !errors.Is(err, errRequestAlreadyRegistered) {
			t.Fatalf("duplicate register() error = %v, want %v", err, errRequestAlreadyRegistered)
		}
		if duplicate != nil {
			t.Fatal("duplicate register() returned a non-nil channel")
		}
		if inflight.chs["request-1"] != original {
			t.Fatal("duplicate register() replaced the original channel")
		}
	})

	t.Run("enforces capacity without changing existing registrations", func(t *testing.T) {
		inflight := newProposalInflight(1)
		original, err := inflight.register("request-1")
		if err != nil {
			t.Fatalf("first register() error = %v", err)
		}

		resultC, err := inflight.register("request-2")
		if !errors.Is(err, errTooManyInflightRequests) {
			t.Fatalf("register() error = %v, want %v", err, errTooManyInflightRequests)
		}
		if resultC != nil {
			t.Fatal("register() above capacity returned a non-nil channel")
		}
		if len(inflight.chs) != 1 || inflight.chs["request-1"] != original {
			t.Fatal("register() above capacity changed existing registrations")
		}
	})
}

func TestProposalInflightComplete(t *testing.T) {
	inflight := newProposalInflight(1)
	resultC, err := inflight.register("request-1")
	if err != nil {
		t.Fatalf("register() error = %v", err)
	}

	want := &applyResult{err: errors.New("apply failed")}
	inflight.complete("request-1", want)

	select {
	case got := <-resultC:
		if got != want {
			t.Fatalf("completed result = %p, want %p", got, want)
		}
	default:
		t.Fatal("complete() did not deliver the result")
	}
	if len(inflight.chs) != 0 {
		t.Fatalf("registered request count after complete() = %d, want 0", len(inflight.chs))
	}

	// Completion frees both the identifier and the capacity slot.
	if _, err := inflight.register("request-1"); err != nil {
		t.Fatalf("register() after complete() error = %v", err)
	}

	// A late result for an unknown request is intentionally discarded.
	inflight.complete("unknown", &applyResult{})
	if len(inflight.chs) != 1 {
		t.Fatalf("unknown complete() changed request count to %d, want 1", len(inflight.chs))
	}
}

func TestProposalInflightRemoveOnlyMatchingRegistration(t *testing.T) {
	inflight := newProposalInflight(1)
	oldC, err := inflight.register("request-1")
	if err != nil {
		t.Fatalf("first register() error = %v", err)
	}

	wrongC := make(chan *applyResult, 1)
	inflight.remove("request-1", wrongC)
	if len(inflight.chs) != 1 {
		t.Fatal("remove() deleted a registration with a different channel")
	}

	inflight.remove("request-1", oldC)
	if len(inflight.chs) != 0 {
		t.Fatal("remove() kept the matching registration")
	}

	newC, err := inflight.register("request-1")
	if err != nil {
		t.Fatalf("second register() error = %v", err)
	}
	inflight.remove("request-1", oldC)
	if inflight.chs["request-1"] != newC {
		t.Fatal("stale remove() deleted a newer registration with the same id")
	}
}

func TestReadIndexInflightRegister(t *testing.T) {
	t.Run("registers a pending read index", func(t *testing.T) {
		inflight := newReadIndexInflight(1)

		signalC, err := inflight.register("request-1")
		if err != nil {
			t.Fatalf("register() error = %v", err)
		}
		if signalC == nil {
			t.Fatal("register() returned a nil channel")
		}
		if cap(signalC) != 1 {
			t.Fatalf("signal channel capacity = %d, want 1", cap(signalC))
		}
		if got := inflight.chs["request-1"].index; got != math.MaxUint64 {
			t.Fatalf("initial read index = %d, want %d", got, uint64(math.MaxUint64))
		}
		if inflight.appliedIndex != 0 {
			t.Fatalf("initial applied index = %d, want 0", inflight.appliedIndex)
		}
		assertReadIndexInflightInvariant(t, inflight)
	})

	t.Run("rejects duplicate ids and excess capacity", func(t *testing.T) {
		inflight := newReadIndexInflight(2)
		original, err := inflight.register("request-1")
		if err != nil {
			t.Fatalf("first register() error = %v", err)
		}

		duplicate, err := inflight.register("request-1")
		if !errors.Is(err, errRequestAlreadyRegistered) {
			t.Fatalf("duplicate register() error = %v, want %v", err, errRequestAlreadyRegistered)
		}
		if duplicate != nil || inflight.chs["request-1"].ch != original {
			t.Fatal("duplicate register() changed the original registration")
		}

		if _, err := inflight.register("request-2"); err != nil {
			t.Fatalf("second unique register() error = %v", err)
		}
		excess, err := inflight.register("request-3")
		if !errors.Is(err, errTooManyInflightRequests) {
			t.Fatalf("register() above capacity error = %v, want %v", err, errTooManyInflightRequests)
		}
		if excess != nil {
			t.Fatal("register() above capacity returned a non-nil channel")
		}
		assertReadIndexInflightInvariant(t, inflight)
	})
}

func TestReadIndexInflightSignalsRegardlessOfEventOrder(t *testing.T) {
	t.Run("read indexes arrive before applied progress", func(t *testing.T) {
		inflight := newReadIndexInflight(4)
		lowC := mustRegisterReadIndex(t, inflight, "low")
		equalAC := mustRegisterReadIndex(t, inflight, "equal-a")
		equalBC := mustRegisterReadIndex(t, inflight, "equal-b")
		pendingC := mustRegisterReadIndex(t, inflight, "pending")

		// Update in an order different from the target indexes to exercise heap.Fix.
		if !inflight.updateReadIndex("equal-a", 4) {
			t.Fatal("updateReadIndex() did not find equal-a")
		}
		if !inflight.updateReadIndex("low", 2) {
			t.Fatal("updateReadIndex() did not find low")
		}
		if !inflight.updateReadIndex("equal-b", 4) {
			t.Fatal("updateReadIndex() did not find equal-b")
		}
		if inflight.updateReadIndex("unknown", 1) {
			t.Fatal("updateReadIndex() found an unknown id")
		}
		assertReadIndexInflightInvariant(t, inflight)

		inflight.advanceApplied(1)
		assertNotSignaled(t, lowC, "target index above applied index")
		assertNotSignaled(t, equalAC, "target index above applied index")
		assertNotSignaled(t, equalBC, "target index above applied index")
		assertNotSignaled(t, pendingC, "read index is still pending")

		inflight.advanceApplied(2)
		assertSignaled(t, lowC, "target index equal to applied index")
		assertNotSignaled(t, equalAC, "target index above applied index")
		assertNotSignaled(t, equalBC, "target index above applied index")
		assertNotSignaled(t, pendingC, "read index is still pending")
		assertReadIndexInflightInvariant(t, inflight)

		inflight.advanceApplied(4)
		assertSignaled(t, equalAC, "first target at applied index")
		assertSignaled(t, equalBC, "second target at applied index")
		assertNotSignaled(t, pendingC, "read index is still pending")
		if inflight.appliedIndex != 4 {
			t.Fatalf("applied index = %d, want 4", inflight.appliedIndex)
		}
		assertReadIndexInflightInvariant(t, inflight)
	})

	t.Run("applied progress arrives before read indexes", func(t *testing.T) {
		inflight := newReadIndexInflight(2)
		inflight.advanceApplied(5)
		readyC := mustRegisterReadIndex(t, inflight, "ready")
		futureC := mustRegisterReadIndex(t, inflight, "future")

		if !inflight.updateReadIndex("future", 6) {
			t.Fatal("updateReadIndex() did not find future")
		}
		assertNotSignaled(t, futureC, "target index above the stored applied index")

		if !inflight.updateReadIndex("ready", 4) {
			t.Fatal("updateReadIndex() did not find ready")
		}
		assertSignaled(t, readyC, "stored applied index already satisfies the target")
		assertNotSignaled(t, futureC, "future target remains pending")

		inflight.advanceApplied(6)
		assertSignaled(t, futureC, "applied progress reaches the future target")
		assertReadIndexInflightInvariant(t, inflight)
	})

	t.Run("applied progress never moves backwards", func(t *testing.T) {
		inflight := newReadIndexInflight(1)
		inflight.advanceApplied(5)
		inflight.advanceApplied(3)
		if inflight.appliedIndex != 5 {
			t.Fatalf("applied index regressed to %d, want 5", inflight.appliedIndex)
		}

		signalC := mustRegisterReadIndex(t, inflight, "request-1")
		if !inflight.updateReadIndex("request-1", 5) {
			t.Fatal("updateReadIndex() did not find request-1")
		}
		assertSignaled(t, signalC, "non-regressed applied index satisfies the target")
		assertReadIndexInflightInvariant(t, inflight)
	})
}

func TestReadIndexInflightRemoveOnlyMatchingRegistration(t *testing.T) {
	inflight := newReadIndexInflight(3)
	oldC := mustRegisterReadIndex(t, inflight, "request-1")
	mustRegisterReadIndex(t, inflight, "request-2")
	if !inflight.updateReadIndex("request-1", 1) {
		t.Fatal("updateReadIndex() did not find request-1")
	}
	if !inflight.updateReadIndex("request-2", 2) {
		t.Fatal("updateReadIndex() did not find request-2")
	}

	wrongC := make(chan struct{}, 1)
	inflight.remove("request-1", wrongC)
	if _, ok := inflight.chs["request-1"]; !ok {
		t.Fatal("remove() deleted a registration with a different channel")
	}

	inflight.remove("request-1", oldC)
	if _, ok := inflight.chs["request-1"]; ok {
		t.Fatal("remove() kept the matching registration")
	}
	assertReadIndexInflightInvariant(t, inflight)

	newC := mustRegisterReadIndex(t, inflight, "request-1")
	if !inflight.updateReadIndex("request-1", 1) {
		t.Fatal("updateReadIndex() did not find the new request-1")
	}
	inflight.remove("request-1", oldC)
	if inflight.chs["request-1"].ch != newC {
		t.Fatal("stale remove() deleted a newer registration with the same id")
	}

	inflight.advanceApplied(1)
	assertSignaled(t, newC, "new registration at applied index")
	assertReadIndexInflightInvariant(t, inflight)
}

func TestProposalInflightConcurrentComplete(t *testing.T) {
	const requestCount = 64
	inflight := newProposalInflight(requestCount)
	channels := make([]<-chan *applyResult, requestCount)
	results := make([]applyResult, requestCount)

	for i := range requestCount {
		id := requestID(i)
		resultC, err := inflight.register(id)
		if err != nil {
			t.Fatalf("register(%q) error = %v", id, err)
		}
		channels[i] = resultC
	}

	var wg sync.WaitGroup
	for i := range requestCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inflight.complete(requestID(i), &results[i])
		}()
	}
	wg.Wait()

	for i, resultC := range channels {
		select {
		case got := <-resultC:
			if got != &results[i] {
				t.Errorf("result for request %d = %p, want %p", i, got, &results[i])
			}
		default:
			t.Errorf("request %d was not completed", i)
		}
	}
	if len(inflight.chs) != 0 {
		t.Fatalf("registered request count after concurrent completion = %d, want 0", len(inflight.chs))
	}
}

func TestReadIndexInflightConcurrentUpdateAndAdvance(t *testing.T) {
	const requestCount = 64
	inflight := newReadIndexInflight(requestCount)
	channels := make([]<-chan struct{}, requestCount)
	for i := range requestCount {
		channels[i] = mustRegisterReadIndex(t, inflight, requestID(i))
	}

	// ReadState updates and local application progress are produced by different
	// goroutines in kvstore, so exercise both possible arrival orders concurrently.
	var wg sync.WaitGroup
	for i := range requestCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !inflight.updateReadIndex(requestID(i), uint64(i+1)) {
				t.Errorf("updateReadIndex(%q) did not find the request", requestID(i))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		inflight.advanceApplied(requestCount)
	}()
	wg.Wait()

	for i, signalC := range channels {
		assertSignaled(t, signalC, "concurrent request "+requestID(i))
	}
	if inflight.appliedIndex != requestCount {
		t.Fatalf("applied index = %d, want %d", inflight.appliedIndex, requestCount)
	}
	assertReadIndexInflightInvariant(t, inflight)
}

func mustRegisterReadIndex(t *testing.T, inflight *readIndexInflight, id string) <-chan struct{} {
	t.Helper()
	signalC, err := inflight.register(id)
	if err != nil {
		t.Fatalf("register(%q) error = %v", id, err)
	}
	return signalC
}

func assertSignaled(t *testing.T, signalC <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-signalC:
	default:
		t.Fatalf("signal not delivered: %s", reason)
	}
}

func assertNotSignaled(t *testing.T, signalC <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-signalC:
		t.Fatalf("unexpected signal: %s", reason)
	default:
	}
}

func assertReadIndexInflightInvariant(t *testing.T, inflight *readIndexInflight) {
	t.Helper()
	if inflight.heap.Len() != len(inflight.chs) {
		t.Fatalf("heap length = %d, map length = %d", inflight.heap.Len(), len(inflight.chs))
	}
	if len(inflight.heap.positions) != len(inflight.heap.ids) {
		t.Fatalf("position count = %d, heap id count = %d", len(inflight.heap.positions), len(inflight.heap.ids))
	}
	for i, id := range inflight.heap.ids {
		if got := inflight.heap.positions[id]; got != i {
			t.Fatalf("position[%q] = %d, want %d", id, got, i)
		}
		if inflight.heap.chs[id] != inflight.chs[id] {
			t.Fatalf("heap and inflight maps disagree for id %q", id)
		}
		if i > 0 {
			parent := (i - 1) / 2
			if inflight.heap.Less(i, parent) {
				t.Fatalf("heap child %q at %d is less than parent %q at %d", id, i, inflight.heap.ids[parent], parent)
			}
		}
	}
}

func requestID(i int) string {
	return "request-" + strconv.Itoa(i)
}
