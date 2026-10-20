package acquirer

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/divizn/payd/internal/payment"
)

var ErrUnavailable = errors.New("acquirer unavailable")

// Acquirer authorises card payments; ref makes repeated calls for one payment return the first decision
type Acquirer interface {
	Authorise(ctx context.Context, ref string, m payment.Money) (approved bool, err error)
}

type Policy struct {
	Attempts int
	Timeout  time.Duration
	Backoff  time.Duration
}

func Authorise(ctx context.Context, a Acquirer, p Policy, ref string, m payment.Money) (bool, error) {
	backoff := p.Backoff
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, p.Timeout)
		approved, err := a.Authorise(actx, ref, m)
		cancel()
		if err == nil {
			return approved, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if !errors.Is(err, ErrUnavailable) && !errors.Is(err, context.DeadlineExceeded) {
			return false, err
		}
		if attempt >= p.Attempts {
			return false, ErrUnavailable
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(backoff/2 + rand.N(backoff)):
		}
		backoff *= 2
	}
}

// Fake declines amounts ending in 51 and injects timeouts after deciding, like a lost reply
type Fake struct {
	Latency     time.Duration
	ErrorRate   float64
	TimeoutRate float64
	Calls       atomic.Int64

	mu sync.Mutex
	// ponytail: unbounded, fine for a fake
	decisions map[string]bool
}

func (f *Fake) Authorise(ctx context.Context, ref string, m payment.Money) (bool, error) {
	f.Calls.Add(1)
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-time.After(f.Latency):
	}
	r := rand.Float64()
	if r < f.ErrorRate {
		return false, ErrUnavailable
	}
	f.mu.Lock()
	if f.decisions == nil {
		f.decisions = map[string]bool{}
	}
	approved, ok := f.decisions[ref]
	if !ok {
		approved = m.Amount%100 != 51
		f.decisions[ref] = approved
	}
	f.mu.Unlock()
	if r < f.ErrorRate+f.TimeoutRate {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return approved, nil
}
