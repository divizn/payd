package acquirer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/divizn/payd/internal/payment"
)

type scripted struct {
	steps []func(ctx context.Context) (bool, error)
	refs  []string
}

func (s *scripted) Authorise(ctx context.Context, ref string, _ payment.Money) (bool, error) {
	s.refs = append(s.refs, ref)
	step := s.steps[min(len(s.refs), len(s.steps))-1]
	return step(ctx)
}

func unavailable(context.Context) (bool, error) { return false, ErrUnavailable }
func approve(context.Context) (bool, error)     { return true, nil }
func hang(ctx context.Context) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

var policy = Policy{Attempts: 3, Timeout: 50 * time.Millisecond, Backoff: time.Millisecond}

func TestRetriesTransientErrorsWithTheSameRef(t *testing.T) {
	s := &scripted{steps: []func(context.Context) (bool, error){unavailable, hang, approve}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	approved, err := Authorise(ctx, s, policy, "pay_1", payment.Money{Amount: 100, Currency: "GBP"})
	if err != nil || !approved {
		t.Fatalf("Authorise = %v, %v; want approved after an error and a timeout", approved, err)
	}
	if len(s.refs) != 3 || s.refs[0] != "pay_1" || s.refs[1] != "pay_1" || s.refs[2] != "pay_1" {
		t.Fatalf("acquirer saw refs %v, want pay_1 three times", s.refs)
	}
}

func TestGivesUpAfterTheLastAttempt(t *testing.T) {
	s := &scripted{steps: []func(context.Context) (bool, error){unavailable}}
	_, err := Authorise(context.Background(), s, policy, "pay_1", payment.Money{Amount: 100, Currency: "GBP"})
	if !errors.Is(err, ErrUnavailable) || len(s.refs) != policy.Attempts {
		t.Fatalf("err = %v after %d calls, want ErrUnavailable after %d", err, len(s.refs), policy.Attempts)
	}
}

func TestCancelStopsTheBackoff(t *testing.T) {
	s := &scripted{steps: []func(context.Context) (bool, error){unavailable}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Authorise(ctx, s, Policy{Attempts: 3, Timeout: time.Second, Backoff: 2 * time.Second}, "pay_1", payment.Money{Amount: 100, Currency: "GBP"})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("err = %v after %s, want the caller's deadline within a second", err, time.Since(start))
	}
}
