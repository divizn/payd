package payment

import (
	"errors"
	"testing"
)

var allStatuses = []Status{
	StatusCreated, StatusAuthorised, StatusCaptured, StatusPartiallyRefunded,
	StatusRefunded, StatusFailed, StatusVoided,
}

func TestNewMoneyRejectsBadInput(t *testing.T) {
	for _, tc := range []struct {
		amount   int64
		currency string
		want     error
	}{
		{0, "GBP", ErrInvalidAmount},
		{-1, "GBP", ErrInvalidAmount},
		{100, "gbp", ErrInvalidCurrency},
		{100, "XYZ", ErrInvalidCurrency},
	} {
		if _, err := NewMoney(tc.amount, tc.currency); !errors.Is(err, tc.want) {
			t.Errorf("NewMoney(%d, %q) = %v, want %v", tc.amount, tc.currency, err, tc.want)
		}
	}
	if m, err := NewMoney(1, "JPY"); err != nil || m != (Money{1, "JPY"}) {
		t.Errorf("NewMoney(1, JPY) = %v, %v", m, err)
	}
}

func TestOnlyLegalTransitionsSucceed(t *testing.T) {
	ops := map[string]struct {
		legal map[Status]Status
		apply func(*Payment) error
	}{
		"authorise": {map[Status]Status{StatusCreated: StatusAuthorised}, (*Payment).Authorise},
		"fail":      {map[Status]Status{StatusCreated: StatusFailed}, (*Payment).Fail},
		"void":      {map[Status]Status{StatusAuthorised: StatusVoided}, (*Payment).Void},
		"capture":   {map[Status]Status{StatusAuthorised: StatusCaptured}, func(p *Payment) error { return p.Capture(10) }},
		"refund": {map[Status]Status{StatusCaptured: StatusPartiallyRefunded, StatusPartiallyRefunded: StatusPartiallyRefunded},
			func(p *Payment) error { return p.Refund(1) }},
	}
	for name, op := range ops {
		for _, s := range allStatuses {
			p := Payment{Money: Money{100, "GBP"}, Status: s, CapturedAmount: 50, RefundedAmount: 10}
			before := p
			err := op.apply(&p)
			if want, ok := op.legal[s]; ok {
				if err != nil || p.Status != want {
					t.Errorf("%s from %s: status %s, err %v; want %s", name, s, p.Status, err, want)
				}
				continue
			}
			if !errors.Is(err, ErrIllegalTransition) {
				t.Errorf("%s from %s: err = %v, want ErrIllegalTransition", name, s, err)
			}
			if p != before {
				t.Errorf("%s from %s: payment changed on rejection: %+v", name, s, p)
			}
		}
	}
}

func TestCaptureBounds(t *testing.T) {
	for amount, want := range map[int64]error{0: ErrInvalidAmount, -5: ErrInvalidAmount, 101: ErrExceedsAmount} {
		p := Payment{Money: Money{100, "GBP"}, Status: StatusAuthorised}
		if err := p.Capture(amount); !errors.Is(err, want) {
			t.Errorf("Capture(%d) of 100 = %v, want %v", amount, err, want)
		}
		if p.Status != StatusAuthorised || p.CapturedAmount != 0 {
			t.Errorf("Capture(%d) changed payment on rejection: %+v", amount, p)
		}
	}
}

func TestRefundsCannotExceedPartialCapture(t *testing.T) {
	p := Payment{Money: Money{100, "GBP"}, Status: StatusAuthorised}
	if err := p.Capture(60); err != nil {
		t.Fatal(err)
	}
	if err := p.Refund(40); err != nil || p.Status != StatusPartiallyRefunded {
		t.Fatalf("Refund(40): err=%v status=%s", err, p.Status)
	}
	if err := p.Refund(21); !errors.Is(err, ErrExceedsAmount) || p.RefundedAmount != 40 {
		t.Fatalf("Refund(21) past captured 60: err=%v refunded=%d", err, p.RefundedAmount)
	}
	if err := p.Refund(20); err != nil || p.Status != StatusRefunded {
		t.Fatalf("Refund(20): err=%v status=%s", err, p.Status)
	}
	if err := p.Refund(1); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("refund after full refund: err=%v", err)
	}
}
