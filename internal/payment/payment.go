package payment

import (
	"errors"
	"fmt"
)

type Status string

const (
	StatusCreated           Status = "created"
	StatusAuthorised        Status = "authorised"
	StatusCaptured          Status = "captured"
	StatusPartiallyRefunded Status = "partially_refunded"
	StatusRefunded          Status = "refunded"
	StatusFailed            Status = "failed"
	StatusVoided            Status = "voided"
)

var (
	ErrIllegalTransition = errors.New("illegal transition")
	ErrExceedsAmount     = errors.New("amount exceeds what is available")
)

type Payment struct {
	ID             string
	MerchantID     string
	Money          Money
	Status         Status
	CapturedAmount int64
	RefundedAmount int64
}

func (p *Payment) illegal(op string) error {
	return fmt.Errorf("%w: %s from %s", ErrIllegalTransition, op, p.Status)
}

func (p *Payment) Authorise() error {
	if p.Status != StatusCreated {
		return p.illegal("authorise")
	}
	p.Status = StatusAuthorised
	return nil
}

func (p *Payment) Fail() error {
	if p.Status != StatusCreated {
		return p.illegal("fail")
	}
	p.Status = StatusFailed
	return nil
}

func (p *Payment) Void() error {
	if p.Status != StatusAuthorised {
		return p.illegal("void")
	}
	p.Status = StatusVoided
	return nil
}

// Capture takes some or all of the authorised amount; the remainder is released.
func (p *Payment) Capture(amount int64) error {
	if p.Status != StatusAuthorised {
		return p.illegal("capture")
	}
	if amount <= 0 {
		return ErrInvalidAmount
	}
	if amount > p.Money.Amount {
		return ErrExceedsAmount
	}
	p.CapturedAmount = amount
	p.Status = StatusCaptured
	return nil
}

func (p *Payment) Refund(amount int64) error {
	if p.Status != StatusCaptured && p.Status != StatusPartiallyRefunded {
		return p.illegal("refund")
	}
	if amount <= 0 {
		return ErrInvalidAmount
	}
	if amount > p.CapturedAmount-p.RefundedAmount {
		return ErrExceedsAmount
	}
	p.RefundedAmount += amount
	if p.RefundedAmount == p.CapturedAmount {
		p.Status = StatusRefunded
	} else {
		p.Status = StatusPartiallyRefunded
	}
	return nil
}
