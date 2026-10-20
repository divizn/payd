package payment

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidAmount   = errors.New("amount must be positive")
	ErrInvalidCurrency = errors.New("unsupported currency")
)

// fixed currency list, swap for golang.org/x/text/currency if every ISO 4217 code is needed
var currencies = map[string]bool{
	"GBP": true, "EUR": true, "USD": true, "JPY": true, "CHF": true,
	"SEK": true, "NOK": true, "DKK": true, "PLN": true, "CAD": true, "AUD": true,
}

type Money struct {
	Amount   int64
	Currency string
}

func NewMoney(amount int64, currency string) (Money, error) {
	if amount <= 0 {
		return Money{}, ErrInvalidAmount
	}
	if !currencies[currency] {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, currency)
	}
	return Money{Amount: amount, Currency: currency}, nil
}
