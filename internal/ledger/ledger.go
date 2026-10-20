package ledger

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Acquirer = "acquirer"

func MerchantAccount(merchantID string) string { return "merchant:" + merchantID }

func Post(ctx context.Context, tx pgx.Tx, paymentID, from, to string, amount int64, currency string) error {
	_, err := tx.Exec(ctx, `INSERT INTO ledger_entries (transaction_id, payment_id, account, amount, currency) VALUES ($1, $2, $3, $5, $6), ($1, $2, $4, -$5, $6)`,
		uuid.New(), paymentID, to, from, amount, currency)
	return err
}

// Check reports unbalanced transactions and payments whose merchant balance disagrees with captured minus refunded
func Check(ctx context.Context, db *pgxpool.Pool) error {
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM ledger_entries GROUP BY transaction_id, currency HAVING sum(amount) <> 0) t`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("%d unbalanced ledger transactions", n)
	}
	err := db.QueryRow(ctx, `
		SELECT count(*) FROM payments p
		LEFT JOIN (
			SELECT payment_id, account, sum(amount) AS balance FROM ledger_entries GROUP BY payment_id, account
		) l ON l.payment_id = p.id AND l.account = 'merchant:' || p.merchant_id
		WHERE coalesce(l.balance, 0) <> p.captured_amount - p.refunded_amount`).Scan(&n)
	if err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("%d payments disagree with the ledger", n)
	}
	return nil
}
