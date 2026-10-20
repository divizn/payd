-- +goose Up
CREATE TYPE payment_status AS ENUM (
    'created', 'authorised', 'captured', 'partially_refunded', 'refunded', 'failed', 'voided'
);

CREATE TABLE payments (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id     text NOT NULL,
    amount          bigint NOT NULL CHECK (amount > 0),
    currency        char(3) NOT NULL,
    status          payment_status NOT NULL DEFAULT 'created',
    captured_amount bigint NOT NULL DEFAULT 0 CHECK (captured_amount BETWEEN 0 AND amount),
    refunded_amount bigint NOT NULL DEFAULT 0 CHECK (refunded_amount BETWEEN 0 AND captured_amount),
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX payments_merchant_created_idx ON payments (merchant_id, created_at DESC);

-- +goose Down
DROP TABLE payments;
DROP TYPE payment_status;
