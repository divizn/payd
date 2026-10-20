-- +goose Up
CREATE TABLE ledger_entries (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id uuid NOT NULL,
    payment_id     uuid NOT NULL REFERENCES payments (id),
    account        text NOT NULL,
    amount         bigint NOT NULL CHECK (amount <> 0),
    currency       char(3) NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ledger_entries_payment_idx ON ledger_entries (payment_id);

CREATE TABLE outbox_events (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id   uuid NOT NULL REFERENCES payments (id),
    type         text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_events_unpublished_idx ON outbox_events (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox_events;
DROP TABLE ledger_entries;
