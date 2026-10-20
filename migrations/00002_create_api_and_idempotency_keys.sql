-- +goose Up
CREATE TABLE api_keys (
    key_hash    bytea PRIMARY KEY,
    merchant_id text NOT NULL
);

CREATE TABLE idempotency_keys (
    merchant_id     text NOT NULL,
    key             text NOT NULL,
    request_hash    bytea NOT NULL,
    response_status int NOT NULL,
    response_body   bytea NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, key)
);

-- +goose Down
DROP TABLE idempotency_keys;
DROP TABLE api_keys;
