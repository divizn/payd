# payd

A payments service in Go: a JSON API to create, authorise, capture, refund and void card payments, backed by PostgreSQL alone. Every write takes a Stripe-style `Idempotency-Key`, every money movement is a balanced pair of ledger entries, and every state change writes an outbox event in the same transaction. Authorisation goes to a fake card-network acquirer that can be made slow, flaky or silent. Plain SQL through pgx, migrations with Goose, metrics with the Prometheus client.

## Layout

- `internal/payment`: the money type (integer minor units plus an ISO 4217 code) and the payment state machine, no I/O
- `internal/server`: HTTP handlers, API key auth, the idempotency layer, metrics
- `internal/ledger`: double-entry postings and the consistency check
- `internal/outbox`: enqueueing events and the relay that publishes them
- `internal/acquirer`: the acquirer interface, the retry policy and the fake
- `migrations`: Goose SQL migrations, embedded and run at startup
- `cmd/payd`: the server; `cmd/loadgen`: the load generator behind the benchmarks
- `mutations`, `scripts/mutate.sh`: reintroduced bugs and the script that checks the tests catch them

## How a write flows

```
POST /payments/{id}/capture, Idempotency-Key: k
BEGIN
  pg_try_advisory_xact_lock(merchant, k)      busy → 409, key in flight
  stored response for (merchant, k)?          same body → replay it, other body → 422
  SELECT payment FOR UPDATE
  state machine: authorised → captured        illegal → stored 409, no change
  UPDATE payment, INSERT ledger pair, INSERT outbox event
  INSERT idempotency_keys (k, request hash, response)
COMMIT → response
relay: SELECT unpublished FOR UPDATE SKIP LOCKED → publish → mark published → COMMIT
```

The effect, its ledger entries, its event and the stored response commit together or not at all. Authorise calls the acquirer at the state machine step, inside the same transaction.

## Guarantees

Each guarantee is followed by the test that checks it. Rates for reintroduced bugs are in Tests.

- Money is `int64` minor units with a currency from a fixed list, never a float. Amounts must be positive, a capture can't exceed the authorised amount and refunds can't exceed what was captured. Postgres `CHECK` constraints enforce the same bounds as a backstop (`TestSchemaRejectsImpossibleAmounts` writes past them directly and expects a check violation). Unit tests in `internal/payment` try every operation from every state.
- Illegal transitions (capture a voided payment, refund before capture, authorise twice) are rejected with 409 and change nothing: no row update, no ledger entry, no event. `TestOnlyLegalTransitionsSucceed` checks every (operation, state) pair in memory: each legal one reaches the right state and each rejection leaves the payment untouched. Through the API, the model test checks the status code and payment fields of every illegal operation it sends, and `TestLedgerAndOutboxFollowEachCommittedChange` checks that a rejected capture writes no event or ledger entry.
- One idempotency key, one effect. The same key with the same body returns the stored response byte for byte; with a different body, 422; while the first request is still running, 409 (retry later); after 24 hours the key is free again. Keys are scoped per merchant, and a request hash covers method, path and body; payment ids in paths must be the canonical lowercase form, so one payment has one path. `TestConcurrentDuplicateCreateChargesOnce`: 50 goroutines send the same create with one key, exactly one payment exists afterwards and all 50 callers get the identical 201 body (callers retry on the 409, as the API asks). Its capture twin checks the same for a capture, plus that only one ledger pair was written. `TestKeyInFlightReturns409` holds a key's lock from another transaction and expects 409; `TestKeyIsScopedToThePath` reuses a capture's key on a refund and expects 422; `TestKeysAreScopedToTheMerchant` sends the same key and body as two merchants and expects two payments.
- No double capture or over-refund under concurrency, whatever the keys. The payment row is locked `FOR UPDATE` for the whole request. 50 captures with 50 different keys: exactly one succeeds, 49 get 409 `illegal_transition`. 50 refunds of 30 against a capture of 100: exactly three succeed, 47 get 422 `invalid_amount`, and 90 is refunded. Both end with the ledger check.
- The ledger balances. Capture posts acquirer to merchant, refund posts merchant to acquirer, both in the payment's transaction. `ledger.Check` verifies that every ledger transaction sums to zero and that each payment's merchant balance equals captured minus refunded. The concurrency tests, the ledger and outbox test, the model test and the crash test end with it.
- Exactly one outbox event per committed state change (`payment.created`, `payment.authorised`, `payment.failed`, `payment.captured`, `payment.partially_refunded`, `payment.refunded`, `payment.voided`), and each event is published at least once. The relay publishes then marks the batch, so a crash in between republishes it; consumers dedupe by the event `id`. Checked by the crash test below.
- Crashes leave nothing half applied. `TestCrashKeepsLedgerAndOutboxConsistent` builds the real binary, runs 16 clients against it, `SIGKILL`s it at random points and restarts it, with the acquirer injecting errors and lost replies at the same time. Afterwards the ledger check passes, every payment has as many outbox events as committed 2xx responses and its last event matches its status, and every event in the outbox was published at least once.
- One authorisation request per payment reaches the acquirer at a time, and every attempt carries the payment id as its reference. `TestConcurrentAuthorisesCallTheAcquirerOnce`: 50 authorises with different keys, one acquirer call, 49 get 409. `TestRetriesTransientErrorsWithTheSameRef` checks that retries reuse the reference. A real acquirer would use the reference to return its first decision to a retry; the fake decides by amount alone, so that part is assumed, not tested.
- An acquirer outage is safe to retry. Each attempt has a 500ms timeout, and transient errors are retried for up to 3 attempts in all, with jittered backoff that stops when the request is cancelled (`TestCancelStopsTheBackoff`). If all fail, the request returns 503 and rolls back, so the payment stays `created` and the key stays unused; the client retries with the same key. `TestAcquirerOutageLeavesTheKeyRetryable`.
- Merchants are isolated: another merchant's payment is a 404 for reads and writes, and lists are per merchant.

## Running it

```sh
docker compose up -d --build     # payd on 8080, Prometheus on 9090
docker compose exec postgres psql -U payd -d payd -c "INSERT INTO api_keys VALUES (sha256('sk_test'), 'm_1')"
curl -s localhost:8080/payments -H 'Authorization: Bearer sk_test' -H 'Idempotency-Key: k1' -d '{"amount":1000,"currency":"GBP"}'
docker compose down -v
```

Environment: `DATABASE_URL` (pgx DSN; `pool_max_conns` sets the pool, Compose uses 32), `ADDR` (`:8080`), `ACQUIRER_LATENCY` (a Go duration), `ACQUIRER_ERROR_RATE` and `ACQUIRER_TIMEOUT_RATE` (0 to 1). The fake acquirer declines any amount ending in 51 minor units (1051 is declined, 1050 is approved). A timeout is injected after the fake has made its decision, like a network that processed the request and lost the reply.

## API

All routes need `Authorization: Bearer <api key>`; every `POST` needs `Idempotency-Key` (1 to 255 bytes of UTF-8). Bodies are at most 1 MiB, one JSON object with no unknown fields; `"amount": null` is rejected, leave the field out for the default. Errors are `{"error":{"code":"...","message":"..."}}`; `TestMalformedRequestsAreRejected` covers each validation error.

- `POST /payments` `{"amount":1000,"currency":"GBP"}`: 201 with the payment, status `created`
- `POST /payments/{id}/authorise`: 200 with status `authorised`, or `failed` on a decline; 503 `acquirer_unavailable` if the acquirer can't be reached
- `POST /payments/{id}/capture` `{"amount":600}` (optional, defaults to the full amount): the rest of the authorisation is released
- `POST /payments/{id}/refund` `{"amount":100}` (optional, defaults to what's left)
- `POST /payments/{id}/void`: only from `authorised`
- `GET /payments/{id}`, `GET /payments?limit=20` (newest first, at most 100)

409 `illegal_transition` for an operation the state forbids, 422 `invalid_amount` or `invalid_money` for bad amounts, 409 `idempotency_key_in_use` and 422 `idempotency_key_reused` as above. Rejections are stored against their key like successes, so a replay returns the same rejection.

Metrics are at `/metrics`, without auth:

- `payd_http_request_duration_seconds{route,status}`: histogram, labelled by route pattern (`POST /payments/{id}/capture`), never the raw path
- `payd_outbox_lag_seconds`: age of the oldest unpublished event, read on each scrape: 0 when there is none, -1 if the query fails
- `payd_outbox_events_published_total`

## Benchmarks

`scripts/bench.sh` starts the Compose stack and runs `cmd/loadgen` against it: each client loops create, authorise, capture in full, refund 400, each request with a fresh key, 3s of warmup and then 20s measured. The fake acquirer answers every authorise after 20ms. Throughput counts requests of all four kinds; a payment is four requests.

```sh
ACQUIRER_LATENCY=20ms PAYD_POOL_MAX_CONNS=8 LEVELS="8 32 128" ./scripts/bench.sh
```

Pool size before and after, measured 2026-10-20, p99 over all requests. The 128-client row was run three times for each pool size.

| clients | 8 connections | p99 | 32 connections | p99 |
|---|---|---|---|---|
| 8 | 852 req/s | 27ms | 829 req/s | 29ms |
| 32 | 870 req/s | 69ms | 1,313 req/s | 55ms |
| 128 | 761, 799, 853 req/s | 331, 409, 308ms | 1,317, 1,144, 1,328 req/s | 152, 642, 156ms |

With pgx's default of 8 connections (one per CPU here), throughput stops at about 860 req/s from 8 clients up, and latency grows in step with the client count. Every authorise holds a connection for at least the acquirer's 20ms, so the pool is the bottleneck and extra clients only queue for it. 32 connections raised throughput by 51% at 32 clients and by 43% to 73% at 128 clients across the three pairs of runs, and Compose now defaults to 32. 64 connections added at most 8% more (857, 1,308 and 1,416 req/s at 8, 32 and 128 clients), so the next limit is somewhere else. It hasn't been profiled; the candidates are commit latency on one SATA SSD and CPU, since the clients, payd and Postgres share one laptop. The p99 at 128 clients did not improve reliably: one of the three runs at 32 connections reached 642ms, which is unexplained.

Intel Core i7-8650U (4 cores, 8 threads), SATA SSD (ext4), Linux 6.8, Go 1.26.0, Docker 29.9, Postgres 16 with default settings, with other work running on the machine (10 of 15 GB of memory in use).

## Tests

```sh
go test -race ./...                                                          # everything, needs Docker
go test -race -run TestModel ./internal/server -model.runs=500 -model.seed=1 # model test, bigger
go test -race -run TestCrash ./cmd/payd -crash.kills=200 -crash.seed=1       # crash test, bigger
./scripts/mutate.sh                                                          # reintroduced bugs, 20 runs each
```

Integration tests run against Postgres 16 in Testcontainers; CI runs gofmt, vet and the whole suite with `-race` on pushes to main and on pull requests.

### Model test

`TestModel` sends seeded random sequences of operations to the API and checks every response against an in-memory reference model: creates with valid and invalid money, authorise, capture and refund with no amount, zero, in range and over the limit, void, get, list, replays of earlier keys (the response must match byte for byte) and keys reused with another body (422). It checks status codes, error codes and every field of the returned payment, follows the authorise response for `authorised` versus `failed`, and runs the ledger check after each run. `go test` runs 5 runs of 100 operations; on 2026-10-20, with `-race`, `-model.runs=200 -model.ops=200 -model.seed=1` passed 40,000 operations in 113 seconds. That run predates the acquirer; the default size passes on the final code.

### Crash test

On 2026-10-20, before the acquirer existed (commit `d9ce927`), 200 kills on each of seeds 1, 2 and 3 passed: 600 kills, 9,449 payments and 36,412 committed state changes, each with exactly one outbox event, the ledger balanced every time, and 1,838 events republished after a crash, none lost. With the acquirer injecting 5% errors and 2% lost replies, 10-kill runs pass; a seeded run gave 375 committed changes with 27 republished events. The bigger runs have not been repeated with acquirer faults on.

### Reintroduced bugs

`scripts/mutate.sh` applies each patch in `mutations/`, runs the whole suite (crash test at 10 kills) 20 times, reverts it, and reports how often each test failed. Measured 2026-10-20, with `-race`. The first eight were measured on commit `9ded13c`, before the acquirer; `list-oldest-first` was measured on its own commit, before the acquirer as well; the two acquirer patches only had 3 runs each.

| bug | caught by | failed |
|---|---|---|
| no `FOR UPDATE` on the payment row | concurrent captures, concurrent refunds | 20 of 20 |
| no unique key on idempotency keys, no advisory lock | duplicate capture; duplicate create, with 4 to 44 payments from 50 identical requests | 20 of 20; 19 of 20 |
| no advisory lock (unique key kept) | duplicate capture; duplicate create. No double charge, but callers get 500 instead of the stored response | 20 of 20; 19 of 20 |
| request body not compared on replay | `TestReusedKeyWithDifferentBodyIsRejected` | 20 of 20 |
| keys never expire | `TestKeysExpireAfter24Hours` | 20 of 20 |
| ledger posted in a second transaction after commit | crash test | 20 of 20 |
| outbox event written in a second transaction after commit | crash test | 20 of 20 |
| relay marks a batch published before publishing it | crash test, events lost | 5 of 20 |
| list sorted oldest first | model test only | 20 of 20 |
| no per-attempt acquirer timeout | `TestRetriesTransientErrorsWithTheSameRef` | 3 of 3 |
| acquirer reference changes between retries | the retry test, the outage test, concurrent authorises | 3 of 3 |

`-count=20` reuses one database per package, so a test that checks the whole ledger can keep failing on rows an earlier iteration corrupted; the table counts only tests that check their own payments, and the crash test starts a fresh database each run. These rates were measured before a review on 2026-10-20 tightened several assertions and added tests; `scripts/mutate.sh` now also reports a hang or a failed package instead of calling it "not caught". The two after-commit patches pass every check when the crash test runs with no kills (3 of 3 runs each), so the crashes are what catch them, not a side effect of the patch. The relay patch is caught only when a kill lands between the mark and the publish, so 5 of 20 is the honest rate at 10 kills per run. A first version of the after-commit patches wrote in a second transaction before the commit instead; that deadlocks against the row lock on the payments foreign key and was rewritten, since a hang proves nothing about the checks.

## Limitations

- The acquirer call sits inside the payment's transaction, so an authorise holds a pool connection, an advisory lock and the payment row lock for as long as the acquirer takes, up to 3 attempts of 500ms plus backoff. Acquirer latency eats into the pool (see Benchmarks). A design for a slow real network would record an `authorising` state, commit, call out, and finish in a second transaction with a recovery job for the gap.
- If the acquirer approves and payd then fails to commit, or every attempt times out after the acquirer did approve, the acquirer holds an authorisation payd has no record of until the client retries the key (which gets the first decision back). A client that never retries leaves it there. Real systems reconcile against the network's records or let the authorisation expire.
- The acquirer is a fake, in-process, and forgets its decisions when payd restarts. There is no real card data, no 3DS, no PCI scope.
- One region, one Postgres primary; no failover or read replicas.
- Auth is a static API key per merchant, stored as a SHA-256 hash, created by hand in SQL. No key rotation, scopes or rate limits.
- The relay publishes to the process log. There is no broker; a real deployment would swap the publish function. Publishing is at least once, and ordering is by event id, which is not strictly commit order across concurrent transactions.
- Expired idempotency keys are only deleted when the same key is reused; nothing sweeps the table.
- If publishing one event kept failing, the relay would retry its batch forever and stall everything behind it. Publishing to the log can't fail today; a real broker would need a retry limit and a dead-letter table.
- `/metrics` is unauthenticated on the API port, and requests rejected by the API key check are not in the latency histogram.
- Fixed currency list of 11 codes, no FX, no minor unit exponents (a JPY amount is still "minor units").
- `GET /payments` returns the newest 100 at most, with no cursor.
