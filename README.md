# Real-Time Stock Price Pipeline

Yahoo Finance (`yfinance`) → Kafka → Python stream processor → TimescaleDB → Streamlit dashboard.

```
producer  --(JSON ticks)-->  Kafka  -->  consumer  -->  TimescaleDB  -->  Streamlit
 yfinance                  stock_prices   validate,        stock_prices     live charts,
                                          moving avg,      alerts           metrics, alerts
                                          % change, alerts
```

## Run

```bash
docker compose up --build
```

Then open http://localhost:8501. First data appears within ~10 seconds.

Stop with `docker compose down` (add `-v` to wipe the database).

## Configure

Edit the `environment` sections in `docker-compose.yml`:

| Variable       | Service  | Meaning                                   |
|----------------|----------|-------------------------------------------|
| `TICKERS`      | producer | Comma-separated symbols                   |
| `POLL_SECONDS` | producer | Seconds between polls                     |
| `MA_WINDOW`    | consumer | Moving-average window (ticks)             |
| `ALERT_PCT`    | consumer | Alert when a tick moves at least this %   |

`BTC-USD` trades 24/7, so it keeps the demo live when US markets are closed
(9:30-16:00 ET, weekdays).

## Inspect the data

```bash
docker compose exec db psql -U stocks -d stocks -c \
  "SELECT * FROM stock_prices ORDER BY ts DESC LIMIT 10;"
docker compose logs -f consumer
```

## Tests

```bash
pip install pytest
python -m pytest tests/
```

## Design notes

- **At-least-once delivery:** the consumer commits Kafka offsets only after a successful DB commit.
- **Idempotent writes:** primary key `(symbol, ts)` with `ON CONFLICT DO NOTHING`, so replays don't duplicate rows.
- **Time-series storage:** `stock_prices` is a TimescaleDB hypertable.
- **Caveat:** `yfinance` is unofficial. It can be rate-limited or break when Yahoo changes things, so keep the ticker list small and the poll interval at 5s or more.

## Ideas to extend

- Add a retention/compression policy in TimescaleDB
- Add Grafana on port 3000 pointed at the DB
- Swap the consumer for Spark Structured Streaming
- Send alerts to Slack or email
