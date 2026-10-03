"""Polls Yahoo Finance (via yfinance) and publishes price ticks to Kafka."""
import json
import logging
import os
import time

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "AAPL,MSFT").split(",") if t.strip()]
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))


def connect() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying in 5s...")
            time.sleep(5)


def fetch_tick(ticker: yf.Ticker, symbol: str):
    try:
        info = ticker.fast_info
        price = info["last_price"]
        if price is None:
            return None
        try:
            volume = info["last_volume"]
        except Exception:
            volume = None
        return {"symbol": symbol, "price": float(price), "volume": volume, "timestamp": time.time()}
    except Exception as exc:  # network errors, rate limits, bad tickers
        log.warning("Fetch failed for %s: %s", symbol, exc)
        return None


def main():
    producer = connect()
    tickers = {s: yf.Ticker(s) for s in TICKERS}
    log.info("Streaming %s to topic '%s' every %ss", TICKERS, TOPIC, POLL_SECONDS)
    while True:
        started = time.time()
        for symbol, ticker in tickers.items():
            tick = fetch_tick(ticker, symbol)
            if tick:
                producer.send(TOPIC, key=symbol, value=tick)
                log.info("%s %.2f", symbol, tick["price"])
        producer.flush()
        time.sleep(max(0.0, POLL_SECONDS - (time.time() - started)))


if __name__ == "__main__":
    main()
