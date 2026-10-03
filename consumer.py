"""Consumes ticks from Kafka, enriches them, and writes to TimescaleDB."""
import json
import logging
import os
import time

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from processing import Processor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
DSN = os.getenv("DB_DSN", "postgresql://stocks:stocks@localhost:5432/stocks")
MA_WINDOW = int(os.getenv("MA_WINDOW", "20"))
ALERT_PCT = float(os.getenv("ALERT_PCT", "1.0"))

PRICE_SQL = """
INSERT INTO stock_prices (ts, symbol, price, volume, pct_change, moving_avg)
VALUES %s ON CONFLICT (symbol, ts) DO NOTHING
"""
ALERT_SQL = """
INSERT INTO alerts (ts, symbol, price, pct_change, message)
VALUES %s ON CONFLICT (symbol, ts) DO NOTHING
"""


def connect_kafka() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id="stock-processor",
                auto_offset_reset="earliest",
                enable_auto_commit=False,  # commit only after DB write => at-least-once
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying in 5s...")
            time.sleep(5)


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(DSN)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError:
            log.warning("DB not ready, retrying in 5s...")
            time.sleep(5)


def main():
    consumer = connect_kafka()
    conn = connect_db()
    proc = Processor(window=MA_WINDOW, alert_pct=ALERT_PCT)
    log.info("Consuming '%s' (MA window=%d, alert >= %.2f%%)", TOPIC, MA_WINDOW, ALERT_PCT)

    while True:
        batches = consumer.poll(timeout_ms=1000, max_records=500)
        rows, alerts = [], []
        for records in batches.values():
            for r in records:
                row, alert = proc.process(r.value)
                if row is None:
                    log.warning("Dropped invalid message: %s", r.value)
                    continue
                rows.append((row["ts"], row["symbol"], row["price"], row["volume"],
                             row["pct_change"], row["moving_avg"]))
                if alert:
                    alerts.append((alert["ts"], alert["symbol"], alert["price"],
                                   alert["pct_change"], alert["message"]))
                    log.warning("ALERT: %s", alert["message"])
        if not batches:
            continue
        try:
            with conn.cursor() as cur:
                if rows:
                    execute_values(cur, PRICE_SQL, rows)
                if alerts:
                    execute_values(cur, ALERT_SQL, alerts)
            conn.commit()
            consumer.commit()
            log.info("Stored %d rows, %d alerts", len(rows), len(alerts))
        except psycopg2.Error as exc:
            log.error("DB error: %s; reconnecting", exc)
            conn.close()
            conn = connect_db()


if __name__ == "__main__":
    main()
