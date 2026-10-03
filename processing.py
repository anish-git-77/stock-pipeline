"""Pure stream-processing logic (no I/O) so it is easy to unit test."""
from collections import defaultdict, deque
from datetime import datetime, timezone


class Processor:
    def __init__(self, window: int = 20, alert_pct: float = 1.0):
        self.window = window
        self.alert_pct = alert_pct
        self.last_price = {}
        self.history = defaultdict(lambda: deque(maxlen=window))

    @staticmethod
    def validate(msg):
        """Return a cleaned record or None if the message is invalid."""
        try:
            symbol = str(msg["symbol"]).upper()
            price = float(msg["price"])
            ts = float(msg["timestamp"])
        except (KeyError, TypeError, ValueError):
            return None
        if price != price or price <= 0:  # NaN or non-positive
            return None
        try:
            volume = int(msg.get("volume"))
        except (TypeError, ValueError):
            volume = None
        return {
            "ts": datetime.fromtimestamp(ts, tz=timezone.utc),
            "symbol": symbol,
            "price": price,
            "volume": volume,
        }

    def process(self, msg):
        """Return (price_row, alert_row). Either may be None."""
        rec = self.validate(msg)
        if rec is None:
            return None, None

        sym, price = rec["symbol"], rec["price"]
        prev = self.last_price.get(sym)
        pct = (price - prev) / prev * 100 if prev else 0.0
        self.last_price[sym] = price

        self.history[sym].append(price)
        avg = sum(self.history[sym]) / len(self.history[sym])

        row = {**rec, "pct_change": pct, "moving_avg": avg}
        alert = None
        if abs(pct) >= self.alert_pct:
            direction = "up" if pct > 0 else "down"
            alert = {
                "ts": rec["ts"],
                "symbol": sym,
                "price": price,
                "pct_change": pct,
                "message": f"{sym} moved {direction} {abs(pct):.2f}% to {price:.2f}",
            }
        return row, alert
