import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "consumer"))
from processing import Processor


def tick(price, ts=1_700_000_000, symbol="AAPL", volume=100):
    return {"symbol": symbol, "price": price, "timestamp": ts, "volume": volume}


def test_invalid_messages_dropped():
    p = Processor()
    assert p.process({"symbol": "AAPL"})[0] is None
    assert p.process(tick(-5))[0] is None
    assert p.process(tick(float("nan")))[0] is None


def test_pct_change_and_moving_average():
    p = Processor(window=2)
    p.process(tick(100))
    row, _ = p.process(tick(110, ts=1_700_000_005))
    assert round(row["pct_change"], 2) == 10.0
    assert row["moving_avg"] == 105.0


def test_alert_triggered():
    p = Processor(alert_pct=1.0)
    p.process(tick(100))
    _, alert = p.process(tick(102, ts=1_700_000_005))
    assert alert is not None and "AAPL" in alert["message"]


def test_no_alert_for_small_move():
    p = Processor(alert_pct=1.0)
    p.process(tick(100))
    _, alert = p.process(tick(100.5, ts=1_700_000_005))
    assert alert is None
