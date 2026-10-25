import json
import subprocess
import sys
from pathlib import Path

from service_charging_consumer import unique_sessions
from service_scheduling_consumer import unique_alerts

HERE = Path(__file__).resolve().parent.parent


def session(sid, energy=1.0):
    return {"session_id": sid, "vin": "SYNTESTVIN0000001", "close_reason": "charge_stop", "energy_added_kwh": energy}


def alert(aid):
    return {"alert_id": aid, "vin": "SYNTESTVIN0000001", "dtc_code": "P0A80", "severity": 4, "lat": 37.12,
            "lon": -122.04}


def test_sessions_dedupe_keeps_first_in_order():
    out = list(unique_sessions([session("a", 1.0), session("b"), session("a", 9.0), session("c"), session("b")]))
    assert [s["session_id"] for s in out] == ["a", "b", "c"]
    assert out[0]["energy_added_kwh"] == 1.0


def test_alerts_dedupe():
    out = list(unique_alerts([alert("x"), alert("x"), alert("y")]))
    assert [a["alert_id"] for a in out] == ["x", "y"]


def test_empty_stream():
    assert list(unique_sessions([])) == []
    assert list(unique_alerts([])) == []


def run(script, rows, tmp_path):
    f = tmp_path / "in.jsonl"
    f.write_text("".join(json.dumps(r) + "\n" for r in rows))
    out = subprocess.run([sys.executable, str(HERE / script), "--from-file", str(f)], check=True,
                         capture_output=True, text=True, cwd=HERE).stdout.splitlines()
    return out


def test_charging_consumer_from_file(tmp_path):
    out = run("service_charging_consumer.py", [session("a"), session("a"), session("b")], tmp_path)
    assert out[-1] == "sessions=2"


def test_scheduling_consumer_from_file(tmp_path):
    out = run("service_scheduling_consumer.py", [alert("x"), alert("x")], tmp_path)
    assert out[-1] == "alerts=1"
