# path: f1_fastf1_downloader_test_jan_2025.py
"""
FastF1 Downloader – TEST MODE (January 2025 only)

This file is a SAFE, FAST test version of the full pipeline.
It downloads ONLY events that happened in January 2025 so you can
verify that:

- FastF1 cache works
- GitHub Actions artifacts are produced
- CSV files are written correctly
- No rate-limit crashes occur

Install:
    pip install fastf1 pandas

Run locally:
    python f1_fastf1_downloader_test_jan_2025.py
"""

from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd
import fastf1


# =============================================================================
# TEST CONFIG (JANUARY 2025 ONLY)
# =============================================================================

TEST_MODE = True
TEST_YEAR = 2025
TEST_START_DATE = "2025-03-01"
TEST_END_DATE = "2025-03-31"

YEARS = [2025]
SESSIONS = ["FP1", "FP2", "FP3", "Q", "R", "S"]

BASE_DIR = Path(".")
CACHE_DIR = BASE_DIR / ".fastf1_cache"
OUTPUT_DIR = BASE_DIR / "output"
PROGRESS_DIR = BASE_DIR / "progress"
LOG_DIR = BASE_DIR / "logs"
PROGRESS_FILE = PROGRESS_DIR / "progress.json"

BASE_BACKOFF = 30
MAX_BACKOFF = 1800
MAX_NETWORK_RETRIES = 3
POLITE_SLEEP = 1.5


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "f1_test_jan_2025.log"),
            logging.StreamHandler(),
        ],
    )


# =============================================================================
# PROGRESS
# =============================================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_progress() -> Dict[str, dict]:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {}


def save_progress(progress: Dict[str, dict]) -> None:
    PROGRESS_DIR.mkdir(exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))


def session_key(year: int, rnd: int, session: str) -> str:
    return f"{year}-R{rnd:02d}-{session}"


# =============================================================================
# ERROR HELPERS
# =============================================================================

def is_rate_limit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "429" in msg or "rate limit" in msg or "500 calls" in msg


def is_network_error(e: Exception) -> bool:
    msg = str(e).lower()
    return any(k in msg for k in ["timeout", "connection", "reset", "dns"])


def backoff(attempt: int) -> float:
    base = min(MAX_BACKOFF, BASE_BACKOFF * (2 ** attempt))
    jitter = base * 0.1
    return base + random.uniform(-jitter, jitter)


# =============================================================================
# EXPORT
# =============================================================================

def export_csv(df: pd.DataFrame, path: Path) -> int:
    if df is not None and not df.empty:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        return len(df)
    return 0


def export_session(session, out_dir: Path) -> dict:
    stats = {
        "laps": export_csv(session.laps, out_dir / "laps.csv"),
        "results": export_csv(session.results, out_dir / "results.csv"),
        "weather": export_csv(session.weather_data, out_dir / "weather.csv"),
        "telemetry": 0,
        "position": 0,
    }

    # telemetry (merged)
    frames = []
    for drv in session.drivers:
        try:
            df = session.car_data[drv]
            if not df.empty:
                df = df.copy()
                df.insert(0, "Driver", drv)
                frames.append(df)
        except Exception:
            pass

    if frames:
        stats["telemetry"] = export_csv(
            pd.concat(frames), out_dir / "telemetry.csv"
        )

    # position (merged)
    frames = []
    for drv in session.drivers:
        try:
            df = session.pos_data[drv]
            if not df.empty:
                df = df.copy()
                df.insert(0, "Driver", drv)
                frames.append(df)
        except Exception:
            pass

    if frames:
        stats["position"] = export_csv(
            pd.concat(frames), out_dir / "position.csv"
        )

    # metadata (always)
    meta = {
        "event": session.event["EventName"],
        "round": session.event["RoundNumber"],
        "session": session.name,
        "date": str(session.date),
        "exported_at": utc_now(),
    }
    export_csv(pd.DataFrame([meta]), out_dir / "metadata.csv")

    return stats


# =============================================================================
# MAIN
# =============================================================================

def run() -> None:
    setup_logging()

    CACHE_DIR.mkdir(exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)

    progress = load_progress()

    for year in YEARS:
        logging.info("Processing year %s", year)
        schedule = fastf1.get_event_schedule(year)

        if TEST_MODE:
            schedule["EventDate"] = pd.to_datetime(schedule["EventDate"])
            start = pd.to_datetime(TEST_START_DATE)
            end = pd.to_datetime(TEST_END_DATE)
            schedule = schedule[
                (schedule["EventDate"] >= start) &
                (schedule["EventDate"] <= end)
            ]
            logging.info(
                "TEST MODE ENABLED → %d event(s) in January 2025",
                len(schedule),
            )

        for _, event in schedule.iterrows():
            rnd = int(event["RoundNumber"])

            for sess in SESSIONS:
                key = session_key(year, rnd, sess)
                if progress.get(key, {}).get("status") in {"done", "no_data"}:
                    continue

                attempt = progress.get(key, {}).get("attempts", 0)
                logging.info("Session %s", key)

                try:
                    session = fastf1.get_session(year, rnd, sess)
                    session.load(telemetry=True, weather=True)

                    out_dir = OUTPUT_DIR / str(year) / f"round_{rnd:02d}" / sess
                    stats = export_session(session, out_dir)

                    status = "done" if any(stats.values()) else "no_data"
                    progress[key] = {
                        "status": status,
                        "attempts": attempt + 1,
                        "updated_at": utc_now(),
                        "stats": stats,
                    }
                    save_progress(progress)
                    time.sleep(POLITE_SLEEP)

                except Exception as e:
                    attempt += 1
                    if is_rate_limit_error(e):
                        wait = backoff(attempt)
                        logging.warning("Rate limit hit. Sleeping %.1fs", wait)
                        time.sleep(wait)
                        continue

                    if is_network_error(e) and attempt <= MAX_NETWORK_RETRIES:
                        wait = backoff(attempt)
                        logging.warning("Network error. Retry in %.1fs", wait)
                        time.sleep(wait)
                        continue

                    logging.error("Failed %s: %s", key, e)
                    progress[key] = {
                        "status": "failed",
                        "attempts": attempt,
                        "error": str(e),
                    }
                    save_progress(progress)

    logging.info("TEST RUN COMPLETE (January 2025)")


if __name__ == "__main__":
    run()
