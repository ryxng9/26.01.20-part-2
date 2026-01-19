#!/usr/bin/env python3
"""
FastF1 bulk downloader/exporter (2025 only), rate-limit friendly

Output structure (matches your screenshot):
  output/2025/<EventName>/<SessionName>/
    - laps.csv
    - car_data_driver_<num>.csv
    - position_data_driver_<num>.csv
    - weather_data.csv (if available)
    - session_results.csv (if available)
    - session_metadata.csv

Resume support:
  progress/download_progress.txt

Cache:
  .fastf1_cache (persist via GitHub Actions cache)

Rate friendliness:
  - configurable delay + jitter
  - retry with exponential backoff for transient failures
  - hard caps: max sessions per run + max runtime minutes
"""

import os
import time
import random
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import fastf1


# -------------------------
# Configuration (2025 only)
# -------------------------
YEAR = 2022

# Keep your session list
SESSIONS = ['FP1', 'FP2', 'FP3', 'Sprint Shootout', 'Sprint', 'Q', 'R']

# Folder structure (matches your workflow: output + progress)
OUTPUT_BASE_DIR = Path(os.getenv("F1_OUTPUT_DIR", "output"))
PROGRESS_DIR = Path(os.getenv("F1_PROGRESS_DIR", "progress"))
PROGRESS_FILE = PROGRESS_DIR / os.getenv("F1_PROGRESS_FILE", "download_progress.txt")

# FastF1 cache dir (matches workflow cache path)
CACHE_DIR = Path(os.getenv("F1_CACHE_DIR", ".fastf1_cache"))

# Rate limit friendliness (tune via workflow env)
DELAY_BETWEEN_SESSIONS = float(os.getenv("F1_DELAY_SECONDS", "2.5"))
JITTER_SECONDS = float(os.getenv("F1_JITTER_SECONDS", "1.0"))

MAX_SESSIONS_PER_RUN = int(os.getenv("F1_MAX_SESSIONS_PER_RUN", "60"))
MAX_RUNTIME_MINUTES = int(os.getenv("F1_MAX_RUNTIME_MINUTES", "50"))

MAX_RETRIES = int(os.getenv("F1_MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = float(os.getenv("F1_BACKOFF_BASE_SECONDS", "3.0"))
BACKOFF_MAX_SECONDS = float(os.getenv("F1_BACKOFF_MAX_SECONDS", "60.0"))

# IMPORTANT:
# Your screenshot does NOT show telemetry_fastest_*.csv.
# That export can add load. Default OFF; you can enable via env if you need it.
EXPORT_DETAILED_TELEMETRY = os.getenv("F1_EXPORT_DETAILED_TELEMETRY", "0").lower() in ("1", "true", "yes")


# -------------------------
# Logging
# -------------------------
PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = PROGRESS_DIR / "f1_download.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger("f1_downloader")


# -------------------------
# FastF1 cache
# -------------------------
CACHE_DIR.mkdir(parents=True, exist_ok=True)
fastf1.Cache.enable_cache(str(CACHE_DIR))


# -------------------------
# Helpers
# -------------------------
def load_progress() -> set[str]:
    if PROGRESS_FILE.exists():
        with PROGRESS_FILE.open("r", encoding="utf-8") as f:
            done = set(line.strip() for line in f if line.strip())
        logger.info(f"Loaded {len(done)} completed sessions from progress file")
        return done
    return set()


def save_progress(session_id: str) -> None:
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    with PROGRESS_FILE.open("a", encoding="utf-8") as f:
        f.write(session_id + "\n")


def sanitize_event_name(event_name: str) -> str:
    safe = "".join(c for c in event_name if c.isalnum() or c in (" ", "-", "_")).strip()
    return safe.replace(" ", "_")


def create_folder_structure(year: int, event_name: str, session_name: str) -> Path:
    path = OUTPUT_BASE_DIR / str(year) / sanitize_event_name(event_name) / session_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_dataframe(df: pd.DataFrame, path: Path, filename: str) -> bool:
    try:
        if df is not None and not df.empty:
            (path / filename).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(path / filename, index=True)
            logger.info(f"  Saved: {filename}")
            return True
    except Exception as e:
        logger.error(f"  Error saving {filename}: {e}")
    return False


def is_rate_limit_error(msg: str) -> bool:
    m = (msg or "").lower()
    return ("ratelimitexceedederror" in m) or ("500 calls" in m) or ("rate limit" in m)


def sleep_with_jitter(base_seconds: float) -> None:
    time.sleep(base_seconds + (random.random() * JITTER_SECONDS))


def should_stop(start_ts: float, sessions_done: int) -> bool:
    if sessions_done >= MAX_SESSIONS_PER_RUN:
        logger.warning(f"Reached MAX_SESSIONS_PER_RUN={MAX_SESSIONS_PER_RUN}. Stopping this run.")
        return True
    elapsed_min = (time.time() - start_ts) / 60.0
    if elapsed_min >= MAX_RUNTIME_MINUTES:
        logger.warning(f"Reached MAX_RUNTIME_MINUTES={MAX_RUNTIME_MINUTES}. Stopping this run.")
        return True
    return False


def export_session_data(session: fastf1.core.Session, path: Path) -> bool:
    """
    Exports the same categories as your original script:
    - results, laps, weather_data, car_data per driver, pos_data per driver, metadata
    - optional detailed fastest-lap telemetry (off by default)
    """
    exported_any = False

    # 1) Session Results
    try:
        if getattr(session, "results", None) is not None:
            exported_any |= save_dataframe(session.results, path, "session_results.csv")
    except Exception as e:
        logger.error(f"  Error exporting session results: {e}")

    # 2) Laps
    try:
        laps = getattr(session, "laps", None)
        if laps is not None:
            exported_any |= save_dataframe(laps, path, "laps.csv")
    except Exception as e:
        logger.error(f"  Error exporting laps: {e}")

    # 3) Weather
    try:
        wd = getattr(session, "weather_data", None)
        if wd is not None:
            exported_any |= save_dataframe(wd, path, "weather_data.csv")
    except Exception as e:
        logger.error(f"  Error exporting weather data: {e}")

    # 4) Car data (per driver)
    try:
        car_data = getattr(session, "car_data", None)
        if car_data:
            for driver_num, df in car_data.items():
                if df is not None and not df.empty:
                    exported_any |= save_dataframe(df, path, f"car_data_driver_{driver_num}.csv")
    except Exception as e:
        logger.error(f"  Error exporting car data: {e}")

    # 5) Position data (per driver)
    try:
        pos_data = getattr(session, "pos_data", None)
        if pos_data:
            for driver_num, df in pos_data.items():
                if df is not None and not df.empty:
                    exported_any |= save_dataframe(df, path, f"position_data_driver_{driver_num}.csv")
    except Exception as e:
        logger.error(f"  Error exporting position data: {e}")

    # 6) Session metadata
    try:
        event = getattr(session, "event", None)
        sess_info = getattr(session, "session_info", {}) or {}
        meta = {
            "EventName": event["EventName"] if event is not None and "EventName" in event else None,
            "Country": event["Country"] if event is not None and "Country" in event else None,
            "Location": event["Location"] if event is not None and "Location" in event else None,
            "EventDate": str(event["EventDate"]) if event is not None and "EventDate" in event else None,
            "SessionName": getattr(session, "name", None),
            "SessionDate": str(getattr(session, "date", None)),
            "SessionType": sess_info.get("Type"),
        }
        exported_any |= save_dataframe(pd.DataFrame([meta]), path, "session_metadata.csv")
    except Exception as e:
        logger.error(f"  Error exporting session metadata: {e}")

    # 7) Optional: Detailed fastest-lap telemetry per driver (can be heavy)
    if EXPORT_DETAILED_TELEMETRY:
        try:
            laps = getattr(session, "laps", None)
            if laps is not None and not laps.empty and "Driver" in laps.columns:
                for driver in laps["Driver"].dropna().unique():
                    try:
                        driver_laps = laps.pick_drivers(driver)
                        if driver_laps.empty:
                            continue
                        fastest_lap = driver_laps.pick_fastest()
                        if fastest_lap is None or not hasattr(fastest_lap, "get_telemetry"):
                            continue
                        tel = fastest_lap.get_telemetry()
                        if tel is not None and not tel.empty:
                            exported_any |= save_dataframe(tel, path, f"telemetry_fastest_{driver}.csv")
                    except Exception as e:
                        logger.debug(f"  Could not get fastest telemetry for {driver}: {e}")
        except Exception as e:
            logger.error(f"  Error exporting detailed telemetry: {e}")

    return exported_any


def load_session_with_retries(year: int, event_name: str, session_name: str) -> fastf1.core.Session:
    last_err: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = fastf1.get_session(year, event_name, session_name)

            # Efficiency choices:
            # - messages=False (you aren't exporting messages)
            # - keep laps/telemetry/weather because you export laps + car_data/pos_data + weather_data
            session.load(laps=True, telemetry=True, weather=True, messages=False)

            return session
        except Exception as e:
            last_err = e
            msg = str(e)

            # If rate limit, bubble up so caller can stop cleanly
            if is_rate_limit_error(msg):
                raise

            backoff = min(BACKOFF_MAX_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
            backoff += random.random() * JITTER_SECONDS

            logger.warning(f"  Load failed (attempt {attempt}/{MAX_RETRIES}) for {year} {event_name} {session_name}: {msg}")
            if attempt < MAX_RETRIES:
                logger.info(f"  Backing off for {backoff:.1f}s...")
                time.sleep(backoff)

    assert last_err is not None
    raise last_err


def download_2025() -> None:
    completed = load_progress()
    start_ts = time.time()
    sessions_done_this_run = 0

    total_attempted = 0
    successful = 0
    skipped = 0
    rate_limit_hit = False

    logger.info("\n" + "=" * 80)
    logger.info(f"Processing Year: {YEAR}")
    logger.info("=" * 80)

    # One schedule call for the year
    try:
        schedule = fastf1.get_event_schedule(YEAR)
    except Exception as e:
        msg = str(e)
        if is_rate_limit_error(msg):
            logger.error("Rate limit hit while loading schedule. Stop and resume later.")
            return
        raise

    for _, event in schedule.iterrows():
        if rate_limit_hit or should_stop(start_ts, sessions_done_this_run):
            break

        event_name = event.get("EventName")
        if not event_name:
            continue

        logger.info(f"\n--- Event: {event_name} ---")

        for session_name in SESSIONS:
            if rate_limit_hit or should_stop(start_ts, sessions_done_this_run):
                break

            session_id = f"{YEAR}|{event_name}|{session_name}"

            if session_id in completed:
                logger.info(f"Skipping already completed: {session_id}")
                skipped += 1
                continue

            total_attempted += 1
            sessions_done_this_run += 1
            logger.info(f"\nProcessing: {session_id}")

            try:
                session = load_session_with_retries(YEAR, event_name, session_name)
                out_path = create_folder_structure(YEAR, event_name, session_name)

                if export_session_data(session, out_path):
                    successful += 1
                    save_progress(session_id)
                    completed.add(session_id)
                    logger.info(f"✓ Successfully exported data for {session_id}")
                else:
                    logger.warning(f"✗ No data exported for {session_id}")

                sleep_with_jitter(DELAY_BETWEEN_SESSIONS)

            except Exception as e:
                msg = str(e)
                if is_rate_limit_error(msg):
                    logger.error("\n" + "!" * 80)
                    logger.error("RATE LIMIT HIT!")
                    logger.error("Stopping now. Next run will resume via progress file.")
                    logger.error("!" * 80)
                    rate_limit_hit = True
                    break

                # Many events simply don't have Sprint sessions; treat as a normal error and continue.
                logger.warning(f"✗ Could not process {session_id}: {msg}")
                continue

    logger.info("\n" + "=" * 80)
    logger.info("Download Summary")
    logger.info("=" * 80)
    logger.info(f"Sessions attempted this run: {total_attempted}")
    logger.info(f"Successful downloads: {successful}")
    logger.info(f"Previously completed (skipped): {skipped}")
    logger.info(f"Status: {'PAUSED due to rate limit' if rate_limit_hit else 'DONE (or stopped by caps)'}")
    logger.info("=" * 80)


if __name__ == "__main__":
    logger.info(f"Starting download at {datetime.now().isoformat(timespec='seconds')}")
    download_2025()
    logger.info(f"Finished at {datetime.now().isoformat(timespec='seconds')}")
