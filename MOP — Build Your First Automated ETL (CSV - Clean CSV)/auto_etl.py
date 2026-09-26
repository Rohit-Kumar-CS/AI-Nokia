import os
import time
import pandas as pd
import schedule
from datetime import datetime

# File Paths
RAW_PATH = "telecom_raw.csv"
OUT_DIR = "output"

OUT_PATH = os.path.join(OUT_DIR, "telecom_cleaned.csv")
TMP_PATH = os.path.join(OUT_DIR, "telecom_cleaned.tmp.csv")
LOG_PATH = os.path.join(OUT_DIR, "etl_run.log")

# Create output folder
os.makedirs(OUT_DIR, exist_ok=True)


# Logging Function
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

    print(f"[{ts}] {msg}")


# Cleaning Function
def clean_frame(df):

    # Standardize Region Names
    if "region" in df.columns:
        df["region"] = (
            df["region"]
            .astype(str)
            .str.strip()
            .str.title()
        )

    # Fill Missing Numeric Values
    for col in ["data_used_gb", "calls_made", "revenue_inr"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(df[col].median())

    # Parse Dates
    if "date" in df.columns:
        df["date"] = pd.to_datetime(
            df["date"],
            errors="coerce",
            dayfirst=True
        )

        df["date"] = df["date"].fillna(
            pd.Timestamp("2025-09-25")
        )

    # Remove Duplicate Rows
    if {"customer_id", "date"}.issubset(df.columns):
        before = len(df)

        df = df.drop_duplicates(
            subset=["customer_id", "date"],
            keep="first"
        )

        removed = before - len(df)
        log(f"Removed {removed} duplicate rows.")

    # Safety Checks
    if "data_used_gb" in df.columns:
        df["data_used_gb"] = df["data_used_gb"].clip(0, 100)

    if "revenue_inr" in df.columns:
        df["revenue_inr"] = df["revenue_inr"].clip(lower=0)

    return df


# ETL JOB
def etl_job():

    try:

        log("Starting ETL Job...")

        if not os.path.exists(RAW_PATH):
            log("Raw CSV not found.")
            return

        # Extract
        df = pd.read_csv(RAW_PATH)

        # Transform
        df = clean_frame(df)

        # Load
        df.to_csv(TMP_PATH, index=False)

        os.replace(TMP_PATH, OUT_PATH)

        log(f"ETL Completed. Rows Written: {len(df)}")

    except Exception as e:
        log(f"ETL Failed: {e}")


# Scheduler
schedule.clear()

schedule.every(20).seconds.do(etl_job)

log("Scheduler Started...")

runs = 3

while runs > 0:

    schedule.run_pending()
    time.sleep(1)

    # Every 20 sec one execution
    if datetime.now().second % 20 == 0:
        runs -= 1

print("Done! Scheduler exited.")