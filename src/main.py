from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

data_dir = BASE_DIR / "data"
log_file = BASE_DIR / "logs" / "app.log"

print("======================== Pathlib ========================")
print(BASE_DIR)
print(data_dir)
print(log_file)
print(data_dir.exists())
print(log_file.name)
print()

import os
print("======================== OS ========================")
print(os.getcwd())
print(os.listdir("."))

os.environ["DB_USER"] = "sanju"
print(os.getenv("DB_USER"))
print()

import logging

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# logging.info("Pipeline started")
# logging.warning("Missing column detected")
# logging.error("Database connection failed")

from configparser import ConfigParser
config = ConfigParser()
config.read(Path("config/settings.ini"))

db_user = config["database"]["user"]
data_url = config["ingestion"]["data_url"]

print(db_user)
print(data_url)
print()

import pandas as pd

csv_path = data_dir / "airtravel.csv"

try:
    logging.info("Downloading data")
    df = pd.read_csv(data_url)
    df.to_csv(csv_path,index=False)
    logging.info(f"Data saved to {csv_path}")
    
    top_rows = df.head(5)
    logging.info(f"\nTop rows:\n{top_rows}")
except Exception as e:
    logging.error(f"Pipeline failed: {e}")