import pandas as pd
import os
import shutil
import logging
from sqlalchemy import create_engine

# ==============================
# CONFIGURATION
# ==============================

engine = create_engine(
    "postgresql://postgres:Aniket1234@localhost:5432/sales"
)

data_folder = r"C:\Users\Aniket\Documents\sales_automation_project\Data"
processed_folder = r"C:\Users\Aniket\Documents\sales_automation_project\Processed"
log_file = r"C:\Users\Aniket\Documents\sales_automation_project\logs\etl_log.log"

# Ensure folders exist
os.makedirs(processed_folder, exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# ==============================
# LOGGING SETUP
# ==============================

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("===== ETL Pipeline Started =====")

# ==============================
# REQUIRED COLUMNS
# ==============================

required_columns = [
    "order_id", "product_id", "category", "price", "cost", "quantity",
    "order_date", "region", "discount", "competitor_price"
]

# ==============================
# LOAD EXISTING DATA
# ==============================

try:
    existing_orders = pd.read_sql("SELECT order_id FROM sales", engine)
    existing_order_ids = set(existing_orders["order_id"])
except Exception as e:
    logging.error(f"Error loading existing order_ids: {e}")
    existing_order_ids = set()

# ==============================
# PROCESS FILES
# ==============================

for file in os.listdir(data_folder):

    if file.endswith(".csv"):

        file_path = os.path.join(data_folder, file)
        logging.info(f"Processing file: {file}")

        try:
            df = pd.read_csv(file_path)

            # Check required columns
            if not all(col in df.columns for col in required_columns):
                logging.warning(f"Skipping {file}: missing required columns")
                continue

            # Remove duplicates inside file
            df = df.drop_duplicates(subset="order_id")

            # Remove missing values
            df = df.dropna(subset=required_columns)

            # Convert date safely
            df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce')
            df = df.dropna(subset=["order_date"])

            # Create metrics
            df["revenue"] = df["price"] * df["quantity"]
            df["profit"] = df["revenue"] - (df["cost"] * df["quantity"])

            # Remove already existing records
            df = df[~df["order_id"].isin(existing_order_ids)]

            # Insert into DB
            if not df.empty:
                df.to_sql("sales", engine, if_exists="append", index=False)
                logging.info(f"{len(df)} rows inserted from {file}")
            else:
                logging.info(f"No new rows to insert from {file}")

            # ==============================
            # SAFE FILE MOVE (NO OVERWRITE)
            # ==============================

            base, ext = os.path.splitext(file)
            dest_path = os.path.join(processed_folder, file)
            counter = 1

            while os.path.exists(dest_path):
                dest_path = os.path.join(processed_folder, f"{base}_{counter}{ext}")
                counter += 1

            shutil.move(file_path, dest_path)
            logging.info(f"{file} moved to Processed folder")

        except Exception as e:
            logging.error(f"Error processing {file}: {e}")

# ==============================
# END
# ==============================

logging.info("===== ETL Pipeline Completed Successfully =====")
print("ETL pipeline executed successfully")