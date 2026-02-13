import pandas as pd
import requests
import json
import time
import os
import zipfile
import smtplib
import traceback
import numpy as np
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text, bindparam
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from dotenv import load_dotenv
load_dotenv()

# ==============================================================================
# 1. GLOBAL CONFIGURATION (Using Environment Variables for Security)
# ==============================================================================
# Business Logic Configuration
MARKETS_TO_PROCESS = os.getenv('MARKETS_TO_PROCESS', 'region_a,region_b,region_c').split(',')
ORDER_STATUSES = [
    "APPROVED", "PROCESSING", "SUBMITTED", "PENDING", "PENDING_PAYMENT",
    "PENDING_APPROVAL", "FAILED", "REMOVED", "INCOMPLETE"
]

# Database Configuration
DB_CONFIG = {
    "server": os.getenv('DB_SERVER', 'YOUR_SQL_SERVER'),
    "database": os.getenv('DB_DATABASE', 'ECOMMERCE_DW'),
    "table": os.getenv('DB_TABLE', 'fact_sales_orders'),
    "schema": os.getenv('DB_SCHEMA', 'dbo')
}

# API Configuration
API_CONFIG = {
    "base_url": os.getenv('API_BASE_URL', 'https://your-instance.oraclecloud.com/ccagentui/v1'),
    "user": os.getenv('API_USER', 'admin@company.com'),
    "pass": os.getenv('API_PASS', 'your_secure_password')
}

# Email Configuration
EMAIL_CONFIG = {
    "smtp_server": "smtp.office365.com",
    "smtp_port": 587,
    "user": os.getenv('EMAIL_USER', 'automation@company.com'),
    "pass": os.getenv('EMAIL_PASS', 'email_password'),
    "recipients": os.getenv('EMAIL_RECIPIPIENTS', 'your_email@gmail.com').split(',')
}

# Paths & Storage
BASE_DIR = Path(os.getenv('BASE_DIR', './data_output'))
BASE_DIR.mkdir(parents=True, exist_ok=True)
CITY_MAPPING_FILE = BASE_DIR / 'address_metadata.json'

# ==============================================================================
# 2. AUTHENTICATION & TOKEN LIFECYCLE MANAGEMENT
# ==============================================================================
bearer_header = None
token_expire = 0

def get_auth_token():
    """Obtains and refreshes the Bearer token for the Cloud API."""
    global bearer_header, token_expire
    url = f"{API_CONFIG['base_url']}/login/"
    payload = f"grant_type=password&username={API_CONFIG['user']}&password={API_CONFIG['pass']}"
    headers = {'accept': 'application/json', 'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'}

    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        token_data = response.json()
        bearer_header = f"Bearer {token_data.get('access_token')}"
        # Set expiration with a 60-second safety buffer
        token_expire = time.time() + token_data.get("expires_in", 3600) - 60
        print(f"[AUTH] Token refreshed successfully. Expires in {token_data.get('expires_in')}s")
    except Exception as e:
        print(f"[AUTH ERROR] Failed to obtain token: {e}")
        raise

def ensure_valid_token():
    """Middleware-like function to ensure token validity before API calls."""
    if not bearer_header or time.time() >= token_expire:
        get_auth_token()

# ==============================================================================
# 3. DATA ENGINEERING: SMART UPSERT LOGIC
# ==============================================================================

def execute_smart_upsert(df_new_data, engine):
    """
    Performs an intelligent Upsert (Update or Insert) on SQL Server.
    Uses a composite key (Order ID + SKU) to detect new records or state changes.
    """
    if df_new_data.empty:
        print("[SQL] No new data to process.")
        return

    print(f"\n[SQL] Starting Smart Upsert for {len(df_new_data)} records...")

    # Generate Composite Key for comparison
    df_new_data['sku_clean'] = df_new_data['product_sku'].fillna('').astype(str)
    df_new_data['order_id_str'] = df_new_data['id'].astype(str)
    df_new_data['composite_key'] = df_new_data['order_id_str'] + '_' + df_new_data['sku_clean']

    unique_ids = df_new_data['id'].unique().tolist()
    batch_size = 1000
    total_ins, total_upd = 0, 0

    for i in range(0, len(unique_ids), batch_size):
        batch_ids = unique_ids[i:i + batch_size]
        df_batch = df_new_data[df_new_data['id'].isin(batch_ids)].copy()

        # Check existing records in DB for this batch
        query = text(f"SELECT id, product_sku, state FROM [{DB_CONFIG['schema']}].[{DB_CONFIG['table']}] WHERE id IN :ids")
        try:
            with engine.connect() as conn:
                df_existing = pd.read_sql_query(query, conn, params={'ids': batch_ids})

            if not df_existing.empty:
                df_existing['composite_key'] = df_existing['id'].astype(str) + '_' + df_existing['product_sku'].fillna('').astype(str)

                # Identify NEW records
                df_to_insert = df_batch[~df_batch['composite_key'].isin(df_existing['composite_key'])].copy()

                # Identify UPDATED records (State changes)
                merged = pd.merge(df_batch, df_existing[['composite_key', 'state']], on='composite_key', suffixes=('_new', '_db'))
                df_to_update = merged[merged['state_new'] != merged['state_db']].copy()
            else:
                df_to_insert = df_batch.copy()
                df_to_update = pd.DataFrame()

            # Transactional execution
            with engine.begin() as conn:
                if not df_to_update.empty:
                    delete_sql = text(f"DELETE FROM [{DB_CONFIG['schema']}].[{DB_CONFIG['table']}] WHERE id = :id AND product_sku = :sku")
                    conn.execute(delete_sql, [{'id': r['id'], 'sku': r['sku_clean']} for _, r in df_to_update.iterrows()])
                    total_upd += len(df_to_update)

                # Prepare final load
                df_to_update.columns = [c.replace('_new', '') for c in df_to_update.columns]
                cols_to_drop = [c for c in df_to_update.columns if c.endswith('_db') or c in ['composite_key', 'sku_clean', 'order_id_str']]
                df_to_insert.drop(columns=['composite_key', 'sku_clean', 'order_id_str'], inplace=True, errors='ignore')
                df_to_update.drop(columns=cols_to_drop, inplace=True, errors='ignore')

                df_final = pd.concat([df_to_insert, df_to_update], ignore_index=True).replace({np.nan: None})

                # Align with DB schema columns
                target_cols = conn.execute(text(f"SELECT TOP 0 * FROM [{DB_CONFIG['schema']}].[{DB_CONFIG['table']}]")).keys()
                df_final = df_final[[c for c in df_final.columns if c in target_cols]]

                df_final.to_sql(DB_CONFIG['table'], con=conn, schema=DB_CONFIG['schema'], if_exists='append', index=False, chunksize=1000)
                total_ins += len(df_to_insert)

        except Exception as e:
            print(f"[SQL ERROR] Batch failure: {e}")
            continue

    print(f"[SQL] Process finished. Inserted: {total_ins}, Updated: {total_upd}")

# ==============================================================================
# 4. ETL CORE: EXTRACTION & TRANSFORMATION
# ==============================================================================

def flatten_nested_json(d, parent_key='', sep='_'):
    """Recursively flattens deeply nested JSON objects from the API."""
    if not isinstance(d, dict): return {}
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_nested_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def fetch_order_details(summary_row, city_map):
    """Enriches order summary with full details from the API."""
    order_id = summary_row.get('id')
    ensure_valid_token()
    headers = {'Authorization': bearer_header, 'Accept': 'application/json'}

    try:
        url = f"{API_CONFIG['base_url']}/orders/{order_id}?includeResult=full"
        res = requests.get(url, headers=headers)
        if res.status_code != 200: return []

        data = res.json()

        # Geolocation Enrichement
        shipping = flatten_nested_json(data.get('shippingAddress', {}), 'ship')
        city_id = shipping.get('ship_city')
        if city_id in city_map:
            shipping['city_name_normalized'] = city_map[city_id]

        # Order metadata
        order_meta = {**summary_row, **shipping, "processed_at": datetime.now(), "source_v": "Pipeline_V3"}

        # Line Item Expansion (ShoppingCart items)
        items = data.get("shoppingCart", {}).get("items", [])
        if not items: return [order_meta]

        product_rows = []
        for item in items:
            product_info = {
                "product_sku": item.get('catRefId'),
                "product_name": item.get('displayName'),
                "quantity": item.get('quantity'),
                "unit_price": item.get('unitPrice'),
                "total_price": item.get('rawTotalPrice')
            }
            product_rows.append({**order_meta, **product_info})
        return product_rows
    except Exception as e:
        print(f"[API ERROR] Order {order_id}: {e}")
        return []

def extract_market_data(market_id, city_map):
    """Orchestrates API extraction per market/site."""
    print(f"\n--- Extracting Market: {market_id} ---")
    summaries = []
    limit = 250

    for status in ORDER_STATUSES:
        ensure_valid_token()
        query = f'siteId sw "{market_id}" and state eq "{status}"'
        params = {'q': query, 'queryFormat': 'SCIM', 'limit': limit, 'sort': 'submittedDate:desc'}

        try:
            res = requests.get(f"{API_CONFIG['base_url']}/orders", headers={'Authorization': bearer_header}, params=params)
            if res.status_code == 200:
                items = res.json().get('items', [])
                summaries.extend(items)
                print(f"  Status {status}: Found {len(items)} orders.")
        except Exception as e:
            print(f"  Error on {status}: {e}")

    if not summaries: return pd.DataFrame()

    # Enrichment Phase
    enriched_data = []
    total = len(summaries)
    for idx, order in enumerate(summaries):
        enriched_data.extend(fetch_order_details(order, city_map))
        if (idx + 1) % 50 == 0:
            print(f"  Progress: {idx + 1}/{total} orders processed...", end='\r')

    return pd.DataFrame(enriched_data)

# ==============================================================================
# 5. NOTIFICATION & UTILS
# ==============================================================================

def send_completion_email(count, zip_file):
    """Sends an automated summary report via SMTP."""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_CONFIG["user"]
    msg['To'] = ", ".join(EMAIL_CONFIG["recipients"])
    msg['Subject'] = f"Data Pipeline Success - {datetime.now():%Y-%m-%d}"

    body = f"The ETL process completed successfully.\nTotal records: {count}\nReport attached."
    msg.attach(MIMEText(body, 'plain'))

    try:
        with open(zip_file, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(zip_file)}")
        msg.attach(part)

        with smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"]) as server:
            server.starttls()
            server.login(EMAIL_CONFIG["user"], EMAIL_CONFIG["pass"])
            server.sendmail(EMAIL_CONFIG["user"], EMAIL_CONFIG["recipients"], msg.as_string())
        print("[EMAIL] Notification sent.")
    except Exception as e:
        print(f"[EMAIL ERROR] {e}")

# ==============================================================================
# 6. MAIN EXECUTION (ORCHESTRATOR)
# ==============================================================================

def main():
    start_time = time.time()
    print(f"🚀 PIPELINE STARTED: {datetime.now():%Y-%m-%d %H:%M:%S}")

    # Load City Metadata
    try:
        with open(CITY_MAPPING_FILE, 'r', encoding='utf-8') as f:
            mapping_data = json.load(f)
            city_map = {c["id"]: c["displayName"] for r in mapping_data for c in r.get("city", [])}
    except:
        print("[WARN] City mapping file not found. Skipping normalization.")
        city_map = {}

    # Extraction Loop
    all_dfs = []
    for market in MARKETS_TO_PROCESS:
        df_market = extract_market_data(market, city_map)
        if not df_market.empty:
            all_dfs.append(df_market)

    if not all_dfs:
        print("❌ No data found. Exiting.")
        return

    # Data Consolidation
    df_final = pd.concat(all_dfs, ignore_index=True)
    df_final.dropna(axis=1, how='all', inplace=True)

    # Backup to Parquet (Production standard)
    parquet_path = BASE_DIR / f"backup_{datetime.now():%Y%m%d}.parquet"
    df_final.to_parquet(parquet_path)
    print(f"[BACKUP] Parquet persistence saved: {parquet_path}")

    # SQL Load
    try:
        conn_str = f"mssql+pyodbc://@{DB_CONFIG['server']}/{DB_CONFIG['database']}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        engine = create_engine(conn_str, pool_pre_ping=True)
        execute_smart_upsert(df_final, engine)
    except Exception as e:
        print(f"[CRITICAL SQL ERROR] {e}")

    # Final Reporting
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_p = BASE_DIR / f"report_{ts}.zip"
    xlsx_p = BASE_DIR / f"report_{ts}.xlsx"

    df_final.to_excel(xlsx_p, index=False)
    with zipfile.ZipFile(zip_p, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(xlsx_p, arcname=xlsx_p.name)

    send_completion_email(len(df_final), zip_p)
    os.remove(xlsx_p)

    print(f"✅ PIPELINE FINISHED in {time.time() - start_time:.2f}s")

if __name__ == '__main__':
    main()