import os
import json
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
JSON_FILE_NAME = 'vellore_floods_sms.json'

# --- New Function to Read from File ---
def read_sms_from_json(filename):
    """Reads and parses SMS data from a local JSON file."""
    print(f"🔎 Reading data from '{filename}'...")
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"✅ Successfully loaded {len(data)} records from file.")
            return data
    except FileNotFoundError:
        print(f"❌ ERROR: File not found. Make sure '{filename}' is in the same directory.")
        return None
    except json.JSONDecodeError:
        print(f"❌ ERROR: Could not parse JSON. Please check the file for formatting errors.")
        return None

# --- Database Functions ---

def get_db_connection():
    """Establishes and returns a connection to the Neon database."""
    try:
        conn = psycopg2.connect(NEON_DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def setup_sms_table(conn):
    """Ensures the sms_reports table exists in the database."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sms_reports (
                id SERIAL PRIMARY KEY,
                sender_number TEXT NOT NULL,
                recipient_number TEXT,
                message_body TEXT,
                message_sid TEXT UNIQUE NOT NULL,
                received_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    conn.commit()
    print("✅ 'sms_reports' table is ready.")

def insert_sms_data(conn, sms_data):
    """Inserts the list of SMS data into the database."""
    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO sms_reports (sender_number, recipient_number, message_body, message_sid)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (message_sid) DO NOTHING;
        """
        
        data_to_insert = [
            (
                msg['from'],
                msg['to'],
                msg['body'],
                msg['messageSid']
            ) for msg in sms_data
        ]
        
        extras.execute_batch(cur, insert_query, data_to_insert)
        inserted_count = cur.rowcount
        conn.commit()

    print(f"✅ Successfully inserted {inserted_count} new SMS records.")
    if inserted_count < len(sms_data):
        print(f"ℹ️ {len(sms_data) - inserted_count} records were already in the database and were skipped.")

# --- Main Execution ---
if __name__ == "__main__":
    if not NEON_DATABASE_URL:
        print("❌ ERROR: NEON_DATABASE_URL not found in .env file.")
    else:
        print("--- Starting SMS Data Load Script ---")
        
        # 1. Load data from the external JSON file
        sms_data_to_load = read_sms_from_json(JSON_FILE_NAME)
        
        # 2. Proceed only if data was loaded successfully
        if sms_data_to_load:
            db_connection = get_db_connection()
            if db_connection:
                try:
                    # 3. Ensure the table exists
                    setup_sms_table(db_connection)
                    
                    # 4. Insert the data from the file
                    insert_sms_data(db_connection, sms_data_to_load)
                finally:
                    # 5. Close the connection
                    db_connection.close()
                    print("🔒 Database connection closed.")
        
        print("--- Script finished. ---")