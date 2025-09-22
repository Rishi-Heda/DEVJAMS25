import os
import requests
import psycopg2
import time
from dotenv import load_dotenv
from typing import Union, List, Dict

# --- Configuration ---
load_dotenv()
OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY")
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
OPENCAGE_ENDPOINT = "https://api.opencagedata.com/geocode/v1/json"

class DataManager:
    """Manages API calls for geocoding."""
    def get_coordinates(self, place_name: str) -> Union[dict, None]:
        if not OPENCAGE_API_KEY:
            print("❌ Error: OPENCAGE_API_KEY not found.")
            return None
        params = {'q': place_name, 'key': OPENCAGE_API_KEY, 'limit': 1}
        try:
            response = requests.get(OPENCAGE_ENDPOINT, params=params)
            response.raise_for_status()
            data = response.json()
            if data['results']:
                result = data['results'][0]
                return {"lat": result['geometry']['lat'], "lon": result['geometry']['lng']}
            return None
        except requests.exceptions.RequestException as e:
            print(f"   -> ❌ API Error: {e}")
            return None

# --- Database Functions ---
def setup_database_schema(conn):
    """Creates the 'geocoded_tweets' table with a status column."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geocoded_tweets (
                id SERIAL PRIMARY KEY,
                source_report_id INTEGER UNIQUE NOT NULL,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                event_summary TEXT,
                event_location TEXT,
                number_of_reports INTEGER,
                geocoded_at TIMESTAMPTZ DEFAULT NOW(),
                status TEXT DEFAULT 'reported' NOT NULL
            );
        """)
        # Add the status column if the table already exists but is missing it
        cur.execute("""
            DO $$
            BEGIN
                ALTER TABLE geocoded_tweets ADD COLUMN status TEXT DEFAULT 'reported' NOT NULL;
            EXCEPTION WHEN duplicate_column THEN
                RAISE NOTICE 'Column "status" already exists.';
            END $$;
        """)
    conn.commit()
    print("✅ Database schema is ready.")

def fetch_unprocessed_reports(conn) -> List[Dict]:
    """Fetches final reports that have not yet been geocoded."""
    reports = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT fr.id, fr.event_summary, fr.event_location, fr.number_of_reports
            FROM final_report fr
            LEFT JOIN geocoded_tweets gt ON fr.id = gt.source_report_id
            WHERE gt.source_report_id IS NULL;
        """)
        colnames = [desc[0] for desc in cur.description]
        reports = [dict(zip(colnames, row)) for row in cur.fetchall()]
    print(f"Found {len(reports)} new reports to geocode.")
    return reports

def insert_geocoded_report(conn, report_data: Dict):
    """Inserts a complete, geocoded report into the final table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO geocoded_tweets 
            (source_report_id, latitude, longitude, event_summary, event_location, number_of_reports)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_report_id) DO NOTHING;
            """,
            (
                report_data['id'], report_data['latitude'], report_data['longitude'],
                report_data['event_summary'], report_data['event_location'], report_data['number_of_reports']
            )
        )
    conn.commit()
    print(f"   -> 💾 Geocoded data for report ID #{report_data['id']} saved.")

# --- Main Execution ---
if __name__ == "__main__":
    print("--- Starting Geocoding Workflow for Final Reports ---")
    data_manager = DataManager()
    db_connection = None
    try:
        db_connection = psycopg2.connect(NEON_DATABASE_URL)
        setup_database_schema(db_connection)
        reports_to_process = fetch_unprocessed_reports(db_connection)
        
        if reports_to_process:
            for report in reports_to_process:
                location_name = report['event_location']
                print(f"\n--- Processing Report ID #{report['id']}: Location '{location_name}' ---")
                search_query = f"{location_name}, Vellore"
                location_data = data_manager.get_coordinates(search_query)
                if location_data:
                    report['latitude'] = location_data['lat']
                    report['longitude'] = location_data['lon']
                    insert_geocoded_report(db_connection, report)
                time.sleep(1)
        else:
            print("No new reports to process.")
    except Exception as error:
        print(f"An error occurred: {error}")
    finally:
        if db_connection is not None:
            db_connection.close()
            print("\n--- Workflow Complete. Database connection closed. ---")