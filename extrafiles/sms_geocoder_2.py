import os
import requests
import psycopg2
import time
from dotenv import load_dotenv
from typing import Union, List, Dict

# --- Step 1: Load Environment Variables ---
load_dotenv()

# --- Step 2: Define Constants and API Information ---
OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY")
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
OPENCAGE_ENDPOINT = "https://api.opencagedata.com/geocode/v1/json"


# --- Step 3: Define the DataManager Class ---
class DataManager:
    """Manages API calls for geocoding."""
    def get_coordinates(self, place_name: str) -> Union[dict, None]:
        """Takes a place name and returns its coordinates using OpenCage API."""
        if not OPENCAGE_API_KEY:
            print("❌ Error: OPENCAGE_API_KEY not found in .env file.")
            return None
            
        params = {'q': place_name, 'key': OPENCAGE_API_KEY, 'limit': 1}
        try:
            response = requests.get(OPENCAGE_ENDPOINT, params=params)
            response.raise_for_status()
            data = response.json()

            if data['results']:
                result = data['results'][0]
                return {
                    "lat": result['geometry']['lat'],
                    "lon": result['geometry']['lng'],
                    "formatted_address": result['formatted']
                }
            else:
                print(f"   -> ⚠️  Warning: Could not find coordinates for '{place_name}'.")
                return None
        except requests.exceptions.RequestException as e:
            print(f"   -> ❌ An API request error occurred: {e}")
            return None

# --- Step 4: Define the Database Functions ---

def setup_database_schema(conn):
    """Creates the new 'geocoded_sms' table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geocoded_sms (
                id SERIAL PRIMARY KEY,
                source_processed_report_id INTEGER UNIQUE NOT NULL,
                sender_number TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                extracted_location TEXT,
                extracted_issue TEXT,
                issue_time TEXT,
                original_sms_body TEXT,
                geocoded_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    conn.commit()
    print("✅ Database schema with 'geocoded_sms' table is ready.")

def fetch_unprocessed_sms(conn) -> List[Dict]:
    """
    Fetches processed SMS reports that have not yet been geocoded and gets the sender number.
    """
    sms_reports = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                psr.id AS source_processed_report_id,
                psr.extracted_location,
                psr.extracted_issue,
                psr.issue_time,
                psr.original_sms_body,
                sr.sender_number
            FROM processed_sms_reports psr
            JOIN sms_reports sr ON psr.original_sms_id = sr.id
            LEFT JOIN geocoded_sms gs ON psr.id = gs.source_processed_report_id
            WHERE gs.source_processed_report_id IS NULL
            AND psr.extracted_location IS NOT NULL
            AND psr.extracted_location != 'Not specified';
        """)
        
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        sms_reports = [dict(zip(colnames, row)) for row in rows]
        
    print(f"Found {len(sms_reports)} new SMS reports to geocode.")
    return sms_reports

def insert_geocoded_sms(conn, sms_data: Dict):
    """Inserts a complete, geocoded SMS record into the final table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO geocoded_sms 
            (source_processed_report_id, sender_number, latitude, longitude, extracted_location, extracted_issue, issue_time, original_sms_body)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_processed_report_id) DO NOTHING;
            """,
            (
                sms_data['source_processed_report_id'],
                sms_data['sender_number'],
                sms_data['latitude'],
                sms_data['longitude'],
                sms_data['extracted_location'],
                sms_data['extracted_issue'],
                sms_data['issue_time'],
                sms_data['original_sms_body']
            )
        )
    conn.commit()
    print(f"   -> 💾 Geocoded data for report ID #{sms_data['source_processed_report_id']} saved.")


# --- Step 5: Main Execution Block ---
if __name__ == "__main__":
    # MODIFIED: Removed GOOGLE_API_KEY check
    if not NEON_DATABASE_URL or not OPENCAGE_API_KEY:
        print("❌ Error: Make sure NEON_DATABASE_URL and OPENCAGE_API_KEY are in your .env file.")
    else:
        print("--- Starting SMS Geocoding Workflow ---")
        data_manager = DataManager()
        db_connection = None
        try:
            db_connection = psycopg2.connect(NEON_DATABASE_URL)
            setup_database_schema(db_connection)
            
            sms_to_process = fetch_unprocessed_sms(db_connection)
            
            if sms_to_process:
                for sms in sms_to_process:
                    location_name = sms['extracted_location']
                    print(f"\n--- Processing Report ID #{sms['source_processed_report_id']}: Location '{location_name}' ---")
                    
                    # MODIFIED: The AI cleaning step has been removed.
                    # We now send the raw location directly to the geocoder.
                    search_query = f"{location_name}, Vellore, India"
                    print(f"   -> Geocoding query: '{search_query}'")
                    location_data = data_manager.get_coordinates(search_query)
                    
                    if location_data:
                        print(f"   -> Found: {location_data['formatted_address']}")
                        sms['latitude'] = location_data['lat']
                        sms['longitude'] = location_data['lon']
                        
                        insert_geocoded_sms(db_connection, sms)
                    
                    time.sleep(1) # Pause to respect OpenCage rate limit
            else:
                print("No new SMS reports to process.")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")
        finally:
            if db_connection is not None:
                db_connection.close()
                print("\n--- Workflow Complete. Database connection closed. ---")