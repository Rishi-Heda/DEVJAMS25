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
    """Creates the new 'geocoded_sms_data' table if it doesn't exist."""
    with conn.cursor() as cur:
        # MODIFIED: Creates the final destination table for geocoded SMS data.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geocoded_sms_data (
                id SERIAL PRIMARY KEY,
                source_sms_id INTEGER UNIQUE NOT NULL,
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
    print("✅ Database schema is ready for geocoding SMS data.")

def fetch_unprocessed_sms(conn) -> List[Dict]:
    """
    Fetches processed SMS reports that have not yet been geocoded.
    """
    sms_reports = []
    with conn.cursor() as cur:
        # MODIFIED: Reads from 'processed_sms_reports' and checks against 'geocoded_sms_data'
        cur.execute("""
            SELECT 
                psr.original_sms_id,
                psr.extracted_location,
                psr.extracted_issue,
                psr.issue_time,
                psr.original_sms_body
            FROM processed_sms_reports psr
            LEFT JOIN geocoded_sms_data gsd ON psr.original_sms_id = gsd.source_sms_id
            WHERE gsd.source_sms_id IS NULL
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
        # MODIFIED: Inserts into the new 'geocoded_sms_data' table.
        cur.execute(
            """
            INSERT INTO geocoded_sms_data 
            (source_sms_id, latitude, longitude, extracted_location, extracted_issue, issue_time, original_sms_body)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_sms_id) DO NOTHING;
            """,
            (
                sms_data['original_sms_id'],
                sms_data['latitude'],
                sms_data['longitude'],
                sms_data['extracted_location'],
                sms_data['extracted_issue'],
                sms_data['issue_time'],
                sms_data['original_sms_body']
            )
        )
    conn.commit()
    print(f"   -> 💾 Geocoded data for SMS ID #{sms_data['original_sms_id']} saved.")


# --- Step 5: Main Execution Block ---
if __name__ == "__main__":
    if not NEON_DATABASE_URL or not OPENCAGE_API_KEY:
        print("❌ Error: Make sure NEON_DATABASE_URL and OPENCAGE_API_KEY are set in your .env file.")
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
                    print(f"\n--- Processing SMS ID #{sms['original_sms_id']}: Location '{location_name}' ---")
                    
                    search_query = f"{location_name}, Vellore, India"
                    location_data = data_manager.get_coordinates(search_query)
                    
                    if location_data:
                        print(f"   -> Found: {location_data['formatted_address']}")
                        # Add the new lat/lon to the existing sms data
                        sms['latitude'] = location_data['lat']
                        sms['longitude'] = location_data['lon']
                        
                        # Insert the complete record into the new table
                        insert_geocoded_sms(db_connection, sms)
                    
                    time.sleep(1) # Pause to respect API rate limits
            else:
                print("No new SMS reports to process.")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")
        finally:
            if db_connection is not None:
                db_connection.close()
                print("\n--- Workflow Complete. Database connection closed. ---")