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
    """Creates the new 'geocoded_tweets' table if it doesn't exist."""
    with conn.cursor() as cur:
        # MODIFIED: Creates a new, separate table for the final geocoded data.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geocoded_tweets (
                id SERIAL PRIMARY KEY,
                source_report_id INTEGER UNIQUE NOT NULL,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                event_summary TEXT,
                event_location TEXT,
                number_of_reports INTEGER,
                geocoded_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    conn.commit()
    print("✅ Database schema with 'geocoded_tweets' table is ready.")

def fetch_unprocessed_reports(conn) -> List[Dict]:
    """
    Fetches final reports that have not yet been geocoded.
    """
    reports = []
    with conn.cursor() as cur:
        # MODIFIED: Reads from 'final_report' and checks against 'geocoded_tweets'.
        cur.execute("""
            SELECT 
                fr.id,
                fr.event_summary,
                fr.event_location,
                fr.number_of_reports
            FROM final_report fr
            LEFT JOIN geocoded_tweets gt ON fr.id = gt.source_report_id
            WHERE gt.source_report_id IS NULL
            AND fr.event_location IS NOT NULL
            AND fr.event_location != 'Not specified';
        """)
        
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        reports = [dict(zip(colnames, row)) for row in rows]
        
    print(f"Found {len(reports)} new final reports to geocode.")
    return reports

def insert_geocoded_report(conn, report_data: Dict):
    """Inserts a complete, geocoded report into the final table."""
    with conn.cursor() as cur:
        # MODIFIED: Inserts into the new 'geocoded_tweets' table.
        cur.execute(
            """
            INSERT INTO geocoded_tweets 
            (source_report_id, latitude, longitude, event_summary, event_location, number_of_reports)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_report_id) DO NOTHING;
            """,
            (
                report_data['id'],
                report_data['latitude'],
                report_data['longitude'],
                report_data['event_summary'],
                report_data['event_location'],
                report_data['number_of_reports']
            )
        )
    conn.commit()
    print(f"   -> 💾 Geocoded data for report ID #{report_data['id']} saved.")


# --- Step 5: Main Execution Block ---
if __name__ == "__main__":
    if not NEON_DATABASE_URL or not OPENCAGE_API_KEY:
        print("❌ Error: Make sure NEON_DATABASE_URL and OPENCAGE_API_KEY are set in your .env file.")
    else:
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
                    
                    # Append ", Vellore" to make the search more specific and accurate
                    search_query = f"{location_name}, Vellore"
                    location_data = data_manager.get_coordinates(search_query)
                    
                    if location_data:
                        print(f"   -> Found: {location_data['formatted_address']}")
                        # Add the new lat/lon to the existing report data
                        report['latitude'] = location_data['lat']
                        report['longitude'] = location_data['lon']
                        
                        # Insert the complete record into the new table
                        insert_geocoded_report(db_connection, report)
                    
                    time.sleep(1) # Pause to respect API rate limits
            else:
                print("No new final reports to process.")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")
        finally:
            if db_connection is not None:
                db_connection.close()
                print("\n--- Workflow Complete. Database connection closed. ---")