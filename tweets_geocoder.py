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
# MODIFIED: Using the same variable name as our other scripts
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
    """Ensures the incident_reports table has latitude and longitude columns."""
    with conn.cursor() as cur:
        # Add columns to store geocoded coordinates if they don't exist
        cur.execute("""
            DO $$
            BEGIN
                ALTER TABLE incident_reports ADD COLUMN latitude DOUBLE PRECISION;
                ALTER TABLE incident_reports ADD COLUMN longitude DOUBLE PRECISION;
                RAISE NOTICE 'Columns "latitude" and "longitude" added to "incident_reports".';
            EXCEPTION
                WHEN duplicate_column THEN
                RAISE NOTICE 'Coordinates columns already exist in "incident_reports".';
            END $$;
        """)
    conn.commit()
    print("✅ Database schema is ready for geocoding.")

def fetch_unprocessed_locations(conn) -> List[str]:
    """
    Fetches unique location names from incident_reports that have not yet been geocoded.
    """
    locations = []
    with conn.cursor() as cur:
        # MODIFIED: This query efficiently finds only the work that needs to be done.
        cur.execute("""
            SELECT DISTINCT extracted_location 
            FROM incident_reports 
            WHERE latitude IS NULL 
            AND extracted_location IS NOT NULL 
            AND extracted_location != 'Not specified';
        """)
        rows = cur.fetchall()
        locations = [row[0] for row in rows]
    print(f"Found {len(locations)} unique locations to geocode.")
    return locations

def update_location_with_coords(conn, location_name: str, lat: float, lon: float):
    """Updates all records with a given location name with the found coordinates."""
    with conn.cursor() as cur:
        # This query updates all matching rows at once
        cur.execute(
            "UPDATE incident_reports SET latitude = %s, longitude = %s WHERE extracted_location = %s",
            (lat, lon, location_name)
        )
    conn.commit()
    print(f"   -> 💾 Database updated for location '{location_name}'.")


# --- Step 5: Main Execution Block ---
if __name__ == "__main__":
    if not NEON_DATABASE_URL or not OPENCAGE_API_KEY:
        print("❌ Error: Make sure NEON_DATABASE_URL and OPENCAGE_API_KEY are set in your .env file.")
    else:
        print("--- Starting Geocoding Workflow ---")
        data_manager = DataManager()
        db_connection = None
        try:
            # 1. Connect to the database
            db_connection = psycopg2.connect(NEON_DATABASE_URL)
            
            # 2. Prepare the database table
            setup_database_schema(db_connection)
            
            # 3. Fetch all unique location names that need geocoding
            places_to_find = fetch_unprocessed_locations(db_connection)
            
            # 4. Loop through each place, get coordinates, and update the DB
            if places_to_find:
                for place in places_to_find:
                    print(f"\n--- Processing: {place} ---")
                    # Append ", Vellore" to make the search more specific and accurate
                    search_query = f"{place}, Vellore"
                    location_data = data_manager.get_coordinates(search_query)
                    
                    if location_data:
                        print(f"   -> Found: {location_data['formatted_address']}")
                        update_location_with_coords(
                            db_connection, 
                            place, 
                            location_data['lat'], 
                            location_data['lon']
                        )
                    
                    # Pause for 1 second to respect the API's free tier rate limit
                    time.sleep(1)
            else:
                print("No new locations to process.")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")
        finally:
            if db_connection is not None:
                db_connection.close()
                print("\n--- Workflow Complete. Database connection closed. ---")