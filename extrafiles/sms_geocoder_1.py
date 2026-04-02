import os
import requests
import psycopg2
import time
import json
import google.generativeai as genai
from dotenv import load_dotenv
from typing import Union, List, Dict

# --- Step 1: Load Environment Variables ---
load_dotenv()

# --- Step 2: Define Constants and API Information ---
OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY")
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
OPENCAGE_ENDPOINT = "https://api.opencagedata.com/geocode/v1/json"

# Configure Gemini API
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)

# --- Step 3: Define AI and DataManager Functions ---

def clean_location_with_ai(vague_location: str) -> str:
    """Uses Gemini to clean up a messy, user-provided location string."""
    if not GOOGLE_API_KEY:
        print("   -> ⚠️  GOOGLE_API_KEY not found, skipping AI cleaning.")
        return vague_location

    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    prompt = f"""
    You are an expert at refining location descriptions for geocoding APIs.
    Convert the following vague location description from Vellore, Tamil Nadu, into a more structured, searchable address.
    Remove ambiguous terms like "near", "opposite", or "behind". Add ", Vellore, Tamil Nadu" to make the location specific.

    Vague description: "{vague_location}"

    Cleaned description:
    """
    try:
        response = model.generate_content(prompt)
        time.sleep(1) # Pause to respect Gemini API rate limits
        return response.text.strip()
    except Exception as e:
        print(f"   -> ⚠️  AI cleaning failed: {e}")
        return vague_location # Return original text on failure

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
    """Ensures the processed_sms_reports table has latitude and longitude columns."""
    with conn.cursor() as cur:
        # MODIFIED: Alters the correct 'processed_sms_reports' table
        cur.execute("""
            DO $$
            BEGIN
                ALTER TABLE processed_sms_reports ADD COLUMN latitude DOUBLE PRECISION;
                ALTER TABLE processed_sms_reports ADD COLUMN longitude DOUBLE PRECISION;
                RAISE NOTICE 'Columns "latitude" and "longitude" added to "processed_sms_reports".';
            EXCEPTION
                WHEN duplicate_column THEN
                RAISE NOTICE 'Coordinates columns already exist in "processed_sms_reports".';
            END $$;
        """)
    conn.commit()
    print("✅ Database schema is ready for geocoding.")

def fetch_unprocessed_sms_locations(conn) -> List[str]:
    """
    Fetches unique location names from processed_sms_reports that have not yet been geocoded.
    """
    locations = []
    with conn.cursor() as cur:
        # MODIFIED: Reads from the correct table
        cur.execute("""
            SELECT DISTINCT extracted_location 
            FROM processed_sms_reports 
            WHERE latitude IS NULL 
            AND extracted_location IS NOT NULL 
            AND extracted_location != 'Not specified';
        """)
        rows = cur.fetchall()
        locations = [row[0] for row in rows]
    print(f"Found {len(locations)} unique SMS locations to geocode.")
    return locations

def update_sms_location_with_coords(conn, location_name: str, lat: float, lon: float):
    """Updates all records with a given location name with the found coordinates."""
    with conn.cursor() as cur:
        # MODIFIED: Updates the correct table
        cur.execute(
            "UPDATE processed_sms_reports SET latitude = %s, longitude = %s WHERE extracted_location = %s",
            (lat, lon, location_name)
        )
    conn.commit()
    print(f"   -> 💾 Database updated for location '{location_name}'.")


# --- Step 5: Main Execution Block ---
if __name__ == "__main__":
    if not NEON_DATABASE_URL or not OPENCAGE_API_KEY or not GOOGLE_API_KEY:
        print("❌ Error: Make sure all required API keys and URLs are in your .env file.")
    else:
        print("--- Starting Geocoding Workflow for Processed SMS ---")
        data_manager = DataManager()
        db_connection = None
        try:
            db_connection = psycopg2.connect(NEON_DATABASE_URL)
            setup_database_schema(db_connection)
            
            places_to_find = fetch_unprocessed_sms_locations(db_connection)
            
            if places_to_find:
                for place in places_to_find:
                    print(f"\n--- Processing Location: {place} ---")
                    
                    cleaned_location = clean_location_with_ai(place)
                    print(f"   -> Cleaned Location: '{cleaned_location}'")

                    location_data = data_manager.get_coordinates(cleaned_location)
                    
                    if location_data:
                        print(f"   -> Found: {location_data['formatted_address']}")
                        update_sms_location_with_coords(
                            db_connection, 
                            place, 
                            location_data['lat'], 
                            location_data['lon']
                        )
                    
                    time.sleep(1) # Pause to respect OpenCage rate limit
            else:
                print("No new SMS locations to process.")

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"An error occurred: {error}")
        finally:
            if db_connection is not None:
                db_connection.close()
                print("\n--- Workflow Complete. Database connection closed. ---")