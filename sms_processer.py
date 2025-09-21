import os
import json
import time
import google.generativeai as genai
import psycopg2
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

try:
    api_key = os.getenv('GOOGLE_API_KEY')
    db_url = os.getenv('NEON_DATABASE_URL')
    if not api_key or not db_url:
        raise ValueError("API key or Database URL not found. Make sure your .env file is correct.")
        
    genai.configure(api_key=api_key)
    NEON_CONN_STRING = db_url
    print("✅ Configuration loaded successfully from .env file.")
    
except (KeyError, ValueError) as e:
    print(f"❌ ERROR: {e}")
    exit()

# --- Database Functions ---

def get_db_connection():
    """Establishes and returns a connection to the Neon database."""
    try:
        conn = psycopg2.connect(NEON_CONN_STRING)
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def setup_processed_sms_table(conn):
    """Creates the new 'processed_sms_reports' table if it doesn't exist."""
    print("Ensuring 'processed_sms_reports' table exists...")
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_sms_reports (
                id SERIAL PRIMARY KEY,
                original_sms_id INTEGER UNIQUE NOT NULL,
                extracted_location TEXT,
                extracted_issue TEXT,
                issue_time TEXT,
                original_sms_body TEXT,
                processed_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
    print("✅ Table is ready.")

def fetch_unprocessed_sms(conn):
    """Fetches messages from 'sms_reports' that are not yet in 'processed_sms_reports'."""
    print("Fetching unprocessed SMS messages from the database...")
    with conn.cursor() as cursor:
        query = """
            SELECT s.id, s.message_body
            FROM sms_reports s
            LEFT JOIN processed_sms_reports ps ON s.id = ps.original_sms_id
            WHERE ps.original_sms_id IS NULL;
        """
        cursor.execute(query)
        colnames = [desc[0] for desc in cursor.description]
        records = [dict(zip(colnames, row)) for row in cursor.fetchall()]
        print(f"✅ Found {len(records)} new SMS messages to process.")
        return records

def insert_processed_sms(conn, report_data):
    """Inserts a single processed SMS report into the new table."""
    with conn.cursor() as cursor:
        query = """
            INSERT INTO processed_sms_reports 
            (original_sms_id, extracted_location, extracted_issue, issue_time, original_sms_body)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (original_sms_id) DO NOTHING;
        """
        cursor.execute(query, (
            report_data['original_sms_id'],
            report_data['location'],
            report_data['issue'],
            report_data['time'],
            report_data['original_sms_body']
        ))
        conn.commit()

# --- Gemini API for Extraction ---

def extract_incident_details_from_sms(sms_body):
    """Sends a single SMS body to Gemini to extract structured details."""
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    
    prompt = f"""
    Analyze the following emergency SMS message. Your task is to extract the specific location of the issue, a concise description of the issue, and the time of the issue.

    Your response MUST be a valid JSON object with three keys: "location", "issue", and "time".
    - "location": The specific place mentioned (e.g., "Arcot Road", "Voorhees College shelter"). If no location is mentioned, return "Not specified".
    - "issue": A brief description of the problem (e.g., "People trapped by water", "Need for medicine", "Wall collapse").
    - "time": The time reference from the message (e.g., "Now", "12 hours"). If not mentioned, return "Not specified".

    Here is the SMS message:
    ---
    {sms_body}
    ---
    """
    
    try:
        response = model.generate_content(prompt)
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        return json.loads(cleaned_response)
    except Exception as e:
        print(f"⚠️  An error occurred with the Gemini API: {e}")
        return None

# --- Main Execution Logic ---

if __name__ == "__main__":
    conn = get_db_connection()
    if conn:
        try:
            setup_processed_sms_table(conn)
            sms_to_process = fetch_unprocessed_sms(conn)

            if not sms_to_process:
                print("No new SMS messages to process. The database is up-to-date.")
            else:
                print(f"\n--- Starting to process {len(sms_to_process)} SMS messages ---")
                for i, sms in enumerate(sms_to_process):
                    print(f"Processing SMS {i + 1}/{len(sms_to_process)} (ID: {sms['id']})...")
                    
                    extracted_data = extract_incident_details_from_sms(sms['message_body'])
                    
                    if extracted_data:
                        report = {
                            "original_sms_id": sms['id'],
                            "location": extracted_data.get('location', 'Extraction Failed'),
                            "issue": extracted_data.get('issue', 'Extraction Failed'),
                            "time": extracted_data.get('time', 'Extraction Failed'),
                            "original_sms_body": sms['message_body']
                        }
                        insert_processed_sms(conn, report)
                        print("   -> Successfully processed and saved.")
                    else:
                        print("   -> Failed to process SMS.")

                    # Pause for 2 seconds to stay safely within the 60 requests/minute limit.
                    time.sleep(2)

        finally:
            conn.close()
            print("\n✅ Processing complete. Database connection closed.")