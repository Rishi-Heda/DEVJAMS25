import os
import json
import time
import google.generativeai as genai
import psycopg2
from psycopg2 import extras
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

def setup_results_table(conn):
    """Creates the new 'incident_reports' table if it doesn't exist."""
    print("Ensuring 'incident_reports' table exists...")
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS incident_reports (
                id SERIAL PRIMARY KEY,
                original_tweet_id BIGINT UNIQUE NOT NULL,
                extracted_location TEXT,
                extracted_issue TEXT,
                issue_time TEXT,
                original_tweet_text TEXT,
                processed_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
    print("✅ Table is ready.")

def fetch_unprocessed_tweets(conn):
    """Fetches tweets from the 'tweets' table that are not yet in 'incident_reports'."""
    print("Fetching unprocessed tweets from the database...")
    with conn.cursor() as cursor:
        query = """
            SELECT t.id, t.text, t.created_at
            FROM tweets t
            LEFT JOIN incident_reports ir ON t.id = ir.original_tweet_id
            WHERE ir.original_tweet_id IS NULL;
        """
        cursor.execute(query)
        colnames = [desc[0] for desc in cursor.description]
        records = [dict(zip(colnames, row)) for row in cursor.fetchall()]
        print(f"✅ Found {len(records)} new tweets to process.")
        return records

def insert_incident_report(conn, report_data):
    """Inserts a single processed incident report into the new table."""
    with conn.cursor() as cursor:
        query = """
            INSERT INTO incident_reports 
            (original_tweet_id, extracted_location, extracted_issue, issue_time, original_tweet_text)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (original_tweet_id) DO NOTHING;
        """
        cursor.execute(query, (
            report_data['original_tweet_id'],
            report_data['location'],
            report_data['issue'],
            report_data['time'],
            report_data['original_tweet_text']
        ))
        conn.commit()

# --- Gemini API for Extraction ---

def extract_incident_details(tweet_text):
    """Sends a single tweet's text to Gemini to extract structured details."""
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    
    prompt = f"""
    Analyze the following tweet about a potential disaster. Your task is to extract the specific location of the issue, a concise description of the issue, and the time of the issue.

    Your response MUST be a valid JSON object with three keys: "location", "issue", and "time".
    - "location": The specific place mentioned (e.g., "Katpadi Road", "Gandhi Nagar"). If no location is mentioned, return "Not specified".
    - "issue": A brief description of the problem (e.g., "Severe waterlogging", "Power cut", "Bridge collapse").
    - "time": The time reference from the tweet (e.g., "Overnight", "Last hour", "Now"). If not mentioned, return "Not specified".

    Here is the tweet:
    ---
    {tweet_text}
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
            setup_results_table(conn)
            tweets_to_process = fetch_unprocessed_tweets(conn)

            if not tweets_to_process:
                print("No new tweets to process. The database is up-to-date.")
            else:
                print(f"\n--- Starting to process {len(tweets_to_process)} tweets ---")
                for i, tweet in enumerate(tweets_to_process):
                    print(f"Processing tweet {i + 1}/{len(tweets_to_process)} (ID: {tweet['id']})...")
                    
                    extracted_data = extract_incident_details(tweet['text'])
                    
                    if extracted_data:
                        report = {
                            "original_tweet_id": tweet['id'],
                            "location": extracted_data.get('location', 'Extraction Failed'),
                            "issue": extracted_data.get('issue', 'Extraction Failed'),
                            "time": extracted_data.get('time', 'Extraction Failed'),
                            "original_tweet_text": tweet['text']
                        }
                        insert_incident_report(conn, report)
                        print("   -> Successfully processed and saved.")
                    else:
                        print("   -> Failed to process tweet.")

                    # Pause for 2 seconds to stay safely within the 60 requests/minute limit.
                    time.sleep(2)

        finally:
            conn.close()
            print("\n✅ Processing complete. Database connection closed.")