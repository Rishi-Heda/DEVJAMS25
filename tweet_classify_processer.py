import os
import json
import time
import psycopg2
import google.generativeai as genai
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

# --- Gemini API Functions ---

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
        print(f"⚠️  An error occurred during detail extraction: {e}")
        return None

def classify_text(text_to_classify: str) -> str:
    """Classifies text as 'Actionable' or 'Noise'."""
    prompt = f"""
    Your task is to classify the following text as either "Actionable" or "Noise".
    Provide the output in JSON format with a single key "classification".

    Actionable data describes a specific, ongoing event or need that requires an immediate response.
    Noise data is a general statement, an opinion, a report of a past event, or a metaphorical use of a word.

    --- EXAMPLES ---
    Text: "There are people stranded on their rooftops in Sainathapuram. They need immediate air rescue." -> {{"classification": "Actionable"}}
    Text: "A huge tree has fallen and blocked the main road in Viruthampet." -> {{"classification": "Actionable"}}
    Text: "The new movie release is expected to see a flood of audience reviews this weekend." -> {{"classification": "Noise"}}
    --- EXAMPLES END ---

    Text: "{text_to_classify}"
    """
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(prompt)
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        result = json.loads(cleaned_response)
        return result.get("classification", "Error")
    except Exception as e:
        print(f"Error during classification: {e}")
        return "Error"

# --- Database Functions ---

def get_db_connection():
    """Establishes and returns a connection to the Neon database."""
    try:
        conn = psycopg2.connect(NEON_CONN_STRING)
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def setup_database_schema(conn):
    """Ensures all necessary tables and columns exist for the entire workflow."""
    with conn.cursor() as cur:
        # 1. Add 'status' column to 'tweets' table for tracking
        cur.execute("""
            DO $$
            BEGIN
                ALTER TABLE tweets ADD COLUMN status TEXT DEFAULT 'unclassified';
                RAISE NOTICE 'Column "status" added to "tweets" table.';
            EXCEPTION
                WHEN duplicate_column THEN
                RAISE NOTICE 'Column "status" already exists in "tweets".';
            END $$;
        """)
        
        # 2. Create table for extracted incident details
        cur.execute("""
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

        # 3. Create table for actionable tweets
        cur.execute("""
            CREATE TABLE IF NOT EXISTS actionable_tweets (
                id SERIAL PRIMARY KEY,
                source_tweet_id BIGINT UNIQUE NOT NULL,
                original_tweet_text TEXT,
                classified_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    conn.commit()
    print("✅ Database schema is ready.")

def fetch_unclassified_tweets(conn):
    """Fetches all tweets with 'unclassified' status."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, text FROM tweets WHERE status = 'unclassified'")
        items = cur.fetchall()
    print(f"Found {len(items)} unclassified tweets to process.")
    return items

def insert_incident_report(conn, report_data):
    """Inserts a single processed incident report into the database."""
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

def insert_actionable_tweet(conn, text_content: str, source_id: int):
    """Inserts a new actionable tweet into the database."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO actionable_tweets (original_tweet_text, source_tweet_id) VALUES (%s, %s) ON CONFLICT (source_tweet_id) DO NOTHING",
            (text_content, source_id)
        )
    conn.commit()

def update_tweet_status(conn, tweet_id: int, new_status: str):
    """Updates the status of a tweet (e.g., to 'processed')."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tweets SET status = %s WHERE id = %s",
            (new_status, tweet_id)
        )
    conn.commit()

# --- Main Execution Logic ---
if __name__ == "__main__":
    print("--- Starting Combined Tweet Processing Workflow ---")
    db_connection = get_db_connection()

    if db_connection:
        try:
            # 1. Prepare database schema
            setup_database_schema(db_connection)
            
            # 2. Fetch all unprocessed tweets
            tweets_to_process = fetch_unclassified_tweets(db_connection)

            if not tweets_to_process:
                print("No new tweets to process.")
            else:
                # 3. Loop through each tweet ONCE
                for i, (tweet_id, tweet_text) in enumerate(tweets_to_process):
                    print(f"\nProcessing Tweet {i+1}/{len(tweets_to_process)} (ID: {tweet_id})")
                    print(f"   Text: '{tweet_text}'")

                    # --- Task 1: Extract Details ---
                    print("   -> Step 1: Extracting details...")
                    extracted_data = extract_incident_details(tweet_text)
                    time.sleep(1) # Pause to respect rate limits

                    if extracted_data:
                        report = {
                            "original_tweet_id": tweet_id,
                            "location": extracted_data.get('location', 'Extraction Failed'),
                            "issue": extracted_data.get('issue', 'Extraction Failed'),
                            "time": extracted_data.get('time', 'Extraction Failed'),
                            "original_tweet_text": tweet_text
                        }
                        insert_incident_report(db_connection, report)
                        print("      -> Details saved to 'incident_reports'.")
                    else:
                        print("      -> Failed to extract details.")

                    # --- Task 2: Classify Tweet ---
                    print("   -> Step 2: Classifying as Actionable/Noise...")
                    classification = classify_text(tweet_text)
                    time.sleep(1) # Pause to respect rate limits
                    
                    print(f"      -> Result: {classification}")

                    if classification == "Actionable":
                        insert_actionable_tweet(db_connection, tweet_text, tweet_id)
                        print(f"      -> ✅ Marked as Actionable and saved.")
                    
                    # 4. Update the status to 'processed' so it's not run again
                    update_tweet_status(db_connection, tweet_id, 'processed')
                    print(f"   -> 🔄 Marked Tweet {tweet_id} as processed.")

        finally:
            # 5. Close the connection
            db_connection.close()
            print("\n--- Workflow Complete. Database connection closed. ---")