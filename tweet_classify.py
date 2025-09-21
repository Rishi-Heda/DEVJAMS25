import os
import json
import psycopg2
import google.generativeai as genai
from dotenv import load_dotenv

def classify_text(text_to_classify: str) -> str:
    """
    Classifies text using a few-shot prompt with specific disaster-related examples.
    """
    prompt = f"""
    Your task is to classify the following text as either "Actionable" or "Noise".
    Provide the output in JSON format with a single key "classification".

    Actionable data describes a specific, ongoing event or need that requires an immediate response.
    Noise data is a general statement, an opinion, a report of a past event, or a metaphorical use of a word.

    --- EXAMPLES START ---
    Text: "There are about 150 people at Voorhees College shelter. They need food and dry clothes."
    {{
      "classification": "Actionable"
    }}

    Text: "My uncle is a farmer near Gudiyatham. He says his entire crop is destroyed. This is a tragedy for our farmers."
    {{
      "classification": "Noise"
    }}

    Text: "There are people stranded on their rooftops in Sainathapuram. They need immediate air rescue."
    {{
      "classification": "Actionable"
    }}

    Text: "A huge tree has fallen and blocked the main road in Viruthampet."
    {{
      "classification": "Actionable"
    }}

    Text: "The new movie release is expected to see a flood of audience reviews this weekend."
    {{
      "classification": "Noise"
    }}
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

# --- Database Functions (Modified for Compatibility) ---

def get_db_connection():
    """Establishes a connection to the Neon database."""
    try:
        conn = psycopg2.connect(os.getenv("NEON_DATABASE_URL"))
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return None

def setup_database_schema(conn):
    """Ensures the necessary tables and columns exist."""
    with conn.cursor() as cur:
        # 1. Add a 'status' column to the 'tweets' table if it doesn't exist
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
        
        # 2. Create the new table for actionable tweets
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
    """Fetches all rows from the 'tweets' table with 'unclassified' status."""
    items = []
    with conn.cursor() as cur:
        # MODIFIED: Targets the 'tweets' table and 'text' column
        cur.execute("SELECT id, text FROM tweets WHERE status = 'unclassified'")
        items = cur.fetchall()
    print(f"Found {len(items)} unclassified tweets to process.")
    return items

def insert_actionable_tweet(conn, text_content: str, source_id: int):
    """Inserts a new actionable tweet into the database."""
    with conn.cursor() as cur:
        # MODIFIED: Inserts into the new 'actionable_tweets' table
        cur.execute(
            "INSERT INTO actionable_tweets (original_tweet_text, source_tweet_id) VALUES (%s, %s) ON CONFLICT (source_tweet_id) DO NOTHING",
            (text_content, source_id)
        )
    conn.commit()

def update_tweet_status(conn, tweet_id: int):
    """Updates the status of a tweet to 'classified'."""
    with conn.cursor() as cur:
        # MODIFIED: Updates the 'tweets' table
        cur.execute(
            "UPDATE tweets SET status = 'classified' WHERE id = %s",
            (tweet_id,)
        )
    conn.commit()

# --- Main execution block (Modified for Compatibility) ---
if __name__ == "__main__":
    load_dotenv()
    # MODIFIED: Standardized API key variable name
    genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

    print("--- Starting Tweet Classification Workflow ---")
    db_connection = get_db_connection()

    if db_connection:
        try:
            # 1. Prepare database schema (adds status column, creates new table)
            setup_database_schema(db_connection)
            
            # 2. Fetch unclassified data from the 'tweets' table
            tweets_to_process = fetch_unclassified_tweets(db_connection)

            if not tweets_to_process:
                print("No new tweets to process.")
            else:
                # 3. Loop, Classify, and Insert
                for tweet_id, tweet_text in tweets_to_process:
                    print(f"\nProcessing Tweet ID {tweet_id}: '{tweet_text}'")
                    classification = classify_text(tweet_text)
                    print(f"   -> Result: {classification}")

                    if classification == "Actionable":
                        insert_actionable_tweet(db_connection, tweet_text, tweet_id)
                        print(f"   -> ✅ Inserted into actionable_tweets table.")
                    
                    # 4. Update the status in the source 'tweets' table
                    update_tweet_status(db_connection, tweet_id)
                    print(f"   -> 🔄 Marked Tweet {tweet_id} as classified.")
        finally:
            # 5. Close the connection
            db_connection.close()
            print("\n--- Workflow Complete. Database connection closed. ---")