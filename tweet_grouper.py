import os
import json
import time
import numpy as np
from sklearn.cluster import DBSCAN
import google.generativeai as genai
import psycopg2
from psycopg2.extras import Json
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
    print("✅ Configuration loaded successfully.")
    
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

def setup_database_schema(conn):
    """Ensures the final_report table and tracking columns exist."""
    with conn.cursor() as cur:
        cur.execute("""
            DO $$
            BEGIN
                ALTER TABLE incident_reports ADD COLUMN status TEXT DEFAULT 'unprocessed';
            EXCEPTION
                WHEN duplicate_column THEN
                RAISE NOTICE 'Column "status" already exists in "incident_reports".';
            END $$;
        """)
        
        # MODIFIED: Added 'event_location' column
        cur.execute("""
            CREATE TABLE IF NOT EXISTS final_report (
                id SERIAL PRIMARY KEY,
                event_summary TEXT,
                event_location TEXT,
                source_incident_ids BIGINT[],
                number_of_reports INTEGER,
                generated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
    conn.commit()
    print("✅ Database schema for final report is ready.")

def fetch_unprocessed_incidents(conn):
    """Fetches all incident reports that have not yet been grouped."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, original_tweet_text, extracted_location, extracted_issue FROM incident_reports WHERE status = 'unprocessed'")
        
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        incident_list = [dict(zip(colnames, row)) for row in rows]
        
    print(f"Found {len(incident_list)} unprocessed incidents to group.")
    return incident_list

def insert_final_event_report(conn, summary, location, incident_ids):
    """Inserts a new summarized event into the final_report table."""
    with conn.cursor() as cur:
        # MODIFIED: Includes 'event_location' in the INSERT statement
        cur.execute(
            "INSERT INTO final_report (event_summary, event_location, source_incident_ids, number_of_reports) VALUES (%s, %s, %s, %s)",
            (summary, location, incident_ids, len(incident_ids))
        )
    conn.commit()

def update_incident_status(conn, incident_ids):
    """Updates the status of processed incidents to 'grouped'."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE incident_reports SET status = 'grouped' WHERE id IN %s",
            (tuple(incident_ids),)
        )
    conn.commit()


# --- AI & Clustering Functions ---

def get_embeddings(texts):
    """Generates numerical embeddings for a list of texts."""
    try:
        result = genai.embed_content(
            model="models/text-embedding-004",
            content=texts,
            task_type="clustering"
        )
        return np.array(result['embedding'])
    except Exception as e:
        print(f"An error occurred during embedding: {e}")
        return None

def summarize_cluster(incident_texts):
    """Generates a high-level summary and location for a group of related incident texts."""
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    text_blob = "\n- ".join(incident_texts)
    
    # MODIFIED: Prompt now asks for JSON with 'summary' and 'location'
    prompt = f"""
    The following are multiple reports describing the same disaster event.
    Analyze them and generate a single, clear, and concise summary and identify the most specific location for the event.
    Your response MUST be a valid JSON object with two keys: "summary" and "location".

    Example:
    Reports:
    - "Water entering ground floor of houses in Gandhi Nagar."
    - "My friend in Gandhi Nagar says their entire street is underwater."
    Response:
    {{
      "summary": "Multiple reports indicate that houses in Gandhi Nagar are experiencing ground-floor flooding.",
      "location": "Gandhi Nagar"
    }}

    Reports to analyze:
    ---
    - {text_blob}
    ---
    """
    try:
        response = model.generate_content(prompt)
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        result = json.loads(cleaned_response)
        # Return a dictionary with both summary and location
        return {
            "summary": result.get("summary", "Could not generate summary."),
            "location": result.get("location", "Not specified")
        }
    except Exception as e:
        print(f"⚠️ Error during summarization: {e}")
        return {
            "summary": "Could not generate summary.",
            "location": "Error"
        }

# --- Main Execution Logic ---
if __name__ == "__main__":
    print("--- Starting Incident Grouping and Summarization Workflow ---")
    db_connection = get_db_connection()

    if db_connection:
        try:
            setup_database_schema(db_connection)
            incidents = fetch_unprocessed_incidents(db_connection)

            if not incidents:
                print("No new incidents to process.")
            else:
                texts_for_embedding = [f"{inc['extracted_location']}: {inc['extracted_issue']}" for inc in incidents]
                
                print("Generating embeddings for all incidents...")
                embeddings = get_embeddings(texts_for_embedding)
                
                if embeddings is not None:
                    print("Clustering incidents to find groups...")
                    db = DBSCAN(eps=0.5, min_samples=2, metric='cosine').fit(embeddings)
                    labels = db.labels_

                    unique_labels = set(labels)
                    for label in unique_labels:
                        if label == -1:
                            continue

                        cluster_indices = [i for i, l in enumerate(labels) if l == label]
                        cluster_incidents = [incidents[i] for i in cluster_indices]
                        incident_ids_in_cluster = [inc['id'] for inc in cluster_incidents]
                        incident_texts_in_cluster = [inc['original_tweet_text'] for inc in cluster_incidents]
                        
                        print(f"\nFound a cluster (Event #{label}) with {len(cluster_incidents)} related reports.")
                        
                        # MODIFIED: The function now returns a dictionary
                        summary_data = summarize_cluster(incident_texts_in_cluster)
                        event_summary = summary_data["summary"]
                        event_location = summary_data["location"]
                        
                        print(f"   -> Location: {event_location}")
                        print(f"   -> Summary: {event_summary}")
                        
                        # MODIFIED: Pass the new location to the insert function
                        insert_final_event_report(db_connection, event_summary, event_location, incident_ids_in_cluster)
                        update_incident_status(db_connection, incident_ids_in_cluster)
                        print(f"   -> ✅ Saved summary to 'final_report' and updated status for {len(incident_ids_in_cluster)} incidents.")
                        time.sleep(1)
        
        finally:
            db_connection.close()
            print("\n--- Workflow Complete. Database connection closed. ---")