import os
import psycopg2
from flask import Flask, jsonify, send_from_directory
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
app = Flask(__name__)
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")

# --- Database Connection Helper ---
def get_db_connection():
    """Establishes and returns a connection to the Neon database."""
    try:
        conn = psycopg2.connect(NEON_DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

# --- Static Data for Points of Interest (POIs) ---
# In a real application, this would also come from a database table.
STATIC_POIS = [
    {'name': 'Vellore North Police Station', 'type': 'Police', 'location': [12.9251, 79.1367]},
    {'name': 'Vellore South Police Station', 'type': 'Police', 'location': [12.9130, 79.1330]},
    {'name': 'Vellore Fire & Rescue Station', 'type': 'Fire Station', 'location': [12.9205, 79.1415]},
    {'name': 'CMC Hospital', 'type': 'Hospital', 'location': [12.9220, 79.1392]},
    {'name': 'Naruvi Hospitals', 'type': 'Hospital', 'location': [12.9285, 79.1401]}
]

# --- API Endpoints ---

@app.route('/api/incidents')
def get_incidents():
    """API endpoint to fetch all geocoded incidents from the database."""
    incidents = []
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            # Query the final geocoded table
            cur.execute("""
                SELECT 
                    source_report_id, 
                    latitude, 
                    longitude, 
                    event_summary, 
                    event_location 
                FROM geocoded_tweets 
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
            """)
            
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            for row in rows:
                incidents.append(dict(zip(colnames, row)))

            cur.close()
        except Exception as e:
            print(f"Error querying incidents: {e}")
        finally:
            conn.close()
            
    return jsonify(incidents)

@app.route('/api/pois')
def get_pois():
    """API endpoint to return the static list of points of interest."""
    return jsonify(STATIC_POIS)

# --- Serve the HTML Frontend ---

@app.route('/')
def serve_dashboard():
    """Serves the main HTML dashboard file."""
    # Assumes dashboard.html is in the same directory as this script
    return send_from_directory('.', 'dashboard.html')

# --- Main Execution Block ---

if __name__ == '__main__':
    # host='0.0.0.0' makes the server accessible on your local network
    # debug=True allows for auto-reloading when you save changes
    app.run(host='0.0.0.0', port=5000, debug=True)