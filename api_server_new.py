import os
import psycopg2
from flask import Flask, jsonify, send_from_directory, request
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
app = Flask(__name__)
NEON_DATABASE_URL = os.getenv("NEON_DATABASE_URL")

# --- Database Connection Helper ---
def get_db_connection():
    try:
        conn = psycopg2.connect(NEON_DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

# --- Static Data for POIs ---
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
    """API endpoint to fetch all active geocoded incidents."""
    incidents = []
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            # --- FIX 1: Querying the correct table 'geocoded_tweets' ---
            cur.execute("""
                SELECT 
                    source_report_id, latitude, longitude, 
                    event_summary, event_location, status
                FROM geocoded_tweets 
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND status != 'completed';
            """)
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            for row in rows:
                incidents.append(dict(zip(colnames, row)))
            cur.close()
        finally:
            conn.close()
    return jsonify(incidents)

@app.route('/api/pois')
def get_pois():
    return jsonify(STATIC_POIS)

@app.route('/api/incidents/<int:report_id>/dispatch', methods=['POST'])
def dispatch_incident(report_id):
    """Toggles the status of an incident between 'reported' and 'dispatched'."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM geocoded_tweets WHERE source_report_id = %s", (report_id,))
            current_status = cur.fetchone()[0]
            new_status = 'reported' if current_status == 'dispatched' else 'dispatched'
            cur.execute(
                "UPDATE geocoded_tweets SET status = %s WHERE source_report_id = %s",
                (new_status, report_id)
            )
            conn.commit()
            cur.close()
            return jsonify({'success': True, 'new_status': new_status})
        finally:
            conn.close()
    return jsonify({'success': False, 'error': 'Database connection failed'}), 500

@app.route('/api/incidents/<int:report_id>/complete', methods=['POST'])
def complete_incident(report_id):
    """Marks an incident as 'completed'."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE geocoded_tweets SET status = 'completed' WHERE source_report_id = %s",
                (report_id,)
            )
            conn.commit()
            cur.close()
            return jsonify({'success': True})
        finally:
            conn.close()
    return jsonify({'success': False, 'error': 'Database connection failed'}), 500

# --- Serve the HTML Frontend ---
@app.route('/')
def serve_dashboard():
    # --- FIX 2: Serving the correct HTML file 'websiteatt7.html' ---
    return send_from_directory('.', 'dashboard_new.html')

# --- Main Execution Block ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)