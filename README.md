# Disaster Management AI Pipeline

## Introduction

This repository provides a complete pipeline for building a situational awareness system for natural disasters in India. It combines Twitter ingestion, NLP classification, incident grouping, geocoding, and a web dashboard to deliver actionable insights from real-time and offline data.

The system is designed to work with both live and static sample data, and it leverages AI models to extract structured information from unstructured text sources.

A full end-to-end pipeline for disaster response data ingestion, classification, grouping, geocoding, and dashboarding.
This project includes handwritten data sources plus live Twitter ingestion and OpenCage geocoding with Google Gemini AI enrichment.

---

## Repository Contents

- `twitter_moniter_3.py` - Live Twitter monitor (API stream) populates `tweets` table.
- `tweet_classify.py` - Classifies tweets as actionable/noise and writes to `actionable_tweets`.
- `tweet_processer_2.py` - Processes actionable tweets into `incident_reports` using AI.
- `tweet_grouper_3.py` - Groups related incidents and creates `final_report`.
- `geocode_events.py` - Geocodes grouped incidents into `geocoded_tweets`.
- `load_sms_data.py` - Loads SMS JSON file into `sms_reports`.
- `sms_processer.py` - Extracts actionable SMS to `processed_sms_reports`.
- `sms_geocoder_3.py` - Geocodes processed SMS and saves to `geocoded_sms`.
- `api_server.py` - Flask dashboard API serving incidents and POIs.
- `.env.example` - Environment variable template.
- `dashboard.html` - Front-end dashboard for visualization.
- `vellore_flood_tweets.json`, `vellore_floods_sms.json` - sample data files.

---

## Prerequisites

1. **Python 3.11+**
2.**Use `pip` to install required packages**:

```bash
pip install -r requirements.txt
```

(If `requirements.txt` is missing, install manually: `flask`, `psycopg2`, `requests`, `python-dotenv`, `google-generativeai`, `scikit-learn`, `numpy`.)

3. Set up a Neon Postgres database and note the connection URL.
4. Create API keys:
   - Twitter Bearer token
   - Google Gemini API key
   - OpenCage geocoding API key

---

## Setup

1. Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

2. Edit `.env` with your credentials:

- `NEON_DATABASE_URL`
- `TWITTER_BEARER_TOKEN`
- `GOOGLE_API_KEY`
- `OPENCAGE_API_KEY`

3. Ensure `.gitignore` contains `.env` so secrets don’t get committed.

---

## Database Tables (created automatically)

- `tweets`
- `actionable_tweets`
- `incident_reports`
- `final_report`
- `geocoded_tweets`
- `sms_reports`
- `processed_sms_reports`
- `geocoded_sms`

---

## Run Order (recommended)

### 1) Acquire data

- Live Twitter:
  ```bash
  python twitter_moniter_3.py
  ```

- Load SMS data:
  ```bash
  python load_sms_data.py
  ```

### 2) Classify tweets

```bash
python tweet_classify.py
```

### 3) Process events

```bash
python tweet_processer_2.py
python sms_processer.py
```

### 4) Group and geocode

```bash
python tweet_grouper_3.py
python geocode_events.py
python sms_geocoder_3.py
```

### 5) Run the API dashboard

```bash
python api_server.py
```



## Notes

- SMS geocoder uses AI cleaning plus caching; you may switch to other versions if you don’t need Gemini.
- Use offline monitor for testing without Twitter and to avoid API quota consumption.

---


## License

MIT (or your preferred open-source license)