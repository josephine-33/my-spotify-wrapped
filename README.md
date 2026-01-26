# Jo's Spotify Wrapped - Spotify Listening Data Pipeline
An automated data pipeline that ingests my Spotify listening history into a MySQL database for long-term analysis and insights. 
### Coming soon: 
- Queryable UI to filter listening history by artist, date range, and track
- Per-day and per-artist listening summaries (play counts, total time listened)
- Spotify Wrapped-style data visualizations (top artists, top tracks, listening trends over time)

## Project Overview
- Fetches recently played tracks from the Spotify Web API
- Stores listening events and metadata in a normalized relational schema
- Incrementally ingests only new listens to avoid duplication
- Designed for analytics and future visualization (e.g. Spotify Wrapped-style insights)

## Tech Stack
- Python
- Spotify Web API (spotipy)
- MySQL
- OAuth 2.0
- cron
- dotenv / environment variables

## Pipeline Logic
- Authenticate with Spotify via OAuth
- Fetch most recent listening activity
- Filter out previously ingested plays
- Upsert albums, artists, and tracks
- Insert new listen events with timestamps
- Update ingestion state and commit transaction

## Incremental Ingestion
- Maintains a persisted `last_played_at` timestamp 
- Ensures idempotent re-runs of the pipeline
- Safe to execute repeatedly without creating duplicates

## Why I Built This
- I love listening to music and look forward to my Spotify Wrapped every year
- I started wondering how accurate my listening stats really were and wanted to explore the data myself
- Building this project let me combine that curiosity with my interest in tracking habits and activities
- It's been a fun way to dig deeper into my listening trends while practicing backend and data engineering concepts!