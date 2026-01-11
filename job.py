# this should run every hour
# fetch the tracks played since previous timestamp, max 50
# store in database
# store new timestamp
from dotenv import load_dotenv
load_dotenv()

import os
import mysql.connector
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timezone

# spotify setup

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REDIRECT_URI = 'http://127.0.0.1:8000'

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope='user-read-recently-played',
        cache_path='.spotifycache'
    )
)

# database helpers

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PWD"],
        database="spotify"
    )

# SQL statements

GET_LAST_PLAYED = """
SELECT last_played_at
FROM ingestion_state
WHERE id=1;
"""

INSERT_TRACK = """
INSERT INTO tracks (track_id, name, duration_ms, explicit, popularity)
VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    popularity = VALUES(popularity);
"""

INSERT_LISTEN = """
INSERT INTO listens (track_id, played_at, ms_played)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
    ms_played = VALUES(ms_played);
"""

UPDATE_LAST_PLAYED = """
UPDATE ingestion_state
SET last_played_at = %s
WHERE id = 1;
"""

# main logic
def main():
    conn = get_db_connection()
    cursor = conn.cursor()

    # timestamp of last ingested track
    cursor.execute(GET_LAST_PLAYED)
    row = cursor.fetchone()
    last_played_at = row[0] if row else None

    if last_played_at is not None:
        last_played_at = last_played_at.replace(tzinfo=timezone.utc)

    print("(debug) last ingested:", last_played_at)

    # fetch recent plays
    recent = sp.current_user_recently_played(limit=50)

    new_items = []

    for item in recent["items"]:
        played_at = datetime.fromisoformat(
            item["played_at"].replace("Z", "+00:00")
        )

        if last_played_at is None or played_at > last_played_at:
            new_items.append((item, played_at))

    print("(debug) new listens found:", len(new_items))


    # insert data
    for item, played_at in new_items:
        track = item["track"]

        # insert track
        cursor.execute(
            INSERT_TRACK,
            (
                track["id"],
                track["name"],
                track["duration_ms"],
                track["explicit"],
                track["popularity"],
            ),
        )

        # insert listen
        cursor.execute(
            INSERT_LISTEN,
            (
                track["id"],
                played_at,
                item.get("ms_played"),
            ),
        )

    # update ingestion state
    if new_items:
        newest_played_at = max(p for _, p in new_items)
    cursor.execute(UPDATE_LAST_PLAYED, (newest_played_at,))

    conn.commit()
    cursor.close()
    conn.close()

    print("(debug) ingestion complete")


if __name__ == "__main__":
    main()
