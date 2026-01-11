import mysql.connector
from mysql.connector import errorcode
import os


def create_tables(cursor):
    cursor.execute(artists_table)
    cursor.execute(albums_table)
    cursor.execute(tracks_table)
    cursor.execute(track_artists_table)
    cursor.execute(audio_features_table)
    cursor.execute(listens_table)
    cursor.execute(ingestion_table)
    cursor.execute(index1)
    cursor.execute(index2)


artists_table = f"""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(32) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        popularity TINYINT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

albums_table = """
    CREATE TABLE IF NOT EXISTS albums (
        album_id VARCHAR(32) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        release_date DATE,
        total_tracks INT,
        album_type ENUM('album', 'single', 'compilation'),
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""


tracks_table = f"""
    CREATE TABLE IF NOT EXISTS tracks (
        track_id VARCHAR(32) PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        album_id VARCHAR(32),
        duration_ms INT,
        explicit BOOLEAN,
        popularity TINYINT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (album_id) REFERENCES albums(album_id)
            ON DELETE SET NULL ON UPDATE CASCADE
    );
"""

track_artists_table = f"""
    CREATE TABLE IF NOT EXISTS track_artists (
        track_id VARCHAR(32),
        artist_id VARCHAR(32),
        artist_order TINYINT,

        PRIMARY KEY (track_id, artist_id),
        FOREIGN KEY (track_id) REFERENCES tracks(track_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    );
"""

audio_features_table = f"""
    CREATE TABLE IF NOT EXISTS audio_features (
        track_id VARCHAR(32) PRIMARY KEY,

        danceability FLOAT,
        energy FLOAT,
        key_val TINYINT,
        loudness FLOAT,
        mode TINYINT,
        speechiness FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        liveness FLOAT,
        valence FLOAT,
        tempo FLOAT,
        time_signature TINYINT,

        FOREIGN KEY (track_id) REFERENCES tracks(track_id)
    );
"""

listens_table = f"""
    CREATE TABLE listens (
        listen_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        track_id VARCHAR(32) NOT NULL,
        played_at TIMESTAMP NOT NULL,
        ms_played INT,

        FOREIGN KEY (track_id) REFERENCES tracks(track_id),
        UNIQUE (track_id, played_at)
    );
"""

ingestion_table = f"""
    CREATE TABLE ingestion_state (
        id TINYINT PRIMARY KEY DEFAULT 1,
        last_played_at TIMESTAMP NOT NULL
    );
"""

# indexes
index1 = f"""CREATE INDEX idx_listens_played_at ON listens(played_at);"""
index2 = f"""CREATE INDEX idx_listens_track ON listens(track_id);"""

if __name__ == "__main__":
    pwd = os.environ.get('MYSQL_PWD')
    user = os.environ.get('MYSQL_USER')
    db = mysql.connector.connect(
        host="localhost",
        user=user,
        password=pwd
    )
    cursor = db.cursor()
    cursor.execute("USE spotify_stats_2026")
    create_tables(cursor)
    cursor.execute("SHOW TABLES")
    result = cursor.fetchall()
    print(result)