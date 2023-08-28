DROP TABLE IF EXISTS staging_events; 
DROP TABLE IF EXISTS staging_songs;
DROP TABLE IF EXISTS songplays;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS songs;
DROP TABLE IF EXISTS artists;
DROP TABLE IF EXISTS time;

CREATE TABLE staging_events (    
    artist VARCHAR(MAX),
    auth VARCHAR(MAX),
    first_name VARCHAR(MAX),
    gender VARCHAR(MAX),
    item_in_session INTEGER,
    last_name VARCHAR(MAX),
    length DOUBLE PRECISION,
    level VARCHAR(MAX),
    location VARCHAR(MAX),
    method VARCHAR(MAX),
    page VARCHAR(MAX),
    registration NUMERIC,
    session_id INTEGER,
    song VARCHAR(MAX),
    status INTEGER,
    timestmp BIGINT,
    user_agent VARCHAR(MAX),
    user_id VARCHAR(MAX)
);

CREATE TABLE staging_songs (
    song_id VARCHAR(MAX), 
    num_songs INTEGER, 
    title VARCHAR(MAX), 
    artist_name VARCHAR(MAX), 
    artist_latitude DOUBLE PRECISION,   
    year INTEGER DEFAULT 0, 
    duration DOUBLE PRECISION, 
    artist_id VARCHAR(MAX), 
    artist_longitude DOUBLE PRECISION, 
    artist_location VARCHAR(MAX)     
);

CREATE TABLE songplays (
    songplay_id VARCHAR(MAX), 
    start_time TIMESTAMP, 
    user_id VARCHAR(MAX) SORTKEY, 
    level VARCHAR(MAX), 
    song_id VARCHAR(MAX) DISTKEY, 
    artist_id VARCHAR(MAX), 
    session_id INTEGER, 
    location VARCHAR(MAX), 
    user_agent VARCHAR(MAX)
);

CREATE TABLE users (
    user_id VARCHAR(MAX) SORTKEY, 
    first_name VARCHAR(MAX), 
    last_name VARCHAR(MAX), 
    gender VARCHAR(MAX), 
    level VARCHAR(MAX)
)
DISTSTYLE ALL;

CREATE TABLE songs (
    song_id VARCHAR(MAX) DISTKEY, 
    title VARCHAR(MAX) SORTKEY, 
    artist_id VARCHAR(MAX), 
    year INTEGER, 
    duration DOUBLE PRECISION
    );

CREATE TABLE artists (
    artist_id VARCHAR(MAX) SORTKEY, 
    name VARCHAR(MAX), 
    location VARCHAR(MAX), 
    lattitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION
    )
    DISTSTYLE ALL;

CREATE TABLE time (
    start_time TIMESTAMP DISTKEY, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday BOOLEAN
    );