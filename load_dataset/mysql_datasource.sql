DROP DATABASE IF EXISTS love_movies;
CREATE DATABASE love_movies;
USE love_movies;


-- Albums load
DROP TABLE IF EXISTS love_movies.movies;

CREATE TABLE IF NOT EXISTS movies (
    id BIGINT,
    title TEXT,
    vote_average FLOAT,
    vote_count BIGINT,
    release_date VARCHAR(20),
    revenue BIGINT,
    runtime INT,
    adult TEXT,
    backdrop_path TEXT,
    budget BIGINT,
    original_language TEXT,
    original_title TEXT,
    overview TEXT,
    popularity FLOAT,
    poster_path TEXT,
    genres TEXT,
    production_companies TEXT,
    production_countries TEXT,
    spoken_languages TEXT,
    keywords TEXT,
    PRIMARY KEY (id)
);

DROP TABLE IF EXISTS love_movies.genre_track;
CREATE TABLE IF NOT EXISTS genre_track (
    id INTEGER PRIMARY KEY,
    genre_name TEXT
);
