
CREATE SCHEMA IF NOT EXISTS movies;
-- Tạo bảng movies_infor
CREATE TABLE IF NOT EXISTS movies.movies_infor (
    id int PRIMARY KEY,
    title TEXT,
    overview TEXT,
    release_date DATE,
    runtime INT,
    genres TEXT[]  -- Lưu danh sách genre dạng chuỗi phân cách hoặc json nếu cần
);

-- Tạo bảng movies_rating
CREATE TABLE IF NOT EXISTS movies.movies_rating (
    id INT PRIMARY KEY,
    vote_average FLOAT,
    vote_count INT
);

-- Tạo bảng movies_genres
CREATE TABLE IF NOT EXISTS movies.movies_genres (
    id INT PRIMARY KEY,
    genre TEXT[]
);

-- Tạo bảng favorite_track
CREATE TABLE IF NOT EXISTS movies.favorite_track (
    id SERIAL PRIMARY KEY,
    title TEXT,
    adult TEXT,
    popularity FLOAT,
    overview TEXT,
    release_date DATE,
    vote_average FLOAT,
    vote_count INT,
    genres TEXT[] -- Lưu dạng chuỗi phân cách hoặc JSON
);

-- Tạo bảng recommendations
CREATE TABLE IF NOT EXISTS movies.recommendations (
    id BIGINT PRIMARY KEY,
    cosine_similarity FLOAT
);
