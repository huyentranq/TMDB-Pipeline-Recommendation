LOAD DATA LOCAL INFILE '/tmp/dataset/genre_track.csv'
INTO TABLE genre_track
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/dataset/movies.csv'
INTO TABLE movies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- -- --text loading data for movies dataset
-- LOAD DATA LOCAL INFILE '/tmp/dataset/test_movies.csv'
-- INTO TABLE movies
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS;

