# http://grouplens.org/datasets/movielens/

DROP DATABASE IF EXISTS `movies`;
CREATE DATABASE `movies`;

DROP TABLE IF EXISTS movies.ratings;
DROP TABLE IF EXISTS movies.users;
DROP TABLE IF EXISTS movies.movies;

CREATE TABLE IF NOT EXISTS movies.users (
    userID INT PRIMARY KEY,
    gender char(1),
    age INT,
    occupation TEXT,
    zipcode TEXT 
    );
LOAD DATA LOCAL INFILE 'users.dat' INTO TABLE movies.users FIELDS TERMINATED BY '::';


CREATE TABLE IF NOT EXISTS movies.movies (
    movieID INT PRIMARY KEY NOT NULL,
    title TEXT,
    genres TEXT
    );
LOAD DATA LOCAL INFILE 'movies.dat' INTO TABLE movies.movies FIELDS TERMINATED BY '::';



CREATE TABLE IF NOT EXISTS movies.ratings (
    userID INT ,
    movieID INT ,
    rating INT,
    timestamp INT,
    FOREIGN KEY (userID) REFERENCES users(userID),
    FOREIGN KEY (movieID) REFERENCES movies(movieID)
    );

LOAD DATA LOCAL INFILE 'ratings.dat'
INTO TABLE movies.ratings FIELDS TERMINATED BY '::';

alter table movies
add column aID INT NOT NULL;
set @aID = 0;
UPDATE movies SET aID=(@aID:=@aID+1);

alter table users
add column aID INT NOT NULL;
set @aID = 0;
UPDATE users SET aID=(@aID:=@aID+1);

alter table ratings
add column aID INT NOT NULL;
set @aID = 0;
UPDATE ratings SET aID=(@aID:=@aID+1);

create index aid_index on movies (aID);
create index aid_index on users (aID);
create index aid_index on ratings (aID);

CREATE TABLE IF NOT EXISTS USERS (
    userID INT,
    gender char(1),
    age INT,
    occupation TEXT,
    zipcode TEXT 
    );

CREATE TABLE IF NOT EXISTS MOVIES (
    movieID INT NOT NULL,
    title TEXT,
    genres TEXT
    );

CREATE TABLE IF NOT EXISTS RATINGS (
    userID INT ,
    movieID INT ,
    rating INT,
    timestamp INT
    );
    
