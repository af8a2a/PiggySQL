CREATE TABLE genres (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE studios (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL
);


CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    title VARCHAR NOT NULL,
    studio_id INTEGER NOT NULL ,
    genre_id INTEGER NOT NULL ,
    released INTEGER NOT NULL,
    rating INTEGER
);

INSERT INTO genres VALUES
    (1, 'Science Fiction'),
    (2, 'Action'),
    (3, 'Drama'),
    (4, 'Comedy');

INSERT INTO studios VALUES
    (1, 'Mosfilm'),
    (2, 'Lionsgate'),
    (3, 'StudioCanal'),
    (4, 'Warner Bros'),
    (5, 'Focus Features');


INSERT INTO movies VALUES
    (1,  'Stalker',             1, 1, 1979, 8),
    (2,  'Sicario',             2, 2, 2015, 7),
    (3,  'Primer',              3, 1, 2004, 6),
    (4,  'Heat',                4, 2, 1995, 8),
    (5,  'The Fountain',        4, 1, 2006, 7),
    (6,  'Solaris',             1, 1, 1972, 8),
    (7,  'Gravity',             4, 1, 2013, 7),
    (8,  '21 Grams',            5, 3, 2003, 7),
    (9,  'Birdman',             4, 4, 2014, 7),
    (10, 'Inception',           4, 1, 2010, 8),
    (11, 'Lost in Translation', 5, 4, 2003, 7),
    (12, 'Eternal Sunshine of the Spotless Mind', 5, 3, 2004, 8);
