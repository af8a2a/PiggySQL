BEGIN;
SELECT * FROM movies;
SELECT title, rating FROM movies WHERE released >= 2000 ORDER BY rating DESC LIMIT 3;
SELECT m.id, m.title, g.name FROM movies m JOIN genres g ON m.genre_id = g.id LIMIT 4;
-- UPDATE movies SET rating = rating+1;
UPDATE movies SET rating = 1;
COMMIT;