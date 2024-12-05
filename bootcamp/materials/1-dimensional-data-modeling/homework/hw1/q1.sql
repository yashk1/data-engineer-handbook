-- DROP TYPE IF EXISTS films_struct CASCADE;

CREATE TYPE films_struct as(
	filmid TEXT,
	votes INTEGER,
	rating REAL,
	film TEXT
);

CREATE TYPE quality_class as ENUM('star', 'good','average','bad');

CREATE TABLE actors(
	actorid TEXT,
	actor TEXT,
	year INTEGER,
	films films_struct[],
	quality_class quality_class,
	is_active BOOLEAN,
	PRIMARY KEY(actorid, year)
);

