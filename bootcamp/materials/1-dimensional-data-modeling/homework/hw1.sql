CREATE TABLE actors (
    actor_id text PRIMARY KEY, -- Unique identifier for the actor
    name VARCHAR(255) NOT NULL, -- Actor's name
    films JSONB NOT NULL, -- JSONB to store array of film details
    quality_class VARCHAR(20), -- Actor's performance quality class
    is_active BOOLEAN, -- Indicates whether the actor is currently active
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp of last update
);

INSERT INTO actors (actor_id, name, films, quality_class, is_active)
SELECT
    af.actorid AS actor_id,
    af.actor AS name,
    JSONB_AGG(
        JSONB_BUILD_OBJECT(
            'film', af.film,
            'votes', af.votes,
            'rating', af.rating,
            'filmid', af.filmid,
            'release_year', af.year
        )
    ) AS films,
    CASE
        WHEN AVG(af.rating) > 8 THEN 'star'
        WHEN AVG(af.rating) > 7 THEN 'good'
        WHEN AVG(af.rating) > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    MAX(af.year) = EXTRACT(YEAR FROM CURRENT_DATE) AS is_active
FROM
    actor_films af
GROUP BY
    af.actorid, af.actor;
