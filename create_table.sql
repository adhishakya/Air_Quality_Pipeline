CREATE TABLE air_quality(
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(100),
    aqi NUMERIC,
    weather_timestamp TIMESTAMP,
    remarks VARCHAR(100)
);