CREATE TABLE IF NOT EXISTS air_quality (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    country VARCHAR(50),
    aqi INT,
    aqi_timestamp TIMESTAMP,
    remarks VARCHAR(50),
    UNIQUE(city, aqi_timestamp)
);