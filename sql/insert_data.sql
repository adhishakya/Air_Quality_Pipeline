INSERT INTO air_quality(city, country, aqi, aqi_timestamp, remarks)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (city, aqi_timestamp) DO NOTHING;