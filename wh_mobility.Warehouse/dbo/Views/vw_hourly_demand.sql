-- Auto Generated (Do not modify) D36D6CF8244370645C46768A4A9A1A81D74BFD73C1C1C2A1FE733542FF69C233
CREATE VIEW dbo.vw_hourly_demand AS
SELECT date_key, pickup_hour, COUNT(*) AS trips,
       AVG(CAST(trip_duration_min AS FLOAT)) AS avg_duration_min
FROM lh_mobility.gold.fact_trips
GROUP BY date_key, pickup_hour;