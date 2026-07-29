CREATE PROCEDURE dbo.sp_top_routes @top_n INT = 10
AS
BEGIN
    SELECT TOP (@top_n)
        pz.zone_name AS pickup_zone, dz.zone_name AS dropoff_zone,
        COUNT(*) AS trips, SUM(f.total_amount) AS revenue
    FROM lh_mobility.gold.fact_trips f
    JOIN lh_mobility.gold.dim_zone pz ON f.pickup_zone_id = pz.zone_id
    JOIN lh_mobility.gold.dim_zone dz ON f.dropoff_zone_id = dz.zone_id
    GROUP BY pz.zone_name, dz.zone_name
    ORDER BY trips DESC;
END;