CREATE MATERIALIZED VIEW water_level_events_mv AS
SELECT 
    event_id,
    rig_name,
    location,
    region,
    sensor_value as water_level_feet,
    timestamp,
    latitude,
    longitude
FROM permian_rig_mv 
WHERE sensor_type = 'water_level'
UNION ALL
SELECT 
    event_id,
    rig_name,
    location,
    region,
    sensor_value as water_level_feet,
    timestamp,
    latitude,
    longitude
FROM eagle_ford_rig_mv
WHERE sensor_type = 'water_level'; 