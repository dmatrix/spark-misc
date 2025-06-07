CREATE MATERIALIZED VIEW temperature_events_mv AS
SELECT 
    event_id,
    rig_name,
    location,
    region,
    sensor_value as temperature_f,
    timestamp,
    latitude,
    longitude
FROM permian_rig_mv 
WHERE sensor_type = 'temperature'
UNION ALL
SELECT 
    event_id,
    rig_name,
    location,
    region,
    sensor_value as temperature_f,
    timestamp,
    latitude,
    longitude
FROM eagle_ford_rig_mv
WHERE sensor_type = 'temperature'; 