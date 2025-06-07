CREATE MATERIALIZED VIEW pressure_events_mv AS
SELECT 
    event_id,
    rig_name,
    location,
    region,
    sensor_value as pressure_psi,
    timestamp,
    latitude,
    longitude
FROM permian_rig_mv 
WHERE sensor_type = 'pressure'
UNION ALL
SELECT 
    event_id,
    rig_name,
    location,
    region,
    sensor_value as pressure_psi,
    timestamp,
    latitude,
    longitude
FROM eagle_ford_rig_mv
WHERE sensor_type = 'pressure'; 