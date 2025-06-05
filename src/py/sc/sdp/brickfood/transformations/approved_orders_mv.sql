CREATE MATERIALIZED VIEW approved_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'approved';
