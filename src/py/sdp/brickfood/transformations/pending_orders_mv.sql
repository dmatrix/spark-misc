CREATE MATERIALIZED VIEW pending_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'pending';