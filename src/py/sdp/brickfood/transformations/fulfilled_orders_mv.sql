CREATE MATERIALIZED VIEW fulfilled_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'fulfilled';