-- 1. Create schema
DROP SCHEMA IF EXISTS holidays CASCADE;
CREATE SCHEMA IF NOT EXISTS holidays;

-- 2. Create the table within the schema
DROP TABLE IF EXISTS holidays.holiday_requests CASCADE;
CREATE TABLE IF NOT EXISTS holidays.holiday_requests (
  request_id SERIAL PRIMARY KEY,
  employee_name VARCHAR(255) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  status VARCHAR(50) NOT NULL,
  manager_note TEXT
);

-- 3. Insert sample holiday requests for all team members
INSERT INTO holidays.holiday_requests (employee_name, start_date, end_date, status, manager_note)
  VALUES
    ('Andre Landgraf', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Carly Akerly', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Daniel Liden', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Denny Lee', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Elise Hollowed', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Holly Smith', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Jenni Jones', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Jules Damji', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Lizzie Santor', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Nick Karpov', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Oleksandra Bovkun', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Robert Pack', '2025-12-01', '2025-12-12', 'Pending', ''),
    ('Torey Bublitz', '2025-12-01', '2025-12-12', 'Pending', '');


-- 4. The Lakebase resource in the App already allows connecting to Lakebase database instance and the database.
--    Grant permissions on the required schema and table.
--    Replace the <CLIENT_ID> with the value from your App
-- simple_app client id ("6706ac70-6ca1-4104-b72d-028a0eaa716f)
-- holiday_request_app client id("277f0bb4-7c1f-4f91-81fd-ec1f83a9fdb9")
GRANT USAGE ON SCHEMA holidays TO "277f0bb4-7c1f-4f91-81fd-ec1f83a9fdb9";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE holidays.holiday_requests TO "277f0bb4-7c1f-4f91-81fd-ec1f83a9fdb9";

SELECT * FROM holidays.holiday_requests;