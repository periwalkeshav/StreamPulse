-- Airflow keeps its metadata in a separate logical database on the same server.
-- Runs once, on the very first boot of the postgres volume.
SELECT 'CREATE DATABASE airflow OWNER streampulse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
