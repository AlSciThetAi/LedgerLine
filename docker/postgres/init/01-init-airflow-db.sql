-- Create the Airflow metadata database if it doesn't exist yet.
-- Runs only on first initialization (when pgdata is empty).

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow') THEN
    CREATE DATABASE airflow;
  END IF;
END $$;