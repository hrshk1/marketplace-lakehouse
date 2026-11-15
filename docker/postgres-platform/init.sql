-- The platform Postgres server hosts four independent databases. Keeping them
-- separate mirrors production, where each would be its own managed service.
CREATE DATABASE iceberg_catalog;
CREATE DATABASE warehouse;
CREATE DATABASE airflow;
CREATE DATABASE metabase;
