-- The platform Postgres server hosts independent databases. Keeping them
-- separate mirrors production, where each would be its own managed service.
-- `warehouse_test` is a scratch database used only by the test suite.
CREATE DATABASE iceberg_catalog;
CREATE DATABASE warehouse;
CREATE DATABASE airflow;
CREATE DATABASE metabase;
CREATE DATABASE warehouse_test;
