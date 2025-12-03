-- init_db.sql
CREATE DATABASE reddit_analytics;
GRANT ALL PRIVILEGES ON DATABASE reddit_analytics TO prefect;

-- Base para Metabase
CREATE DATABASE metabase_appdb;
GRANT ALL PRIVILEGES ON DATABASE metabase_appdb TO prefect;