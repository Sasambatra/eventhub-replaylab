#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
\getenv app_password DB_PASSWORD
\getenv migration_password MIGRATION_PASSWORD
\getenv a_password LAB_A_DB_PASSWORD
\getenv b_password LAB_B_DB_PASSWORD
CREATE ROLE eventhub LOGIN PASSWORD :'app_password';
CREATE ROLE eventhub_migrator LOGIN PASSWORD :'migration_password';
CREATE ROLE eventhub_lab_a LOGIN PASSWORD :'a_password';
CREATE ROLE eventhub_lab_b LOGIN PASSWORD :'b_password';
CREATE DATABASE eventhub OWNER eventhub_migrator;
CREATE DATABASE eventhub_test OWNER eventhub_migrator;
CREATE DATABASE eventhub_lab_a OWNER eventhub_migrator;
CREATE DATABASE eventhub_lab_b OWNER eventhub_migrator;
CREATE DATABASE eventhub_lab_a_test OWNER eventhub_migrator;
CREATE DATABASE eventhub_lab_b_test OWNER eventhub_migrator;
REVOKE CONNECT ON DATABASE eventhub,eventhub_test,eventhub_lab_a,eventhub_lab_b,eventhub_lab_a_test,eventhub_lab_b_test FROM PUBLIC;
GRANT CONNECT ON DATABASE eventhub,eventhub_test TO eventhub;
GRANT CONNECT ON DATABASE eventhub_lab_a,eventhub_lab_a_test TO eventhub_lab_a;
GRANT CONNECT ON DATABASE eventhub_lab_b,eventhub_lab_b_test TO eventhub_lab_b;
SQL
for database in eventhub eventhub_test eventhub_lab_a eventhub_lab_b eventhub_lab_a_test eventhub_lab_b_test; do
  case "$database" in
    eventhub|eventhub_test) runtime_role=eventhub ;;
    eventhub_lab_a*) runtime_role=eventhub_lab_a ;;
    *) runtime_role=eventhub_lab_b ;;
  esac
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$database" --set=runtime_role="$runtime_role" <<'SQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO eventhub_migrator;
GRANT USAGE ON SCHEMA public TO :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE eventhub_migrator IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO :"runtime_role";
ALTER DEFAULT PRIVILEGES FOR ROLE eventhub_migrator IN SCHEMA public GRANT USAGE,SELECT ON SEQUENCES TO :"runtime_role";
SQL
done
