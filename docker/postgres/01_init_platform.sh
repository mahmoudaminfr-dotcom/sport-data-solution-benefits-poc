#!/bin/sh
(
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS hr_restricted;
    CREATE SCHEMA IF NOT EXISTS oltp_sports;
    CREATE SCHEMA IF NOT EXISTS gold_mart;
    CREATE SCHEMA IF NOT EXISTS ops_monitoring;

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${SDS_PIPELINE_USER}') THEN
            CREATE ROLE ${SDS_PIPELINE_USER} WITH LOGIN PASSWORD '${SDS_PIPELINE_PASSWORD}';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${SDS_DEBEZIUM_USER}') THEN
            CREATE ROLE ${SDS_DEBEZIUM_USER} WITH LOGIN REPLICATION PASSWORD '${SDS_DEBEZIUM_PASSWORD}';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${SDS_BI_READER_USER}') THEN
            CREATE ROLE ${SDS_BI_READER_USER} WITH LOGIN PASSWORD '${SDS_BI_READER_PASSWORD}';
        END IF;
    END
    \$\$;

    CREATE TABLE IF NOT EXISTS hr_restricted.employees_audit (
        employee_id INTEGER PRIMARY KEY,
        last_name VARCHAR(100) NOT NULL,
        first_name VARCHAR(100) NOT NULL,
        birth_date DATE NOT NULL,
        business_unit VARCHAR(60) NOT NULL,
        hire_date DATE NOT NULL,
        annual_gross_salary_eur NUMERIC(12, 2) NOT NULL CHECK (annual_gross_salary_eur > 0),
        contract_type VARCHAR(20) NOT NULL,
        paid_leave_days INTEGER NOT NULL CHECK (paid_leave_days >= 0),
        home_address TEXT NOT NULL,
        declared_commute_mode VARCHAR(80) NOT NULL,
        computed_distance_km NUMERIC(8, 3),
        max_allowed_distance_km NUMERIC(6, 2),
        is_commute_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
        is_mobility_bonus_eligible BOOLEAN NOT NULL DEFAULT FALSE,
        anomaly_reason TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS oltp_sports.sport_activities (
        activity_id BIGSERIAL PRIMARY KEY,
        employee_id INTEGER NOT NULL,
        employee_full_name VARCHAR(200) NOT NULL,
        sport_type VARCHAR(80) NOT NULL,
        activity_start TIMESTAMPTZ NOT NULL,
        activity_end TIMESTAMPTZ NOT NULL,
        elapsed_seconds INTEGER NOT NULL CHECK (elapsed_seconds > 0),
        distance_m INTEGER CHECK (distance_m IS NULL OR distance_m >= 0),
        comment TEXT,
        ingestion_mode VARCHAR(20) NOT NULL DEFAULT 'live_event'
            CHECK (ingestion_mode IN ('historical_seed', 'live_event')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT chk_activity_chronology CHECK (activity_end >= activity_start)
    );

    ALTER TABLE oltp_sports.sport_activities REPLICA IDENTITY FULL;

    CREATE INDEX IF NOT EXISTS idx_sport_activities_emp_start
        ON oltp_sports.sport_activities (employee_id, activity_start DESC);

    CREATE TABLE IF NOT EXISTS gold_mart.dim_business_parameters (
        parameter_set_id INTEGER PRIMARY KEY DEFAULT 1 CHECK (parameter_set_id = 1),
        prime_rate NUMERIC(5, 4) NOT NULL,
        max_walk_run_km NUMERIC(6, 2) NOT NULL,
        max_bike_micro_km NUMERIC(6, 2) NOT NULL,
        min_annual_activities INTEGER NOT NULL,
        wellbeing_days_granted INTEGER NOT NULL,
        last_recomputed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS ops_monitoring.pipeline_runs (
        run_id UUID PRIMARY KEY,
        pipeline_name VARCHAR(100) NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        status VARCHAR(30) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'WARNING')),
        rows_processed INTEGER NOT NULL DEFAULT 0,
        anomalies_detected INTEGER NOT NULL DEFAULT 0,
        duration_ms INTEGER,
        details JSONB
    );

    CREATE TABLE IF NOT EXISTS ops_monitoring.quality_check_results (
        check_id BIGSERIAL PRIMARY KEY,
        run_id UUID REFERENCES ops_monitoring.pipeline_runs(run_id),
        layer_name VARCHAR(40) NOT NULL,
        check_name VARCHAR(150) NOT NULL,
        outcome VARCHAR(20) NOT NULL CHECK (outcome IN ('PASS', 'FAIL', 'WARN')),
        failed_rows_count INTEGER NOT NULL DEFAULT 0,
        evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    GRANT CONNECT ON DATABASE "${POSTGRES_DB}" TO ${SDS_PIPELINE_USER}, ${SDS_DEBEZIUM_USER}, ${SDS_BI_READER_USER};
    GRANT USAGE, CREATE ON SCHEMA hr_restricted, oltp_sports, gold_mart, ops_monitoring TO ${SDS_PIPELINE_USER};
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hr_restricted, oltp_sports, gold_mart TO ${SDS_PIPELINE_USER};
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ops_monitoring TO ${SDS_PIPELINE_USER} WITH GRANT OPTION;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA oltp_sports, ops_monitoring TO ${SDS_PIPELINE_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold_mart GRANT SELECT ON TABLES TO ${SDS_BI_READER_USER};
    ALTER DEFAULT PRIVILEGES FOR ROLE ${SDS_PIPELINE_USER} IN SCHEMA gold_mart GRANT SELECT ON TABLES TO ${SDS_BI_READER_USER};

    GRANT USAGE ON SCHEMA oltp_sports TO ${SDS_DEBEZIUM_USER};
    GRANT SELECT ON oltp_sports.sport_activities TO ${SDS_DEBEZIUM_USER};
    CREATE PUBLICATION sds_activities_pub FOR TABLE oltp_sports.sport_activities;

    REVOKE ALL ON SCHEMA hr_restricted FROM PUBLIC, ${SDS_BI_READER_USER};
    REVOKE ALL ON SCHEMA oltp_sports FROM PUBLIC, ${SDS_BI_READER_USER};
    REVOKE ALL ON ALL TABLES IN SCHEMA ops_monitoring FROM PUBLIC, ${SDS_BI_READER_USER};
    GRANT USAGE ON SCHEMA gold_mart, ops_monitoring TO ${SDS_BI_READER_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA gold_mart TO ${SDS_BI_READER_USER};
EOSQL
)