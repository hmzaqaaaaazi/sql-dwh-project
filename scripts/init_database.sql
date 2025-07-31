-- =====================================================
-- DATA WAREHOUSE INITIALIZATION SCRIPT
-- =====================================================
-- Purpose: Set up clean environment and create schema structure
-- Database: datawarehouse
-- Architecture: Medallion (Bronze -> Silver -> Gold)
-- =====================================================

-- =====================================================
-- STEP 1: CLEAR ALL EXISTING CONNECTIONS
-- =====================================================
-- This DO block terminates all other active connections to the datawarehouse
-- to ensure we have exclusive access during setup operations

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datawarehouse') THEN
        PERFORM pg_terminate_backend(pid)
        FROM pg_stat_activity 
        WHERE datname = 'datawarehouse' 
        AND pid <> pg_backend_pid();
        
        RAISE NOTICE 'All connections to datawarehouse database have been terminated';
    ELSE
        RAISE NOTICE 'Database datawarehouse does not exist';
    END IF;
END $$
LANGUAGE plpgsql;

DROP DATABASE IF EXISTS datawarehouse;

CREATE DATABASE datawarehouse;


-- =====================================================
-- STEP 2: CREATE MEDALLION ARCHITECTURE SCHEMAS
-- =====================================================
-- The medallion architecture organizes data in three layers:
-- Bronze: Raw, unprocessed data (landing zone)
-- Silver: Cleaned, validated, and deduplicated data
-- Gold: Business-ready, aggregated data for analytics

-- Starting with creating Schemas and also using debugging technique for making sure no mistakes.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


