-- ============================================
-- Banking Source System Database Setup
-- ============================================
-- This script creates the source database for the banking system
-- Run this script first using: psql -U postgres -f 01_create_database.sql

-- Connect to PostgreSQL as postgres user
-- Create database for banking source system
CREATE DATABASE banking_source;

-- Connect to the new database
\c banking_source;

-- Create schema for better organization
CREATE SCHEMA banking;
SET search_path TO banking;

-- Display confirmation
SELECT 'Database banking_source created successfully!' as status;

