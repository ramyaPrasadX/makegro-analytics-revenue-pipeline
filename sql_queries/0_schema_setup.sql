-- ==============================================================================
-- BIGQUERY SCHEMA SETUP: MakeGro Analytics 
-- ==============================================================================
-- Description: DDL script to create the tables for the Marketing Analytics Hub.

-- Create the Dataset (if it doesn't already exist)
CREATE SCHEMA IF NOT EXISTS `makegro-analytics-hub.agency_performance`;

-- 1. Create the Cleaned Meta Leads Table
CREATE OR REPLACE TABLE `makegro-analytics-hub.agency_performance.meta_leads` (
    Lead_ID STRING NOT NULL,
    Date DATE NOT NULL,
    Phone_Number STRING,        -- Contains the hashed UID for privacy
    Client_Name STRING,
    Campaign_Name STRING
);

-- 2. Create the Cleaned CRM Sales Table
CREATE OR REPLACE TABLE `makegro-analytics-hub.agency_performance.crm_sales` (
    Contract_ID STRING NOT NULL,
    Closing_Date DATE NOT NULL,
    Phone_Number STRING,        -- Contains the hashed UID for exact JOINs
    Final_Brand_Booked STRING,
    Contract_Value_INR INT64,   -- Parsed from "Lakhs" strings, 18% GST included
    Sales_Rep STRING
);
