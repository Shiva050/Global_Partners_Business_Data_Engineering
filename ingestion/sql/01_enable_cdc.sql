-- ============================================================================
-- Enable MS-CDC on the RDS SQL Server source so AWS DMS can read change data.
-- Run as the RDS master user against the SQL Server instance.
--
-- Prereqs:
--   * RDS SQL Server edition/version that supports CDC (Standard 2016 SP1+,
--     Enterprise, or Web). Express does NOT support CDC.
--   * Automated backups enabled on the RDS instance (DMS needs the tx log).
-- ============================================================================

-- 1) Enable CDC at the DATABASE level (RDS-specific stored proc).
--    This creates the CDC capture/cleanup SQL Agent jobs.
EXEC msdb.dbo.rds_cdc_enable_db 'GlobalPartners';
GO

USE [GlobalPartners];
GO

-- 2) Enable CDC per TABLE. DMS reads changes from the generated change tables.
--    @role_name = NULL  -> no gating role, any reader with access can query.
--    @supports_net_changes = 0 -> capture every change (we want the full I/U/D log).

EXEC sys.sp_cdc_enable_table
     @source_schema        = N'dbo',
     @source_name          = N'order_items',
     @role_name            = NULL,
     @supports_net_changes = 0;
GO

EXEC sys.sp_cdc_enable_table
     @source_schema        = N'dbo',
     @source_name          = N'order_item_options',
     @role_name            = NULL,
     @supports_net_changes = 0;
GO

EXEC sys.sp_cdc_enable_table
     @source_schema        = N'dbo',
     @source_name          = N'date_dim',
     @role_name            = NULL,
     @supports_net_changes = 0;
GO

-- 3) (Recommended) Extend CDC change-table retention so a slow/failed DMS task
--    can resume without losing changes. Default is 3 days (4320 min).
--    Bump to 7 days for safety.
EXEC sys.sp_cdc_change_job
     @job_type  = N'cleanup',
     @retention = 10080;   -- minutes = 7 days
GO

-- 4) Verify CDC is on.
SELECT name, is_cdc_enabled
FROM   sys.databases
WHERE  name = 'GlobalPartners';
GO

EXEC sys.sp_cdc_help_change_data_capture;   -- lists CDC-enabled tables
GO
