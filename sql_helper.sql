--SQL table sice query
SELECT 
    t.NAME AS TableName,
    s.Name AS SchemaName,
    SUM(p.rows) AS [Rows],
    SUM(a.total_pages) * 8 AS TotalSpaceKB, 
    CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB,
    SUM(a.used_pages) * 8 AS UsedSpaceKB, 
    CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS UsedSpaceMB, 
    (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB,
    CAST(ROUND(((SUM(a.total_pages) - SUM(a.used_pages)) * 8) / 1024.00, 2) AS NUMERIC(36, 2)) AS UnusedSpaceMB
FROM 
    sys.tables t
INNER JOIN      
    sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN 
    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN 
    sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    t.NAME NOT LIKE 'dt%' 
    AND t.is_ms_shipped = 0
    AND i.OBJECT_ID > 255 
GROUP BY 
    t.Name, s.Name
ORDER BY 
    TotalSpaceMB DESC, t.Name
	
-- Blocking
SELECT session_id, blocking_session_id, wait_time, wait_resource, start_time, status, command, sql_handle, plan_handle, [context_info], statement_end_offset
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0;


SELECT s1.sql_handle,    
    (SELECT TOP 1 SUBSTRING(s2.text,statement_start_offset / 2+1 ,   
      ( (CASE WHEN statement_end_offset = -1   
         THEN (LEN(CONVERT(nvarchar(max),s2.text)) * 2)   
         ELSE statement_end_offset END)  - statement_start_offset) / 2+1))  AS sql_statement

fROM 
sys.dm_exec_requests as s1
CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS s2 
WHERE s1.session_id = 165
--Properties
SELECT @@VERSION
SELECT * FROM sys.databases
SELECT * FROM sys.objects
SELECT * FROM sys.dm_os_schedulers
SELECT * FROM sys.dm_os_sys_info
SELECT * FROM sys.dm_os_process_memory --Not supported in Azure SQL Database
SELECT * FROM sys.dm_exec_requests
SELECT SERVERPROPERTY('EngineEdition')
SELECT * FROM sys.dm_user_db_resource_governance -- Available only in Azure SQL Database and SQL Managed Instance
SELECT * FROM sys.dm_instance_resource_governance -- Available only in Azure SQL Managed Instance
SELECT * FROM sys.dm_os_job_object -- Available only in Azure SQL Database and SQL Managed Instance


SELECT 'ALTER TABLE scvstage.rptg_fact_ord ALTER COLUMN '
	+ c.name
	+ ' '
	+ t.name 
	+ case when t.name =  'nvarchar'
			then '(' + CAST( c.max_length/2 AS NVARCHAR(4)) + ')'
			when t.name =  'varchar'
			then '(' + CAST( c.max_length AS NVARCHAR(4)) + ')'
		ELSE ''
	END
	+ ' NULL'
FROM sys.columns as c 
	INNER JOIN sys.systypes as t ON (t.xtype = c.system_type_id)
WHERE object_id = OBJECT_ID('scvstage.rptg_fact_ord')
AND t.name <> 'sysname'
AND is_nullable = 0


/*clarify usage
 sys.dm_io_virtual_file_stats

sys.dm_os_performance_counters

sys.dm_user_db_resource_governance_internal (SQL Managed Instance only)
sys.dm_resource_governor_resource_pools_history_ex
sys.dm_resource_governor_workload_groups_history_ex
*/
SELECT er.session_id, er.status, er.command, er.wait_type, er.last_wait_type, er.wait_resource, er.wait_time
FROM sys.dm_exec_requests er
INNER JOIN sys.dm_exec_sessions es
ON er.session_id = es.session_id
AND es.is_user_process = 1;

SELECT * FROM sys.dm_os_wait_stats
ORDER BY waiting_tasks_count DESC;

SELECT io_stall_write_ms/num_of_writes as avg_tlog_io_write_ms, * 
FROM sys.dm_io_virtual_file_stats
(db_id('AdventureWorks'), 2);

-- failover and replicas
SELECT DATABASEPROPERTYEX(DB_NAME(), 'Updateability')
SELECT * FROM sys.dm_database_replica_states

--extendent events
IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE name='test_session')
    DROP EVENT session test_session ON SERVER;
GO

CREATE EVENT SESSION test_session
ON SERVER
    ADD EVENT sqlos.async_io_requested,
    ADD EVENT sqlserver.lock_acquired
    ADD TARGET package0.etw_classic_sync_target (SET default_etw_session_logfile_path = N'C:\demo\traces\sqletw.etl' )
    WITH (MAX_MEMORY=4MB, MAX_EVENT_SIZE=4MB);
GO

--list of available events 
SELECT
    obj.object_type,
    pkg.name AS [package_name],
    obj.name AS [object_name],
    obj.description AS [description]
FROM sys.dm_xe_objects  AS obj
    INNER JOIN sys.dm_xe_packages AS pkg  ON pkg.guid = obj.package_guid
WHERE obj.object_type in ('action',  'event',  'target')
ORDER BY obj.object_type,
    pkg.name,
    obj.name;
	
--Locks

SELECT tst.session_id, [database_name] = db_name(s.database_id)
    , tat.transaction_begin_time
    , transaction_duration_s = datediff(s, tat.transaction_begin_time, sysdatetime()) 
    , transaction_type = CASE tat.transaction_type  WHEN 1 THEN 'Read/write transaction'
        WHEN 2 THEN 'Read-only transaction'
        WHEN 3 THEN 'System transaction'
        WHEN 4 THEN 'Distributed transaction' END
    , input_buffer = ib.event_info, tat.transaction_uow     
    , transaction_state  = CASE tat.transaction_state    
        WHEN 0 THEN 'The transaction has not been completely initialized yet.'
        WHEN 1 THEN 'The transaction has been initialized but has not started.'
        WHEN 2 THEN 'The transaction is active - has not been committed or rolled back.'
        WHEN 3 THEN 'The transaction has ended. This is used for read-only transactions.'
        WHEN 4 THEN 'The commit process has been initiated on the distributed transaction.'
        WHEN 5 THEN 'The transaction is in a prepared state and waiting resolution.'
        WHEN 6 THEN 'The transaction has been committed.'
        WHEN 7 THEN 'The transaction is being rolled back.'
        WHEN 8 THEN 'The transaction has been rolled back.' END 
    , transaction_name = tat.name, request_status = r.status
    , tst.is_user_transaction, tst.is_local
    , session_open_transaction_count = tst.open_transaction_count  
    , s.host_name, s.program_name, s.client_interface_name, s.login_name, s.is_user_process
FROM sys.dm_tran_active_transactions tat 
INNER JOIN sys.dm_tran_session_transactions tst  on tat.transaction_id = tst.transaction_id
INNER JOIN Sys.dm_exec_sessions s on s.session_id = tst.session_id 
LEFT OUTER JOIN sys.dm_exec_requests r on r.session_id = s.session_id
CROSS APPLY sys.dm_exec_input_buffer(s.session_id, null) AS ib
ORDER BY tat.transaction_begin_time DESC;

--Statistics

SELECT sp.stats_id, 
       name, 
       last_updated, 
       rows, 
       rows_sampled
FROM sys.stats
     CROSS APPLY sys.dm_db_stats_properties(object_id, stats_id) AS sp
WHERE user_created = 1

--Rebuilding

USE AdventureWorks2017
GO

ALTER INDEX [IX_Address_StateProvinceID] ON [Person].[Address] REBUILD PARTITION = ALL 
WITH (PAD_INDEX = OFF, 
    STATISTICS_NORECOMPUTE = OFF, 
    SORT_IN_TEMPDB = OFF, 
    IGNORE_DUP_KEY = OFF, 
    ONLINE = OFF, 
    ALLOW_ROW_LOCKS = ON, 
    ALLOW_PAGE_LOCKS = ON)

--Profiling

OPTION(USE HINT ('QUERY_PLAN_PROFILE'));
--
/*This functionality lets you quickly identify the runtime stats for the last execution of any query in your system, 
with minimal overhead. The image below shows how to retrieve the plan. If you click on the execution plan XML, 
which will be the first column of results, will display the execution plan shown in the second image below.*/
