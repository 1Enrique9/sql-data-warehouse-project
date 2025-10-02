CREATE OR ALTER PROCEDURE bronze.load_bronze_tables AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=======================================';
        PRINT 'Cargando datos en tablas Bronze...';
        PRINT '=======================================';

        PRINT '---------------------------------------';
        PRINT 'Cargando tablas CRM...';
        PRINT '---------------------------------------';
        
        SET @start_time = GETDATE();
        PRINT '>> Cargando tabla: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Insertando datos en: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/cust_info.csv'
        WITH (
            FIELDTERMINATOR = ',',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duración de la carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -------------';

        SET @start_time = GETDATE();
        PRINT '>> Cargando tabla: bronze.crm_cust_prd_info';
        TRUNCATE TABLE bronze.crm_cust_prd_info;
        PRINT '>> Insertando datos en: bronze.crm_cust_prd_info';
        BULK INSERT bronze.crm_cust_prd_info
        FROM '/var/opt/mssql/prd_info.csv'
        WITH (
            FIELDTERMINATOR = ',',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duración de la carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -------------';

        SET @start_time = GETDATE();
        PRINT '>> Cargando tabla: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Insertando datos en: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/sales_details.csv'
        WITH (
            FIELDTERMINATOR = ',',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duración de la carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -------------';

        PRINT '---------------------------------------';
        PRINT 'Cargando tablas ERP';
        PRINT '---------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Cargando tabla: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Insertando datos en: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/LOC_A101.csv'
        WITH (
            FIELDTERMINATOR = ',',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duración de la carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -------------';

        SET @start_time = GETDATE();
        PRINT '>> Cargando tabla: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Insertando datos en: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/CUST_AZ12.csv'
        WITH (
            FIELDTERMINATOR = ',',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duración de la carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -------------';

        SET @start_time = GETDATE();
        PRINT '>> Cargando tabla: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Insertando datos en: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/PX_CAT_G1V2.csv'
        WITH (
            FIELDTERMINATOR = ',',
            FIRSTROW = 2,
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Duración de la carga: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' segundos';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '=======================================';
        PRINT 'Carga de tablas Bronze completa';
        PRINT 'Duración total: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' segundos';
        PRINT '=======================================';
    END TRY
    BEGIN CATCH
        PRINT '=======================================';
        PRINT 'Error durante la carga de tablas Bronze';
        PRINT 'Mensaje de Error: ' + ERROR_MESSAGE();
        PRINT 'Número de Error: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Estado de Error: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=======================================';
    END CATCH
END;
