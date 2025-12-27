/*
==========================================
stored procedure: Load Bronze Layer(source -> Bronze)
==========================================
-This stored procedure loads data into tge 'bronze' schema from external CSV files. 
-Truncate tables before loading 
-Use the 'bulk insert' command to load the data 
*/


ALTER   PROCEDURE [bronze].[load_bronze] AS
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME;
	BEGIN TRY
		PRINT'====================================';
		PRINT'LOADING BRONZE LAYER';

		PRINT'-------------------------------------';
		PRINT'loading CRM tables ';
		PRINT'-------------------------------------';


		set @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE -> bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>> INSERTING ATA INTO -> bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\mohan\Downloads\studying\data engineering\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		set @end_time=GETDATE();
		PRINT'>> LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'SECONDS'
		PRINT'----------'

		set @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE -> crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'>> INSERTING ATA INTO -> bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\mohan\Downloads\studying\data engineering\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		set @end_time=GETDATE();
		PRINT'>> LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'SECONDS'
		PRINT'----------'



		set @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE -> bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'>> INSERTING ATA INTO -> bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\mohan\Downloads\studying\data engineering\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		set @end_time=GETDATE();
		PRINT'>> LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'SECONDS'
		PRINT'----------'



		PRINT'-------------------------------------';
		PRINT'loading ERP tables ';
		PRINT'-------------------------------------'; 


		set @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE -> bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT'>> INSERTING ATA INTO -> bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\mohan\Downloads\studying\data engineering\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		set @end_time=GETDATE();
		PRINT'>> LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'SECONDS'
		PRINT'----------'


		set @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE -> bronze.erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT'>> INSERTING ATA INTO -> bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\mohan\Downloads\studying\data engineering\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		set @end_time=GETDATE();
		PRINT'>> LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'SECONDS'
		PRINT'----------'

		set @start_time=GETDATE();
		PRINT'>> TRUNCATING TABLE -> bronze.erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT'>> INSERTING ATA INTO -> erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\mohan\Downloads\studying\data engineering\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		set @end_time=GETDATE();
		PRINT'>> LOAD DURATION: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'SECONDS'
		PRINT'----------'
	END TRY
	BEGIN CATCH 
	     PRINT'============================='
		 PRINT'error accurred during loading bronze layer'
		 PRINT'Error message' + ERROR_MESSAGE();
		 PRINT'=============================='
	END CATCH


	END
	
