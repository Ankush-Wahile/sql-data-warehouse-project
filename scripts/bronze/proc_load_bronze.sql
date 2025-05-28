/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
	DECLARE
    start_time timestamp;
    end_time timestamp;
    duration interval;
	batch_start_time timestamp;
    batch_end_time timestamp;
    batch_duration interval;
	BEGIN  --for exception handling
	--show search_path;
		batch_start_time := clock_timestamp();
	
		SET search_path TO bronze;
		-- or to include multiple schemas
		--SET search_path TO schema_name, public;
		
		-- list all the schemas in database
		/*
		SELECT nspname AS schema_name
		FROM pg_namespace
		ORDER BY nspname
		*/
		
		
		---
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'Loading data into bronze layer schema.....';
		RAISE NOTICE '==========================================';
	
		RAISE NOTICE '-------------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '-------------------------------------------';

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		RAISE NOTICE '>> Inserting Data Into Table: bronze.crm_cust_info';
		COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		FROM 'C:\Users\Raksha\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
    	duration := end_time - start_time;
    	RAISE NOTICE 'Data loaded into bronze.crm_cust_info. Time taken: %', duration;
		
		RAISE NOTICE '--------------------------------------------------------------';
		
		
		--select * from bronze.crm_cust_info 
		
		--select count(*) from bronze.crm_cust_info; #18494
		
		
		---

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		
	
		RAISE NOTICE '>> Inserting Data Into Table: bronze.crm_prd_info';
		COPY bronze.crm_prd_info 
		FROM 'C:\Users\Raksha\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
    	duration := end_time - start_time;
    	RAISE NOTICE 'Data loaded into bronze.crm_prd_info. Time taken: %', duration;
		
		RAISE NOTICE '-------------------------------------------';
		
		--select * from bronze.crm_prd_info; #397
		---

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	
		RAISE NOTICE '>> Inserting Data Into Table: bronze.crm_sales_details';
		COPY bronze.crm_sales_details 
		FROM 'C:\Users\Raksha\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
    	duration := end_time - start_time;
    	RAISE NOTICE 'Data loaded into bronze.crm_sales_details. Time taken: %', duration;
		
		RAISE NOTICE '-------------------------------------------';
		
		--select * from bronze.crm_sales_details; #60398
		---
	
		RAISE NOTICE '-------------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '-------------------------------------------';
	
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		RAISE NOTICE '>> Inserting Data Into Table: bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101
		FROM 'C:\Users\Raksha\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
    	duration := end_time - start_time;
    	RAISE NOTICE 'Data loaded into bronze.erp_loc_a101. Time taken: %', duration;
		
		
		RAISE NOTICE '-------------------------------------------';
		
		--select * from bronze.erp_loc_a101; # 18484
		---
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		RAISE NOTICE '>> Inserting Data Into Table: bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM 'C:\Users\Raksha\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
    	duration := end_time - start_time;
    	RAISE NOTICE 'Data loaded into bronze.erp_cust_az12. Time taken: %', duration;
		
		RAISE NOTICE '-------------------------------------------';
		
		--select * from bronze.erp_cust_az12; #18484
		---
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		RAISE NOTICE '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Raksha\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		DELIMITER ','
		CSV HEADER;
		end_time := clock_timestamp();
    	duration := end_time - start_time;
    	RAISE NOTICE 'Data loaded into bronze.erp_px_cat_g1v2. Time taken: %', duration;
		
		RAISE NOTICE '-------------------------------------------';
		
		--select * from bronze.erp_px_cat_g1v2; #37
		
		---
		batch_end_time := clock_timestamp();
    	duration := batch_end_time - batch_start_time;
    	RAISE NOTICE 'Total time taken for all batches: %', duration;
	
	EXCEPTION
		WHEN OTHERS THEN
			 RAISE NOTICE '========================================';
			 RAISE NOTICE 'Error Message: %', SQLERRM;
			 RAISE NOTICE 'Error Code: %', SQLSTATE;
	END;
	
		
	
END;
$$;
