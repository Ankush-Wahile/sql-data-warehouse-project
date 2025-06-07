CREATE OR REPLACE PROCEDURE silver.load_silver()
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
			batch_start_time := clock_timestamp();

				RAISE NOTICE '==========================================';
				RAISE NOTICE 'Loading data into Silver layer schema.....';
				RAISE NOTICE '==========================================';
	
				RAISE NOTICE '-------------------------------------------';
				RAISE NOTICE 'Loading CRM Tables';
				RAISE NOTICE '-------------------------------------------';
			
				start_time := clock_timestamp();
				RAISE NOTICE 'TRUNCATING TABLE:: silver.crm_cust_info==============================================';
				TRUNCATE TABLE silver.crm_cust_info;
				RAISE NOTICE 'Inserting DATA INTO:: silver.crm_cust_info ==========================================';
				RAISE NOTICE '=====================================================================================';
				INSERT INTO silver.crm_cust_info(
					cst_id,
					cst_key,
					cst_firstname,
					cst_lastname,
					cst_marital_status,
					cst_gndr,
					cst_create_date)
				select 
					cst_id,
					cst_key,
					TRIM(cst_firstname) as cst_firstname,
					TRIM(cst_lastname) as cst_lastname,
					CASE WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
						 WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
						 ELSE 'n\a'
					END cst_marital_status,
					CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
						 WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
						 ELSE 'n\a'
					END
					cst_gndr,
					cst_create_date
				from(
					select * ,
					ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
					from bronze.crm_cust_info where cst_id is NOT NULL
				)t WHERE flag_last=1;


				end_time := clock_timestamp();
    			duration := end_time - start_time;
    			RAISE NOTICE 'Data loaded into silver.crm_cust_info. Time taken: %', duration;
		
				RAISE NOTICE '--------------------------------------------------------------';
				
-----------------------------------------------------------------------				
				
				start_time := clock_timestamp();
				RAISE NOTICE 'TRUNCATING TABLE:: silver.crm_prd_info==============================================';
				TRUNCATE TABLE silver.crm_prd_info;
				RAISE NOTICE 'Inserting DATA INTO:: silver.crm_prd_info ==========================================';
				RAISE NOTICE '=====================================================================================';
				
				INSERT INTO silver.crm_prd_info(
					prd_id,
					cat_id,
					prd_key,
					prd_nm,
					prd_cost,
					prd_line,
					prd_start_dt,
					prd_end_dt
				)
				SELECT
					prd_id,
					REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,	--Extract category_id
					SUBSTRING(prd_key,7,LENGTH(prd_key))as prd_key, --Extract product key
					prd_nm,
					COALESCE(prd_cost, 0) AS prd_cost,
					CASE UPPER(TRIM(prd_line))
						WHEN 'M' THEN 'Mountain'
						WHEN 'R' THEN 'Road'
						WHEN 'S' THEN 'Other Sales'
						WHEN 'T' THEN 'Touring'
						ELSE 'n\a'
					END as prd_line,--map product line codes to descriptive values
					prd_start_dt,
					LEAD(prd_start_dt) over (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt --Calculate end date as one day before the next start date
				from bronze.crm_prd_info;

				end_time := clock_timestamp();
    			duration := end_time - start_time;
    			RAISE NOTICE 'Data loaded into silver.crm_prd_info. Time taken: %', duration;
		
				RAISE NOTICE '-------------------------------------------';
				
				
---------------------------------------------------------------------------				
				
				start_time := clock_timestamp();
				RAISE NOTICE 'TRUNCATING TABLE:: silver.crm_sales_details==============================================';
				TRUNCATE TABLE silver.crm_sales_details;
				RAISE NOTICE 'Inserting DATA INTO:: silver.crm_sales_details ==========================================';
				RAISE NOTICE '=====================================================================================';
				
				
				INSERT INTO silver.crm_sales_details(
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					sls_order_dt,
					sls_ship_dt,
					sls_due_dt,
					sls_sales,
					sls_quantity,
					sls_price
					)
				SELECT
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					CASE 
						WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 
						THEN NULL
						ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
					END AS sls_order_dt,
					CASE 
						WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 
						THEN NULL
						ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
					END AS sls_ship_dt,
					CASE 
						WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 
						THEN NULL
						ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
					END AS sls_due_dt,
					CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!=sls_quantity * ABS(sls_price)
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
					END AS sls_sales,
					sls_quantity,
					CASE WHEN sls_price IS NULL OR sls_price <=0
						THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
					END as sls_price
				FROM bronze.crm_sales_details;
				end_time := clock_timestamp();
    			duration := end_time - start_time;
    			RAISE NOTICE 'Data loaded into silver.crm_sales_details. Time taken: %', duration;
		
				RAISE NOTICE '-------------------------------------------';

-----------------------------------------------------------------


				RAISE NOTICE '-------------------------------------------';
				RAISE NOTICE 'Loading ERP Tables';
				RAISE NOTICE '-------------------------------------------';
	
				start_time := clock_timestamp();
				
				RAISE NOTICE 'TRUNCATING TABLE:: silver.erp_cust_az12==============================================';
				TRUNCATE TABLE silver.erp_cust_az12;
				RAISE NOTICE 'Inserting DATA INTO:: silver.erp_cust_az12 ==========================================';
				RAISE NOTICE '=====================================================================================';
				
				
				INSERT INTO silver.erp_cust_az12(
					cid,
					bdate,
					gen
				)
				SELECT  
				CASE WHEN cid LIKE '%NAS%' THEN SUBSTRING(cid,4,LENGTH(cid)) ---Rmove 'NAS' Prefix if present
					ELSE cid
				END AS cid,
				CASE WHEN bdate > CURRENT_DATE THEN NULL
					 ELSE bdate
				END AS bdate, -- Set future birthdate to NULL
				case when UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
					 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
					 ELSE 'n/a'
				END AS gen -- Normalize Gender Value and Handle unknown cases
				from bronze.erp_cust_az12;
				
				end_time := clock_timestamp();
    			duration := end_time - start_time;
    			RAISE NOTICE 'Data loaded into silver.erp_cust_az12. Time taken: %', duration;

------------------------------------------------				

				start_time := clock_timestamp();
				RAISE NOTICE 'TRUNCATING TABLE:: silver.erp_loc_a101==============================================';
				TRUNCATE TABLE silver.erp_loc_a101;
				RAISE NOTICE 'Inserting DATA INTO:: silver.erp_loc_a101 ==========================================';
				RAISE NOTICE '=====================================================================================';
				
				
				INSERT INTO silver.erp_loc_a101(
					cid,
					cntry
				)
				select replace(cid,'-','') cid,
				case when TRIM(cntry) ='DE' THEN 'Germany'
					when TRIM(cntry) IN ('US','USA') THEN 'United States'
					when TRIM(cntry) = '' or TRIM(cntry) is NULL THEN 'n/a'
				ELSE TRIM(cntry)
				end as cntry -- normalize and handle missing and blank country codes
				FROM bronze.erp_loc_a101;

				end_time := clock_timestamp();
    			duration := end_time - start_time;
    			RAISE NOTICE 'Data loaded into silver.erp_loc_a101. Time taken: %', duration;
				
-------------------------------
				
				start_time := clock_timestamp();
				RAISE NOTICE 'TRUNCATING TABLE:: silver.erp_px_cat_g1v2==============================================';
				TRUNCATE TABLE silver.erp_px_cat_g1v2;
				RAISE NOTICE 'Inserting DATA INTO:: silver.erp_px_cat_g1v2 ==========================================';
				RAISE NOTICE '=====================================================================================';
				
				
				INSERT into silver.erp_px_cat_g1v2(
					id,
					cat,
					subcat,
					maintenance
				)
				select 
					id,
					cat,
					subcat,
					maintenance
				from 
				bronze.erp_px_cat_g1v2;

				end_time := clock_timestamp();
    			duration := end_time - start_time;
    			RAISE NOTICE 'Data loaded into silver.erp_px_cat_g1v2. Time taken: %', duration;
			EXCEPTION
		WHEN OTHERS THEN
			 RAISE NOTICE '========================================';
			 RAISE NOTICE 'Error Message: %', SQLERRM;
			 RAISE NOTICE 'Error Code: %', SQLSTATE;
		END;
	

	END;
$$;


call silver.load_silver();
