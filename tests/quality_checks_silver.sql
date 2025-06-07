/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--show search_path;

--Check For NULLs or Duplicates in Primary Key
--Expectation: No result

select cst_id,count(*)  from bronze.crm_cust_info
group by cst_id
having count(*)>1 OR cst_id IS NULL;


--(Quality Check)Check for unwanted spaces in a string values
--Expectations: No result
select cst_firstname from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

select cst_gndr from bronze.crm_cust_info --# as ther is no result for thsi query the quality of this table is good
where cst_gndr != TRIM(cst_gndr);

--(Quality Check)- Check the consistency of values in low Cardinality Columns
--ex. cst_marital_status,cst_gndr this two columns
-- Data Standarization and consitency[In our datawarehouse we aim to store clear and meaningful values rather than abbreviated terms]
--[IN our datawarehouse we use default values 'n/a' for missing values]
select distinct cst_gndr from bronze.crm_cust_info



----- SILVER LAYERS
--Check For NULLs or Duplicates in Primary Key
--Expectation: No result
select prd_id,count(*)  from silver.crm_prd_info
group by prd_id
having count(*)>1 OR prd_id IS NULL;


--(Quality Check)Check for unwanted spaces in a string values
--Expectations: No result
select prd_nm from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

--Check For NULLS or Negative Numbers
--Expectation: No result

select prd_id,prd_cost from silver.crm_prd_info
where prd_cost <0 or prd_cost is NULL ;

--(Quality Check)- Check the consistency of values in low Cardinality Columns
--ex. prd_line columns
-- Data Standarization and consitency[In our datawarehouse we aim to store clear and meaningful values rather than abbreviated terms]
--[IN our datawarehouse we use default values 'n/a' for missing values]

select DISTINCT prd_line from silver.crm_prd_info;


--CHECK FOR INVALID DATES[ END DATE must not be earlier than the start date]
select * from silver.crm_prd_info
where prd_end_dt< prd_start_dt;


-- Final look at the silver.crm_prd_info;
select * from silver.crm_prd_info;


-- FOr COmplex Transformation in SQL , I Typically narrow it down to a specific example and brainstrom multiple solution approaches
--Switch the start date with the end Date
--Each record must has a start date
--Solution2--> Derive the end date from the start date
-----------> END Date= Start Date of the 'NEXT' Record
-----------> for more convinent we reduce one day fro the end date of END Date--> END DATE-1
------NOTE-> End DATE of previous record must be smaller than the start date of next record.
----> For that we use LEAD() --> Access value from the next row within a Window.
---> for the scenario in problem try with some records first and then apply it

select * from bronze.crm_prd_info;

select prd_id,prd_key,prd_nm,prd_start_dt,prd_end_dt,
LEAD(prd_start_dt) over (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')



----Data quality checks
---------crm.sales_details
select * FROM bronze.crm_sales_details
---Check for Invalid Dates--[ Negative numbers or zeros can't be cast to dates]
-- in sls_order_dt , the length of the date muxt be 8
-- Check for Outliers by validating the boundries of the date range

select NULLIF(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0
or LENGTH(sls_order_dt::text)!=8
or sls_order_dt>20500101
or sls_order_dt<19000101

-- in above query do it for sls_ship_dt and sls_due_dt
-- Order_date is always before(earlier or smaller than) the shipping_date and due_date

select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0;

-- Check for INVALID DATE Orders
select *from silver.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt>sls_due_dt;

-- Business RULE
-- Sales= Quantity * Price
-- Negative ,Zerors,Nulls are not allowed!

-- For above -- Check data consistency : Between Sales,Quantity and Price
-- >> Sales = Quantity * Price
-- >> Values Must not be NULL , Zero or Negative.




select sls_sales,
sls_quantity,sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity <=0 OR sls_price<=0
order by sls_sales,sls_quantity,sls_price ;

-- In above query the sls_slaes is negative so solve this type of issue we have to discuss it with the seniors
-- 1 SOlution--> Data issues will be fixed directly at source system\
-- 2 solution--> Data issues has to fixed into Data warehouse
-- all the rules are different from company to company

--Rules -for this project
-- If Sales is Negative,zero,or null derive it using quantity and price
-- If Price is zero or null ,calculate using sales and quantity
-- If the price is -ve,convert it into positive value

SELECT DISTINCT
--sls_sales as old_sls_sales,
sls_quantity,
--sls_price as old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales!=sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0
		THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
END as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales<=0 OR sls_quantity <=0 OR sls_price<=0
order by sls_sales,sls_quantity,sls_price ;


---Data quality checks
---TABLE--->> bronze.erp_cust_az12
-- Identify Out of range dates(check for very old customer)

select Distinct bdate from bronze.erp_cust_az12
where bdate < '1924-01-01';--(This record is very old -- about 100 years from now)

--Check for birthday in future

select Distinct bdate from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate>CURRENT_DATE;

-- DATA standadization and consistency
select distinct gen from silver.erp_cust_az12

select distinct gen,
case when UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen FROM bronze.erp_cust_az12;



---DATA standadization and consistency bronze.erp_loc_a101
select replace(cid,'-','') cid

select DISTINCT cntry as old_cntry,
case when TRIM(cntry) ='DE' THEN 'Germany'
	when TRIM(cntry) IN ('US','USA') THEN 'United States'
	when TRIM(cntry) = '' or TRIM(cntry) is NULL THEN 'n/a'
ELSE TRIM(cntry)
end as cntry
from bronze.erp_loc_a101 order by cntry;
